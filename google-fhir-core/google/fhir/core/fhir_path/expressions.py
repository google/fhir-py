# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Common classes for FHIR Path expression support.

Most users should use the FHIR-version specific modules to create these classes,
such as in the r4 sub-package.
"""

import collections
import copy
import datetime
from typing import Any, Dict, Iterable, List, Optional, Union, cast

from google.protobuf import message
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import quantity
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import annotation_utils

# TODO(b/208900793): Expand to all FHIRPath-comparable equivalent types.
Comparable = Union[
    str, bool, int, float, datetime.date, datetime.datetime, quantity.Quantity
]
BuilderOperand = Union[Comparable, 'Builder']
StructureDefinition = message.Message


class ValueSetBuilder:
  """Convenience class for building a valueset proto usable in views."""

  def __init__(self, url: str, value_set: message.Message):
    # TODO(b/208900793): Use a protocol for ValueSets to avoid need to cast.
    self._value_set = cast(Any, value_set)
    self._value_set.url.value = url

  def with_codes(
      self, system: str, code_values: List[str]
  ) -> 'ValueSetBuilder':
    """Adds codes from the given system and returns the valueset builder."""
    for code in code_values:
      contains_elem = self._value_set.expansion.contains.add()
      contains_elem.system.value = system
      contains_elem.code.value = code

    return self

  def with_version(self, version: str) -> 'ValueSetBuilder':
    self._value_set.version.value = version
    return self

  def build(self) -> message.Message:
    # TODO(b/208900793): Use a protocol for ValueSets to avoid need to cast.
    return cast(message.Message, self._value_set)


# TODO(b/208900793): Add support for basic functions and comparisons.
class Builder:
  """FHIRPath expression builder.

  This class offers a fluent builder with tab completion to create FHIRPath
  expressions in Python code. For example, a user might build an expression
  to retrieve patient family names like this:

    builder = r4.builder(Patient.DESCRIPTOR)
    expression = builder.name.family.to_expression()

  This produces a CompiledExpression (defined above) that can be evaluated
  against any FHIR resource, returning the desired field or expression results.

  Python users are encouraged to create FHIRPath expressions with this
  rather than as strings because it will provide auto-suggest feedback for
  fields and FHIRPath functions and validate them as the expressions are built.

  In addition to FHIRPath fields and expressions shown above, users can also
  build boolean expressions using the & (and) | (or) and ^ (xor) operators,
  similar to other libraries like Pandas. For example, the patient builder
  above could be used to find active patients with at least one address:

    builder.active & builder.address.exists()

  which will build the following FHIRPath expression:

    "active and address.exists()"

  See https://hl7.org/fhirpath/#boolean-logic for details on the semantics.

  Consumers should create instances of this class for the version of FHIR
  they are using. For instance, FHIR R4 users should use the
  google.fhir.r4.fhir_path.builder method to create this.
  """

  def __init__(
      self,
      node: _evaluation.ExpressionNode,
      handler: primitive_handler.PrimitiveHandler,
  ):
    self._node = node
    self._handler = handler

  @classmethod
  def replace_with_operand(
      cls,
      old_builder: 'Builder',
      old_path: str,
      replacement_node: _evaluation.ExpressionNode,
  ) -> 'Builder':
    """Returns a builder with the old path replaced with a new node.

    Args:
      old_builder: Builder with nodes to be copied into the new one.
      old_path: String of the old path to be replaced in the old_builder. If no
        path matches, then the old builder will be the same as the new builder.
      replacement_node: An expression node that will replace the node that
        matches the old_path.

    Returns:
      A builder with the new expression node tree.
    """
    localized = copy.deepcopy(old_builder.node)
    localized.replace_operand(old_path, replacement_node)
    return cls(localized, old_builder._handler)  # pylint: disable=protected-access

  def _primitive_to_fhir_path(self, primitive: Optional[Comparable]) -> str:
    """Converts a primitive type into a FHIRPath literal string."""
    if isinstance(primitive, bool):
      return 'true' if primitive else 'false'
    elif isinstance(primitive, datetime.date):
      return f'@{cast(datetime.date, primitive).isoformat()}'
    elif isinstance(primitive, datetime.datetime):
      return f'@{cast(datetime.datetime, primitive).isoformat()}'
    elif isinstance(primitive, quantity.Quantity):
      return str(primitive)
    elif primitive is None:
      return '{}'
    else:
      return repr(primitive)

  def _primitive_to_message(self, primitive: Comparable) -> message.Message:
    """Converts a primitive type to the corresponding FHIR Proto message."""
    if isinstance(primitive, str):
      return self._handler.new_string(primitive)
    elif isinstance(primitive, bool):
      return self._handler.new_boolean(primitive)
    elif isinstance(primitive, int):
      return self._handler.new_integer(primitive)
    elif isinstance(primitive, float):
      return self._handler.new_decimal(str(primitive))
    elif isinstance(primitive, datetime.datetime):
      if not primitive.tzinfo:
        primitive = primitive.replace(tzinfo=datetime.timezone.utc)
      return self._handler.primitive_wrapper_from_json_value(
          primitive.isoformat(), self._handler.date_time_cls
      ).wrapped
    elif isinstance(primitive, datetime.date):
      return self._handler.primitive_wrapper_from_json_value(
          primitive.isoformat(), self._handler.date_cls
      ).wrapped
    elif isinstance(primitive, quantity.Quantity):
      return self._handler.new_quantity(primitive.value, primitive.unit)
    else:
      raise ValueError(f'Unsupported primitive type: {type(primitive)}')

  def _to_node(self, operand: BuilderOperand) -> _evaluation.ExpressionNode:
    """Returns a node from a Builder or Comparable.

    Args:
      operand: An input to the operator that is either a comparable or Builder.

    Returns:
      An ExpressionNode.
    """
    # Linter doesn't realize rhs is the same class.
    # pylint: disable=protected-access
    if isinstance(operand, Builder):
      # TODO(b/244184211): Add check that operand has the same
      # root resource as self.
      return operand.node
    # pylint: enable=protected-access
    else:  # Should be a primitive type.
      as_message = (
          None if operand is None else self._primitive_to_message(operand)
      )
      primitive_type = _fhir_path_data_types.primitive_type_from_type_code(
          type(operand).__name__
      )
      return _evaluation.LiteralNode(
          self.node.context,
          as_message,
          self._primitive_to_fhir_path(operand),
          primitive_type,
      )

  @property
  def fhir_path(self) -> str:
    """The FHIRPath expression as a string."""
    return self.node.to_fhir_path()

  def __getattr__(self, name: str) -> 'Builder':
    # Prevents infinite recursion when Builder is deep copied.
    if name.startswith('__'):
      raise AttributeError(name)

    if isinstance(
        self.node.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      raise AttributeError(
          'Cannot directly access polymorphic fields. '
          f"Please use ofType['{name}'] instead."
      )

    # If a basic FHIR field, simply return it.
    if name in self.node.fields():
      return Builder(
          _evaluation.InvokeExpressionNode(self.node._context, name, self.node),
          self._handler,
      )

    # Check if the string is a builder shorthand for a choice type, such as
    # Observation.valueQuantity would be expressed as
    # Observation.value.ofType('Quantity') in FHIRPath.
    for base_name, type_names in self._choice_fields().items():
      for type_name in type_names:
        if name == f'{base_name}{type_name[0].upper() + type_name[1:]}':
          return getattr(self, base_name).ofType(type_name)

    raise AttributeError((
        f'No such field {name} in {self.fhir_path}',
        f'Expected something in {self.fhir_path_fields()}',
    ))

  @property
  def node(self) -> _evaluation.ExpressionNode:
    return self._node

  @property
  def return_type(self) -> _fhir_path_data_types.FhirPathDataType:
    return self.node.return_type

  @property
  def primitive_handler(self) -> primitive_handler.PrimitiveHandler:
    return self._handler

  def _to_builder(self, node: _evaluation.ExpressionNode) -> 'Builder':
    return Builder(node, self._handler)

  def get_parent_builder(self) -> 'Builder':
    return self._to_builder(self.node.parent_node)

  def get_root_builder(self) -> 'Builder':
    return self._to_builder(self.node.get_root_node())

  def get_resource_builders(self) -> List['Builder']:
    return [
        self._to_builder(node) for node in set(self.node.get_resource_nodes())
    ]

  def all(self, criteria: 'Builder') -> 'Builder':
    """The FHIRPath all() function.

    Returns True if all elements in the parent expression meet the given
    criteria.

    Here is an example use:

    >>> pat = <patient fhirpath builder>
    >>> all_addresses_have_use = pat.address.all(pat.address.use.exists())

    Args:
      criteria: An expression containing the matching logic to be applied.

    Returns:
      An expression that returns True if all items in the parent meet the
      criteria.
    """
    param_nodes = self._function_args_to_nodes(
        self.node, [criteria], element_of_array=True
    )
    return Builder(
        _evaluation.AllFunction(self.node.context, self.node, param_nodes),
        self._handler,
    )

  def exists(self) -> 'Builder':
    """The FHIRPath exists() function.

    Returns:
      An expression that returns True if the parent expression evaluates
      to one or more values.
    """
    return self._to_builder(
        _evaluation.ExistsFunction(self.node.context, self.node, [])
    )

  def count(self) -> 'Builder':
    """The FHIRPath count() function.

    Returns:
      An expression that evaluates to the count of items in the parent.
    """
    return self._to_builder(
        _evaluation.CountFunction(self.node.context, self.node, [])
    )

  def empty(self) -> 'Builder':
    """The FHIRPath empty() function.

    Returns:
      An expression that evaluates to True if the parent evaluates to empty.
    """
    return self._to_builder(
        _evaluation.EmptyFunction(self.node.context, self.node, [])
    )

  def matches(self, regex: str) -> 'Builder':
    """The FHIRPath matches() function.

    Args:
      regex: a regular expression to match against the parent element.

    Returns:
      An expression that evaluates to True if the parent matches the given
      regular expression.
    """
    param_nodes = self._function_args_to_nodes(self.node, [regex])
    return self._to_builder(
        _evaluation.MatchesFunction(self.node.context, self.node, param_nodes)
    )

  def not_(self) -> 'Builder':
    """The FHIRPath not() function.

    Returns:
      An expression that evaluates to negation of the parent.
    """
    return self._to_builder(
        _evaluation.NotFunction(self.node.context, self.node, [])
    )

  def first(self) -> 'Builder':
    """The FHIRPath first() function.

    Returns:
      An expression that evaluates to the first element of the parent, or
      empty if the parent has no results.
    """
    return self._to_builder(
        _evaluation.FirstFunction(self.node.context, self.node, [])
    )

  def hasValue(self) -> 'Builder':  # pylint: disable=invalid-name
    """The FHIRPath hasValue() function.

    Returns:
      An expression that evaluates to True if the parent has a single value
      that is a primitive.
    """
    return self._to_builder(
        _evaluation.HasValueFunction(self.node.context, self.node, [])
    )

  def idFor(self, resource_type: str) -> 'Builder':  # pylint: disable=invalid-name
    """Function that returns the raw id for the given resource type.

    This is used in FHIR references. For example, subject.idFor('Patient')
    returns the raw patient id.

    Args:
      resource_type: The FHIR resource to get the id for, e.g. 'Patient' or
        'http://hl7.org/fhir/StructureDefinition/Patient'

    Returns:
      An expression retrieving the resource id.
    """
    param_nodes = self._function_args_to_nodes(self.node, [resource_type])
    return self._to_builder(
        _evaluation.IdForFunction(self.node.context, self.node, param_nodes)
    )

  def memberOf(self, value_set: Union[str, message.Message]) -> 'Builder':  # pylint: disable=invalid-name
    """The FHIRPath memberOf() function.

    This is used to determine whether a codeable concept is a member of a given
    value set. The value set may be a literal, FHIR ValueSet proto or
    (more commonly) a URI that is resolved by the underlying evaluation engine.

    For example, the following expression can be used to determine if an
    observation code is part of a specific valueset:

    >>> obs = <obs expression builder>
    >>> obs.code.MemberOf('url:example:my:valueset')

    Args:
      value_set: may be either a string containing a value set URL or an
        expanded value set in the form of a proto. See examples in the README
        documentation for more details.

    Returns:
      An expression to that evaluates to true if the parent is a member of
      the given value set.
    """
    param_nodes = self._function_args_to_nodes(self.node, [value_set])
    return self._to_builder(
        _evaluation.MemberOfFunction(self.node.context, self.node, param_nodes)
    )

  def ofType(self, fhir_type: str) -> 'Builder':  # pylint: disable=invalid-name
    """The FHIRPath ofType() function.

    This is used to convert FHIR Choice types to specific elements. For example,

    >>> obs = <observation builder>
    >>> quantity_value = obs.value.ofType('Quantity')

    See the FHIR resource documentation to see what each choice type contains.

    Args:
      fhir_type: The type of the choice to be returned.

    Returns:
      The an expression that evaluates to the specified choice type.
    """
    param_nodes = self._function_args_to_nodes(self.node, [fhir_type])
    return self._to_builder(
        _evaluation.OfTypeFunction(self.node.context, self.node, param_nodes)
    )

  def where(self, criteria: 'Builder') -> 'Builder':
    """The FHIRPath where() function.

    Filters the collection of FHIR elements to meet criteria defined by a
    builder expression.

    Here is an example use:

    >>> pat = <patient fhirpath builder>
    >>> home_addresses = pat.address.where(pat.address.use = 'home')

    Args:
      criteria: An expression builder containing the filtering logic.

    Returns:
      An expression that contains the items that match the given criteria.
    """
    param_nodes = self._function_args_to_nodes(
        self.node, [criteria], element_of_array=True
    )
    return self._to_builder(
        _evaluation.WhereFunction(self.node.context, self.node, param_nodes)
    )

  def anyTrue(self) -> 'Builder':  # pylint: disable=invalid-name
    """The FHIRPath anyTrue() function.

    Returns True if any element in ther operand's collection is True.

    Here is an example use:

    >>> eob = <ExplanationOfBenefit fhirpath builder>
    >>> eob.select({
        'primary_diagnoses':
            eob.diagnosis.where(
                eob.diagnosis.type.memberOf(primary_valueset).anyTrue())
    })

    Returns:
      A boolean indicating if any element in the operand collection is True.
    """
    return self._to_builder(
        _evaluation.AnyTrueFunction(self.node.context, self.node, [])
    )

  def toInteger(self) -> 'Builder':  # pylint: disable=invalid-name
    """The FHIRPath toInteger() function.

    Casts its operand to an integer.
    Returns an empty collection if the operand can not be coerced to an integer.
    Raises a ValueError if the operand collection contains more than one
    element.

    Returns:
      An integer representation of its operand.
    """
    return self._to_builder(
        _evaluation.ToIntegerFunction(self.node.context, self.node, [])
    )

  def _function_args_to_nodes(
      self,
      operand_node: _evaluation.ExpressionNode,
      args: List[Any],
      element_of_array: bool = False,
  ) -> List[_evaluation.ExpressionNode]:
    """Converts builder args to FHIRPath expressions into evaluation nodes."""
    params: List[_evaluation.ExpressionNode] = []
    for arg in args:
      if (
          isinstance(arg, message.Message)
          and annotation_utils.is_resource(arg)
          and annotation_utils.get_structure_definition_url(arg)
          == _evaluation.VALUE_SET_URL
      ):
        # Handle ValueSet literals passed in as part of the builder. This is
        # specifically to support common valueset-based inclusion checks.
        value_set = cast(Any, arg)
        fhir_path_str = f"'{value_set.url.value}'"

        params.append(
            _evaluation.LiteralNode(
                self.node.context,
                value_set,
                fhir_path_str,
                _fhir_path_data_types.String,
            )
        )
      elif isinstance(arg, Builder):
        # Expression builder parameters are passed as expression
        # nodes that are localized to run under the current expression.
        # For example, the "where" expression builder in
        # `patient.address.where(patient.address.use = 'home')` is built from
        # patient, but should be evaluated in the context of address.

        localized = copy.deepcopy(arg.node)
        localized.replace_operand(
            operand_node.to_fhir_path(),
            _evaluation.ReferenceNode(
                self.node.context,
                operand_node,
                element_of_array=element_of_array,
            ),
        )
        params.append(localized)
      else:
        # All other types are treated as literals.
        rhs_message = self._primitive_to_message(arg)
        fhir_path_str = self._primitive_to_fhir_path(arg)
        primitive_type = _fhir_path_data_types.primitive_type_from_type_code(
            type(arg).__name__
        )
        params.append(
            _evaluation.LiteralNode(
                self.node.context, rhs_message, fhir_path_str, primitive_type
            )
        )
    return params

  def contains(self, rhs: BuilderOperand) -> 'Builder':
    return Builder(
        _evaluation.ContainsNode(
            self.node.context, self.node, self._to_node(rhs)
        ),
        self._handler,
    )

  def union(self, rhs: BuilderOperand) -> 'Builder':
    return Builder(
        _evaluation.UnionNode(self.node.context, self.node, self._to_node(rhs)),
        self._handler,
    )

  def __eq__(self, rhs: BuilderOperand) -> 'Builder':
    return Builder(
        _evaluation.EqualityNode(
            self.node.context,
            _ast.EqualityRelation.Op.EQUAL,
            self.node,
            self._to_node(rhs),
        ),
        self._handler,
    )

  def __ne__(self, rhs: BuilderOperand) -> 'Builder':
    return Builder(
        _evaluation.EqualityNode(
            self.node.context,
            _ast.EqualityRelation.Op.NOT_EQUAL,
            self.node,
            self._to_node(rhs),
        ),
        self._handler,
    )

  def __or__(self, rhs: 'Builder') -> 'Builder':
    return Builder(
        _evaluation.BooleanOperatorNode(
            self.node.context, _ast.BooleanLogic.Op.OR, self.node, rhs.node
        ),
        self._handler,
    )

  def __and__(self, rhs: Union[str, 'Builder']) -> 'Builder':
    # Both string concatenation and boolean and use the operator '&'
    if isinstance(rhs, str):
      return self._arithmetic_node(_ast.Arithmetic.Op.STRING_CONCATENATION, rhs)
    return Builder(
        _evaluation.BooleanOperatorNode(
            self.node.context, _ast.BooleanLogic.Op.AND, self.node, rhs.node
        ),
        self._handler,
    )

  def __xor__(self, rhs: 'Builder') -> 'Builder':
    return Builder(
        _evaluation.BooleanOperatorNode(
            self.node.context, _ast.BooleanLogic.Op.XOR, self.node, rhs.node
        ),
        self._handler,
    )

  def implies(self, rhs: 'Builder') -> 'Builder':
    """The FHIRPath implies opeator."""
    # Linter doesn't realize rhs is the same class.
    # pylint: disable=protected-access
    return Builder(
        _evaluation.BooleanOperatorNode(
            self.node.context,
            _ast.BooleanLogic.Op.IMPLIES,
            self.node,
            rhs.node,
        ),
        self._handler,
    )
    # pylint: enable=protected-access

  def _comparison_node(
      self, operator: _ast.Comparison.Op, rhs: BuilderOperand
  ) -> 'Builder':
    rhs_node = self._to_node(rhs)
    return Builder(
        _evaluation.ComparisonNode(
            self.node.context, operator, self.node, rhs_node
        ),
        self._handler,
    )

  def __lt__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.LESS_THAN, rhs)

  def __gt__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.GREATER_THAN, rhs)

  def __le__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.LESS_THAN_OR_EQUAL, rhs)

  def __ge__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.GREATER_THAN_OR_EQUAL, rhs)

  def _arithmetic_node(
      self, operator: _ast.Arithmetic.Op, rhs: BuilderOperand
  ) -> 'Builder':
    return Builder(
        _evaluation.ArithmeticNode(
            self.node.context, operator, self.node, self._to_node(rhs)
        ),
        self._handler,
    )

  def __getitem__(self, key: int) -> 'Builder':
    # TODO(b/226135993): consider supporting other types, such as Builders
    # themselves in the index.
    if isinstance(key, int):
      idx_msg = self._primitive_to_message(key)
      fhir_path_str = self._primitive_to_fhir_path(key)
      index = _evaluation.LiteralNode(
          self.node.context,
          idx_msg,
          fhir_path_str,
          _fhir_path_data_types.Integer,
      )
      return Builder(
          _evaluation.IndexerNode(self.node.context, self.node, index),
          self._handler,
      )
    else:
      raise TypeError('Expected int index type')

  def __add__(self, rhs: BuilderOperand) -> 'Builder':
    return self._arithmetic_node(_ast.Arithmetic.Op.ADDITION, rhs)

  def __mul__(self, rhs: BuilderOperand) -> 'Builder':
    return self._arithmetic_node(_ast.Arithmetic.Op.MULTIPLICATION, rhs)

  def __sub__(self, rhs: BuilderOperand) -> 'Builder':
    return self._arithmetic_node(_ast.Arithmetic.Op.SUBTRACTION, rhs)

  def __truediv__(self, rhs: BuilderOperand) -> 'Builder':
    return self._arithmetic_node(_ast.Arithmetic.Op.DIVISION, rhs)

  def __floordiv__(self, rhs: BuilderOperand) -> 'Builder':
    return self._arithmetic_node(_ast.Arithmetic.Op.TRUNCATED_DIVISION, rhs)

  def __mod__(self, rhs: BuilderOperand) -> 'Builder':
    return self._arithmetic_node(_ast.Arithmetic.Op.MODULO, rhs)

  def _choice_fields(self) -> Dict[str, List[str]]:
    """Returns a map from the base choice field name to the field types."""
    node_type = self.node.return_type
    if not isinstance(node_type, _fhir_path_data_types.StructureDataType):
      return {}

    children = cast(
        _fhir_path_data_types.StructureDataType, node_type
    ).child_defs
    choice_fields = collections.defaultdict(list)
    for name, elem_def in children.items():
      # Include field and type codes for choice types (fields with > 1 type),
      # such as Observation values Quantity, CodeableConcept, etc.
      if len(elem_def.type) > 1:
        for type_code in elem_def.type:
          choice_fields[name].append(type_code.code.value)
    return choice_fields

  def fhir_path_fields(self) -> List[str]:
    """Returns the FHIR Path fields available on this builder, if any.

    This includes shorthand fields for FHIR choice type, such as Observation's
    valueQuantity, valueCodeableConcept, and so on.
    """
    fields: List[str] = []

    # Get choice type field names and add simple fields.
    for base_name, types in self._choice_fields().items():
      for field_type in types:
        fields.append(f'{base_name}{field_type[0].upper() + field_type[1:]}')

    fields.extend(self.node.fields())
    fields.sort()
    return fields

  def __dir__(self) -> Iterable[str]:  # pytype: disable=signature-mismatch  # overriding-return-type-checks
    # If the current node is for a structure, return the fields for that
    # structure to support code auto completion, as well as the normal
    # class methods.
    fields = self.fhir_path_fields()
    fields.extend(dir(type(self)))
    return fields

  def __repr__(self):
    return f'Builder("{self.node.to_fhir_path()}")'

  def __str__(self):
    return self.node.to_fhir_path()

  def debug_string(self, with_typing: bool = False) -> str:
    return self.node.debug_string(with_typing)


def from_fhir_path_expression(
    fhir_path_expression: str,
    fhir_context: context.FhirPathContext,
    structdef_type: _fhir_path_data_types.StructureDataType,
    handler: primitive_handler.PrimitiveHandler,
    root_node_context: Optional[Builder] = None,
) -> 'Builder':
  """Function to create an expression builder from a fhir path string.

  Args:
    fhir_path_expression: The FHIRPath expression to parse.
    fhir_context: The context containing the FHIR resources.
    structdef_type: The root structure definition for the expression.
    handler: The primitive handler.
    root_node_context: Optional root expression that fhir_path_expression may
      reference.

  Returns:
    The expression Builder equivalent of the fhir_path_expression.
  """
  ast = _ast.build_fhir_path_ast(fhir_path_expression)

  new_context = root_node_context.node if root_node_context else None
  visitor = _evaluation.FhirPathCompilerVisitor(
      handler, fhir_context, structdef_type, new_context
  )
  root = visitor.visit(ast)
  return Builder(root, handler)

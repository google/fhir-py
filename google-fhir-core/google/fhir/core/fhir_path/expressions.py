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

import copy
import datetime
import decimal
from typing import Any, Iterable, List, Union, cast

from google.protobuf import message
from google.fhir.core import codes
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import annotation_utils
from google.fhir.core.utils import fhir_types
from google.fhir.core.utils import proto_utils

# TODO: Expand to all FHIRPath-comparable equivalent types.
Comparable = Union[str, bool, int, float, datetime.date, datetime.datetime]
BuilderOperand = Union[Comparable, 'Builder']


class ValueSetBuilder:
  """Convenience class for building a valueset proto usable in views."""

  def __init__(self, url: str, value_set: message.Message):
    # TODO: Use a protocol for ValueSets to avoid need to cast.
    self._value_set = cast(Any, value_set)
    self._value_set.url.value = url

  def with_codes(self, system: str,
                 code_values: List[str]) -> 'ValueSetBuilder':
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
    # TODO: Use a protocol for ValueSets to avoid need to cast.
    return cast(message.Message, self._value_set)


# TODO: support other result types and messages, and introspection
# on the actual returned type.
class EvaluationResult:
  """The result of a FHIRPath expression evaluation.

  Users should inspect and convert this to the expected target type based
  on their evaluation needs.
  """

  def __init__(self, messages: List[message.Message],
               work_space: _evaluation.WorkSpace):
    self._messages = messages
    self._work_space = work_space

  def has_value(self) -> bool:
    """Returns true if the evaluation returned a value; false if not."""
    return bool(self._messages)

  def as_string(self) -> str:
    """Returns the result as a string.

    Raises:
      ValueError if the `EvaluationResult` is not a single string.
    """
    if len(self._messages) != 1:
      raise ValueError('FHIRPath did not evaluate to a single string.')

    # Codes can be stored internally as enums, but users will expect the FHIR
    # defined string value.
    if fhir_types.is_type_or_profile_of_code(self._messages[0]):
      return codes.get_code_as_string(self._messages[0])

    # TODO): Check primitive type rather than assuming value.
    return proto_utils.get_value_at_field(self._messages[0], 'value')

  def as_bool(self) -> bool:
    """Returns the result as a boolean.

    Raises:
      ValueError if the `EvaluationResult` is not a single boolean.
    """
    if len(self._messages) != 1:
      raise ValueError('FHIRPath did not evaluate to a single boolean.')

    # TODO): Check primitive type rather than assuming value.
    return proto_utils.get_value_at_field(self._messages[0], 'value')

  def as_int(self) -> int:
    """Returns the result as an integer.

    Raises:
      ValueError if the `EvaluationResult` is not a single integer.
    """
    if len(self._messages) != 1:
      raise ValueError('FHIRPath did not evaluate to a single integer.')

    # TODO): Check primitive type rather than assuming value.
    return proto_utils.get_value_at_field(self._messages[0], 'value')

  def as_decimal(self) -> decimal.Decimal:
    """Returns the result as a decimal.

    Raises:
      ValueError if the `EvaluationResult` is not a single decimal.
    """
    if len(self._messages) != 1:
      raise ValueError('FHIRPath did not evaluate to a single decimal.')

    # TODO): Check primitive type rather than assuming value.
    return decimal.Decimal(
        proto_utils.get_value_at_field(self._messages[0], 'value'))


class CompiledExpression:
  """Compiled FHIRPath expression."""

  def __init__(self, root_node: _evaluation.ExpressionNode,
               handler: primitive_handler.PrimitiveHandler,
               fhir_context: context.FhirPathContext, fhir_path: str):
    self._root_node = root_node
    self._primitive_handler = handler
    self._context = fhir_context
    self._fhir_path = fhir_path

  @classmethod
  def compile(
      cls,
      fhir_path: str,
      handler: primitive_handler.PrimitiveHandler,
      structdef_url: str,
      fhir_context: context.FhirPathContext,
  ) -> 'CompiledExpression':
    """Compiles the FHIRPath expression that targets the given structure."""

    structdef = fhir_context.get_structure_definition(structdef_url)
    data_type = _fhir_path_data_types.StructureDataType(structdef)

    ast = _ast.build_fhir_path_ast(fhir_path)
    visitor = _evaluation.FhirPathCompilerVisitor(handler, fhir_context,
                                                  data_type)

    root = visitor.visit(ast)
    return CompiledExpression(root, handler, fhir_context, fhir_path)

  def evaluate(self, resource: message.Message) -> EvaluationResult:
    return self._evaluate(
        _evaluation.WorkSpaceMessage(message=resource, parent=None))

  @property
  def fhir_path(self) -> str:
    """The FHIRPath expression as a string."""
    return self._fhir_path

  def _evaluate(
      self,
      workspace_message: _evaluation.WorkSpaceMessage) -> EvaluationResult:
    work_space = _evaluation.WorkSpace(
        primitive_handler=self._primitive_handler,
        fhir_context=self._context,
        message_context_stack=[workspace_message])
    results = self._root_node.evaluate(work_space)
    return EvaluationResult(
        messages=[result.message for result in results], work_space=work_space)


# TODO: Add support for basic functions and comparisons.
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

  def __init__(self, node: _evaluation.ExpressionNode,
               fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler):
    # TODO: Eliminate passing fhir_context into Builder. Just retrieve
    # the fhir_context from the input node.
    self._node = node
    self._context = fhir_context
    self._handler = handler

  def to_expression(self) -> CompiledExpression:
    """Returns the compiled expression that was built here."""
    return CompiledExpression(self._node, self._handler, self._context,
                              self.fhir_path)

  def _primitive_to_fhir_path(self, primitive: Comparable) -> str:
    """Converts a primitive type into a FHIRPath literal string."""
    if isinstance(primitive, bool):
      return 'true' if primitive else 'false'
    elif isinstance(primitive, datetime.date):
      return f'@{cast(datetime.date, primitive).isoformat()}'
    elif isinstance(primitive, datetime.datetime):
      return f'@{cast(datetime.datetime, primitive).isoformat()}'
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
      return self._handler.primitive_wrapper_from_json_value(
          primitive.isoformat(), self._handler.date_time_cls).wrapped
    elif isinstance(primitive, datetime.date):
      return self._handler.primitive_wrapper_from_json_value(
          primitive.isoformat(), self._handler.date_cls).wrapped
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
      # TODO: Add check that operand has the same
      # root resource as self.
      return operand._node
    # pylint: enable=protected-access
    else:  # Should be a primitive type.
      primitive_type = _fhir_path_data_types.primitive_type_from_type_code(
          type(operand).__name__)
      return _evaluation.LiteralNode(self._context,
                                     self._primitive_to_message(operand),
                                     self._primitive_to_fhir_path(operand),
                                     primitive_type)

  @property
  def fhir_path(self) -> str:
    """The FHIRPath expression as a string."""
    return self._node.to_fhir_path()

  def __getattr__(self, name: str) -> 'Builder':
    # If the node has a known return type, ensure the field exists on it.
    if self._node.return_type() and name not in self.fhir_path_fields():
      raise AttributeError(f'No such field {name} in {self.fhir_path}.')
    return Builder(
        _evaluation.InvokeExpressionNode(self._context, name, self._node),
        self._context, self._handler)

  def _builder(self, node: _evaluation.ExpressionNode) -> 'Builder':
    return Builder(node, self._context, self._handler)

  def get_node(self) -> _evaluation.ExpressionNode:
    return self._node

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
    param_nodes = self._function_args_to_nodes(self._node, [criteria])
    return Builder(
        _evaluation.AllFunction(self._context, self._node, param_nodes),
        self._context, self._handler)

  def exists(self) -> 'Builder':
    """The FHIRPath exists() function.

    Returns:
      An expression that returns True if the parent expression evaluates
      to one or more values.
    """
    return self._builder(
        _evaluation.ExistsFunction(self._context, self._node, []))

  def count(self) -> 'Builder':
    """The FHIRPath count() function.

    Returns:
      An expression that evaluates to the count of items in the parent.
    """
    return self._builder(
        _evaluation.CountFunction(self._context, self._node, []))

  def empty(self) -> 'Builder':
    """The FHIRPath empty() function.

    Returns:
      An expression that evaluates to True if the parent evaluates to empty.
    """
    return self._builder(
        _evaluation.EmptyFunction(self._context, self._node, []))

  def matches(self, regex: str) -> 'Builder':
    """The FHIRPath matches() function.

    Args:
      regex: a regular expression to match against the parent element.

    Returns:
      An expression that evaluates to True if the parent matches the given
      regular expression.
    """
    param_nodes = self._function_args_to_nodes(self._node, [regex])
    return self._builder(
        _evaluation.MatchesFunction(self._context, self._node, param_nodes))

  def not_(self) -> 'Builder':
    """The FHIRPath not() function.

    Returns:
      An expression that evaluates to negation of the parent.
    """
    return self._builder(_evaluation.NotFunction(self._context, self._node, []))

  def first(self) -> 'Builder':
    """The FHIRPath first() function.

    Returns:
      An expression that evaluates to the first element of the parent, or
      empty if the parent has no results.
    """
    return self._builder(
        _evaluation.FirstFunction(self._context, self._node, []))

  def hasValue(self) -> 'Builder':  # pylint: disable=invalid-name
    """The FHIRPath hasValue() function.

    Returns:
      An expression that evaluates to True if the parent has a single value
      that is a primitive.
    """
    return self._builder(
        _evaluation.HasValueFunction(self._context, self._node, []))

  def idFor(self, resource_type: str) -> 'Builder':  # pylint: disable=invalid-name
    """Function that returns the raw id for the given resource type.

    This is used in FHIR references. For example, subject.idFor('Patient')
    returns the raw patient id.

    Args:
      resource_type: the FHIR resource to get the id, e.g. 'Patient'.

    Returns:
      An expression retrieving the resource id.
    """
    param_nodes = self._function_args_to_nodes(self._node, [resource_type])
    return self._builder(
        _evaluation.IdForFunction(self._context, self._node, param_nodes))

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
    param_nodes = self._function_args_to_nodes(self._node, [value_set])
    return self._builder(
        _evaluation.MemberOfFunction(self._context, self._node, param_nodes))

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
    param_nodes = self._function_args_to_nodes(self._node, [fhir_type])
    return self._builder(
        _evaluation.OfTypeFunction(self._context, self._node, param_nodes))

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
    param_nodes = self._function_args_to_nodes(self._node, [criteria])
    return self._builder(
        _evaluation.WhereFunction(self._context, self._node, param_nodes))

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
    return self._builder(
        _evaluation.AnyTrueFunction(self._context, self._node, []))

  def _function_args_to_nodes(
      self, operand_node: _evaluation.ExpressionNode,
      args: List[Any]) -> List[_evaluation.ExpressionNode]:
    """Converts builder args to FHIRPath expressions into evaluation nodes."""
    params: List[_evaluation.ExpressionNode] = []
    for arg in args:
      if (isinstance(arg, message.Message) and
          annotation_utils.is_resource(arg) and
          annotation_utils.get_structure_definition_url(arg)
          == _evaluation.VALUE_SET_URL):
        # Handle ValueSet literals passed in as part of the builder. This is
        # specifically to support common valueset-based inclusion checks.
        value_set = cast(Any, arg)
        fhir_path_str = f"'{value_set.url.value}'"
        params.append(
            _evaluation.LiteralNode(self._context, value_set, fhir_path_str))
      elif isinstance(arg, Builder):
        # Expression builder parameters are passed as expression
        # nodes that are localized to run under the current expression.
        # For example, the "where" expression builder in
        # `patient.address.where(patient.address.use = 'home')` is built from
        # patient, but should be evaluated in the context of address. We do so
        # by replacing the path to the operand node with a new "root" context.
        # pylint: disable=protected-access
        localized = copy.deepcopy(arg._node, {})
        # pylint: enable=protected-access
        localized.replace_operand(
            operand_node.to_fhir_path(),
            _evaluation.RootMessageNode(self._context,
                                        operand_node.return_type()))
        params.append(localized)
      else:
        # All other types are treated as literals.
        rhs_message = self._primitive_to_message(arg)
        fhir_path_str = self._primitive_to_fhir_path(arg)
        params.append(
            _evaluation.LiteralNode(self._context, rhs_message, fhir_path_str))
    return params

  def __eq__(self, rhs: BuilderOperand) -> 'Builder':
    return Builder(
        _evaluation.EqualityNode(self._context, self._handler,
                                 _ast.EqualityRelation.Op.EQUAL, self._node,
                                 self._to_node(rhs)), self._context,
        self._handler)

  def __ne__(self, rhs: BuilderOperand) -> 'Builder':
    return Builder(
        _evaluation.EqualityNode(self._context, self._handler,
                                 _ast.EqualityRelation.Op.NOT_EQUAL, self._node,
                                 self._to_node(rhs)), self._context,
        self._handler)

  def __or__(self, rhs: 'Builder') -> 'Builder':
    return Builder(
        _evaluation.BooleanOperatorNode(self._context, self._handler,
                                        _ast.BooleanLogic.Op.OR, self._node,
                                        rhs._node), self._context,
        self._handler)

  def __and__(self, rhs: Union[str, 'Builder']) -> 'Builder':
    # Both string concatenation and boolean and use the operator '&'
    if isinstance(rhs, str):
      return self._arithmetic_node(_ast.Arithmetic.Op.STRING_CONCATENATION, rhs)
    return Builder(
        _evaluation.BooleanOperatorNode(self._context, self._handler,
                                        _ast.BooleanLogic.Op.AND, self._node,
                                        rhs._node), self._context,
        self._handler)

  def __xor__(self, rhs: 'Builder') -> 'Builder':
    return Builder(
        _evaluation.BooleanOperatorNode(self._context, self._handler,
                                        _ast.BooleanLogic.Op.XOR, self._node,
                                        rhs._node), self._context,
        self._handler)

  def implies(self, rhs: 'Builder') -> 'Builder':
    """The FHIRPath implies opeator."""
    # Linter doesn't realize rhs is the same class.
    # pylint: disable=protected-access
    return Builder(
        _evaluation.BooleanOperatorNode(self._context, self._handler,
                                        _ast.BooleanLogic.Op.IMPLIES,
                                        self._node, rhs._node), self._context,
        self._handler)
    # pylint: enable=protected-access

  def _comparison_node(self, operator: _ast.Comparison.Op,
                       rhs: BuilderOperand) -> 'Builder':
    rhs_node = self._to_node(rhs)
    return Builder(
        _evaluation.ComparisonNode(self._context, self._handler, operator,
                                   self._node, rhs_node), self._context,
        self._handler)

  def __lt__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.LESS_THAN, rhs)

  def __gt__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.GREATER_THAN, rhs)

  def __le__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.LESS_THAN_OR_EQUAL, rhs)

  def __ge__(self, rhs: BuilderOperand) -> 'Builder':
    return self._comparison_node(_ast.Comparison.Op.GREATER_THAN_OR_EQUAL, rhs)

  def _arithmetic_node(self, operator: _ast.Arithmetic.Op,
                       rhs: BuilderOperand) -> 'Builder':
    return Builder(
        _evaluation.ArithmeticNode(self._context,
                                   self._handler, operator, self._node,
                                   self._to_node(rhs)), self._context,
        self._handler)

  def __getitem__(self, key: int) -> 'Builder':
    # TODO: consider supporting other types, such as Builders
    # themselves in the index.
    if isinstance(key, int):
      idx_msg = self._primitive_to_message(key)
      fhir_path_str = self._primitive_to_fhir_path(key)
      index = _evaluation.LiteralNode(self._context, idx_msg, fhir_path_str,
                                      _fhir_path_data_types.Integer)
      return Builder(
          _evaluation.IndexerNode(self._context, self._node, index),
          self._context, self._handler)
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

  def fhir_path_fields(self) -> List[str]:
    """Returns the FHIR Path fields available on this builder, if any."""
    return list(self._node.fields())

  def __dir__(self) -> Iterable[str]:
    # If the current node is for a structure, return the fields for that
    # structure to support code auto completion, as well as the normal
    # class methods.
    fields = self.fhir_path_fields()
    fields.extend(dir(type(self)))
    return fields

  def __repr__(self):
    return f'Builder("{self._node.to_fhir_path()}")'

  def __str__(self):
    return self._node.to_fhir_path()

  def debug_string(self, with_typing: bool = False) -> str:
    return self._node.debug_string(with_typing)

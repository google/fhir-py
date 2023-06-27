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
"""Internal FHIRPath evaluation library."""

import abc
import copy
import dataclasses
import datetime
import decimal
import re
import threading
from typing import Any, Dict, FrozenSet, List, Optional, Set, cast
import urllib

from google.protobuf import message
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import annotation_utils
from google.fhir.core.utils import fhir_types

# google-fhir supports multiple <major>.<minor>.x interpreters. If unable to
# import zoneinfo from stdlib, fallback to the backports package. See more at:
# https://pypi.org/project/backports.zoneinfo/.
# pylint: disable=g-import-not-at-top
try:
  import zoneinfo
except ImportError:
  from backports import zoneinfo  # pytype: disable=import-error
# pylint: enable=g-import-not-at-top

VALUE_SET_URL = 'http://hl7.org/fhir/StructureDefinition/ValueSet'
QUANTITY_URL = 'http://hl7.org/fhir/StructureDefinition/Quantity'
ElementDefinition = message.Message


_MAPPING_FUNCTIONS = frozenset([
    _ast.Function.Name.ALL,
    _ast.Function.Name.WHERE,
])


@dataclasses.dataclass(frozen=True, order=True)
class CodeValue:
  """An immutable code,value tuple for local use and set operations."""

  system: str
  value: str


@dataclasses.dataclass(frozen=True)
class ValueSetCodes:
  """A value set and the codes it contains."""

  url: str
  version: Optional[str]
  codes: FrozenSet[CodeValue]


def to_code_values(value_set_proto: message.Message) -> FrozenSet[CodeValue]:
  """Helper function to convert a ValueSet proto into a set of code value data types."""
  # TODO(b/208900793): Use a protocol for ValueSets to allow type checking.
  expansion = value_set_proto.expansion.contains  # pytype: disable=attribute-error
  codes = [
      CodeValue(code_elem.system.value, code_elem.code.value)
      for code_elem in expansion
  ]
  return frozenset(codes)


class ExpressionNode(abc.ABC):
  """Abstract base class for all FHIRPath expression evaluation."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      return_type: _fhir_path_data_types.FhirPathDataType,
  ) -> None:
    self._return_type = return_type
    self._context = fhir_context

  @abc.abstractmethod
  def to_fhir_path(self) -> str:
    """Returns the FHIRPath string for this and its children node."""

  def to_path_token(self) -> str:
    """Returns the path of the node itself."""
    return ''

  @property
  def context(self) -> context.FhirPathContext:
    return self._context

  @property
  def return_type(self) -> _fhir_path_data_types.FhirPathDataType:
    """The descriptor of the items returned by the expression, if known."""
    return self._return_type

  @abc.abstractmethod
  def get_root_node(self) -> 'ExpressionNode':
    pass

  @abc.abstractmethod
  def get_resource_nodes(self) -> List['ExpressionNode']:
    """Returns base FHIR resources that are referenced in the builder."""
    return []

  @property
  @abc.abstractmethod
  def parent_node(self) -> 'ExpressionNode':
    pass

  @abc.abstractmethod
  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    pass

  def __hash__(self) -> int:
    return hash(self.to_fhir_path())

  def fields(self) -> Set[str]:
    """Returns known fields from this expression, or none if they are unknown.

    These are names pulled directly from the FHIR spec, and used in the FHIRPath
    and the JSON representation of the structure.
    """
    if self.return_type:
      return self.return_type.fields()
    return set()

  @property
  @abc.abstractmethod
  def operands(self) -> List['ExpressionNode']:
    """Returns the operands contributing to this node."""

  @abc.abstractmethod
  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    """Replace any operand that matches the given expression string."""

  def __str__(self) -> str:
    return self.to_fhir_path()

  def _operand_to_string(
      self, operand: 'ExpressionNode', with_typing: bool, indent: int = 0
  ) -> str:
    """Function to recursively print the operands of the input operand."""
    operand_name = f'{operand} '
    if operand.__class__.__name__ == 'ReferenceNode':
      operand_name = ''
      operand_prints = f'&{operand}'
    else:
      operand_prints = ''.join(
          '\n' + self._operand_to_string(op, with_typing, indent + 1)
          for op in operand.operands
      )
    type_print = f' type={operand.return_type}' if with_typing else ''
    return (
        f'{"| " * indent}+ '
        f'{operand_name}<{operand.__class__.__name__}{type_print}> ('
        f'{operand_prints})'
    )

  def debug_string(self, with_typing: bool = False) -> str:
    """Returns debug string of the current node."""
    return self._operand_to_string(self, with_typing)

  def __deepcopy__(self, memo) -> 'ExpressionNode':
    """Returns a deep copy of the node without copying the expensive fields."""
    new = self.__class__.__new__(self.__class__)
    # FhirPathContext is designed to be shared but mutable since it allows
    # access to all of the structure definitions that underlie the nodes which
    # may be static or through a server.
    memo.setdefault(id(self.context), self.context)
    # FhirPathDataType is immutable so we can skip the deepcopy on it.
    memo.setdefault(id(self.return_type), self.return_type)

    for field, val in self.__dict__.items():
      setattr(new, field, copy.deepcopy(val, memo))
    return new


def _check_is_predicate(
    function_name: str, params: List[ExpressionNode]
) -> None:
  """Raise an exception if expression params are a boolean predicate."""
  if len(params) != 1:
    raise ValueError((
        f'{function_name} expression require a single parameter,',
        f' got {len(params)}',
    ))

  if not isinstance(
      params[0].return_type, _fhir_path_data_types.Boolean.__class__
  ):
    raise ValueError((
        f'{function_name} expression require a boolean predicate',
        f' got {params[0].to_fhir_path()}',
    ))


class BinaryExpressionNode(ExpressionNode):
  """Base class for binary expressions."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      left: ExpressionNode,
      right: ExpressionNode,
      return_type: _fhir_path_data_types.FhirPathDataType,
  ) -> None:
    self._left = left
    self._right = right
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self.left.get_resource_nodes() + self.right.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self._left.get_root_node()

  @property
  def parent_node(self) -> ExpressionNode:
    return self._left

  @property
  def left(self) -> ExpressionNode:
    return self._left

  @property
  def right(self) -> ExpressionNode:
    return self._right

  @property
  def operands(self) -> List[ExpressionNode]:
    return [self._left, self._right]

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    if self._left.to_fhir_path() == expression_to_replace:
      self._left = replacement
    else:
      self._left.replace_operand(expression_to_replace, replacement)

    if self._right.to_fhir_path() == expression_to_replace:
      self._right = replacement
    else:
      self._right.replace_operand(expression_to_replace, replacement)

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    raise ValueError('Unable to visit BinaryExpression node')


class CoercibleBinaryExpressionNode(BinaryExpressionNode):
  """Base class for binary expressions which coerce operands.

  Unlike BinaryExpressionNode, requires the left and right to be coercible to
  each other.
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      left: ExpressionNode,
      right: ExpressionNode,
      return_type: _fhir_path_data_types.FhirPathDataType,
  ) -> None:
    if not _fhir_path_data_types.is_coercible(
        left.return_type, right.return_type
    ):
      raise ValueError(
          'Left and right operands are not coercible to each '
          f'other. {left.return_type} {right.return_type}'
      )
    super().__init__(fhir_context, left, right, return_type)


class StructureBaseNode(ExpressionNode):
  """Returns nodes built from a StructureDefinition."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      return_type: Optional[_fhir_path_data_types.FhirPathDataType],
  ) -> None:
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return []

  def get_root_node(self) -> 'ExpressionNode':
    return self

  @property
  def parent_node(self):
    return None

  def to_path_token(self) -> str:
    # The FHIRPath of a root structure is simply the base type name,
    # so return that if it exists.
    if not self.return_type:
      return ''

    if not hasattr(self.return_type, 'base_type'):
      return str(self.return_type)

    return self.return_type.base_type

  def to_fhir_path(self) -> str:
    return self.to_path_token()

  @property
  def operands(self) -> List[ExpressionNode]:
    return []

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    # No operands to replace
    pass

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return None


class RootMessageNode(StructureBaseNode):
  """Returns the root node of the workspace."""

  def get_resource_nodes(self) -> List[ExpressionNode]:
    # Only RootMessageNodes are considered to be resource nodes.
    return [self]

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_root(self)


class LiteralNode(ExpressionNode):
  """Node expressing a literal FHIRPath value."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      value: Optional[message.Message],
      fhir_path_str: str,
      return_type: _fhir_path_data_types.FhirPathDataType,
  ) -> None:
    if value:
      primitive_type = annotation_utils.is_primitive_type(value)
      valueset_type = (
          annotation_utils.is_resource(value)
          and annotation_utils.get_structure_definition_url(value)
          == VALUE_SET_URL
      )
      quantity_type = (
          annotation_utils.get_structure_definition_url(value) == QUANTITY_URL
      )
      if not (primitive_type or valueset_type or quantity_type):
        raise ValueError(
            'LiteralNode should be a primitive, a quantity or a valueset, '
            f'instead, is: {value}'
        )  # pytype: disable=attribute-error

    self._value = value
    self._fhir_path_str = fhir_path_str
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return []

  def get_root_node(self) -> ExpressionNode:
    return self  # maybe return none instead.

  @property
  def parent_node(self):
    return None

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_literal(self)

  def get_value(self) -> message.Message:
    """Returns a defensive copy of the literal value."""
    return copy.deepcopy(self._value)

  def to_path_token(self) -> str:
    return self._fhir_path_str

  def to_fhir_path(self) -> str:
    return self.to_path_token()

  @property
  def operands(self) -> List[ExpressionNode]:
    return []

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    # No operands to replace
    pass


class InvokeExpressionNode(ExpressionNode):
  """Handles the FHIRPath InvocationExpression."""

  _identifier: str
  _parent_node: ExpressionNode

  def __new__(cls, *args) -> 'InvokeExpressionNode':
    """Creates a new InvokeExpressionNode node or one of its subclasses.

    Creates either an InvokeExpressionNode or InvokeReferenceNode, a subclass of
    InvokeExpressionNode. The InvokeReferenceNode is returned when a field named
    'reference' is invoked against a FHIR Reference resource. Database backends
    have special behavior for reference nodes. This reference-specific node type
    allows them to define visitors to implement their reference-specific logic.

    Args:
      *args: The args passed to `__init__`.

    Returns:
      A new InvokeExpressionNode of the appropriate type.
    """
    # For special initializations, such as via a copy, no args are supplied.
    if not args:
      return super().__new__(cls)

    # For regular initialization, the arguments will be the same as __init__.
    _, identifier, parent_node = args
    if identifier == 'reference' and isinstance(
        parent_node.return_type,
        _fhir_path_data_types.ReferenceStructureDataType,
    ):
      return super().__new__(InvokeReferenceNode)

    return super().__new__(InvokeExpressionNode)

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      identifier: str,
      parent_node: ExpressionNode,
  ) -> None:
    self._identifier = identifier
    self._parent_node = parent_node
    return_type = None
    if self._identifier == '$this':
      return_type = self._parent_node.return_type
    else:
      return_type = fhir_context.get_child_data_type(
          self._parent_node.return_type, self._identifier
      )

    if not return_type:
      raise ValueError(
          f'Identifier {identifier} cannot be extracted from parent node'
          f' {self._parent_node}'
      )
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self.parent_node.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self.parent_node.get_root_node()

  @property
  def parent_node(self) -> ExpressionNode:
    return self._parent_node

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_invoke_expression(self)

  @property
  def identifier(self) -> str:
    return self._identifier

  def to_path_token(self) -> str:
    if self.identifier == '$this':
      return self._parent_node.to_path_token()
    return self.identifier

  def to_fhir_path(self) -> str:
    # Exclude the root message name from the FHIRPath, following conventions.
    if self.identifier == '$this':
      return self._parent_node.to_fhir_path()
    elif isinstance(self._parent_node, StructureBaseNode) or isinstance(
        self._parent_node, ReferenceNode
    ):
      return self.identifier
    else:
      return self.parent_node.to_fhir_path() + '.' + self.identifier

  @property
  def operands(self) -> List[ExpressionNode]:
    return [self.parent_node]

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    if self._parent_node.to_fhir_path() == expression_to_replace:
      self._parent_node = replacement
    else:
      self._parent_node.replace_operand(expression_to_replace, replacement)


class InvokeReferenceNode(InvokeExpressionNode):
  """An invocation of a 'reference' field against a FHIR Reference resource.

  See `InvokeExpressionNode.__new__` for more details.
  """

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_invoke_reference(self)


class IndexerNode(ExpressionNode):
  """Handles the indexing operation."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      collection: ExpressionNode,
      index: LiteralNode,
  ) -> None:
    if not isinstance(index.return_type, _fhir_path_data_types._Integer):
      raise ValueError(
          f'Expected index type to be Integer. Got {index.return_type} instead.'
      )
    self._collection = collection
    self._index = index
    return_type = collection.return_type.get_new_cardinality_type(
        _fhir_path_data_types.Cardinality.SCALAR
    )
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return (
        self.collection.get_resource_nodes() + self.index.get_resource_nodes()
    )

  def get_root_node(self) -> ExpressionNode:
    return self.collection.get_root_node()

  @property
  def parent_node(self) -> ExpressionNode:
    return self.collection

  @property
  def collection(self) -> ExpressionNode:
    return self._collection  # pytype: disable=attribute-error

  @property
  def index(self) -> LiteralNode:
    return self._index  # pytype: disable=attribute-error

  def to_fhir_path(self) -> str:
    return f'{self._collection.to_fhir_path()}[{self.index.to_fhir_path()}]'

  @property
  def operands(self) -> List[ExpressionNode]:
    return [self.collection, self.index]

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    if self.collection.to_fhir_path() == expression_to_replace:
      self._collection = replacement
    else:
      self.collection.replace_operand(expression_to_replace, replacement)

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_indexer(self)


class NumericPolarityNode(ExpressionNode):
  """Numeric polarity support."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      polarity: _ast.Polarity,
  ) -> None:
    if operand.return_type and not _fhir_path_data_types.is_numeric(
        operand.return_type
    ):
      raise ValueError(
          f'Operand must be of numeric type. {operand.return_type}'
      )
    self._operand = operand
    self._polarity = polarity
    super().__init__(fhir_context, operand.return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self._operand.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self._operand.get_root_node()

  @property
  def parent_node(self) -> ExpressionNode:
    return self._operand

  @property
  def op(self) -> str:
    return self._polarity.op

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_polarity(self)

  def to_fhir_path(self) -> str:
    return f'{str(self._polarity.op)} {self._operand.to_fhir_path()}'

  @property
  def operands(self) -> List[ExpressionNode]:
    return [self._operand]

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    if self._operand.to_fhir_path() == expression_to_replace:
      self._operand = replacement
    else:
      self._operand.replace_operand(expression_to_replace, replacement)


class FunctionNode(ExpressionNode):
  """Base class for FHIRPath function calls.

  Subclasses of this should validate parameters in their constructors and raise
  a ValueError if the parameters do not meet function needs.
  """

  NAME: str

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
      return_type: Optional[_fhir_path_data_types.FhirPathDataType] = None,
  ) -> None:
    super().__init__(fhir_context, return_type)
    self._operand = operand
    self._params = params

  def get_resource_nodes(self) -> List[ExpressionNode]:
    result = self._operand.get_resource_nodes()
    for p in self._params:
      result += p.get_resource_nodes()
    return result

  def get_root_node(self) -> ExpressionNode:
    return self._operand.get_root_node()

  @property
  def parent_node(self) -> ExpressionNode:
    return self._operand

  def to_fhir_path(self) -> str:
    param_str = ', '.join([param.to_fhir_path() for param in self._params])
    # Exclude the root message name from the FHIRPath, following conventions.
    if isinstance(self._operand, StructureBaseNode):
      return f'{self.NAME}({param_str})'
    else:
      return f'{self._operand.to_fhir_path()}.{self.NAME}({param_str})'

  def params(self) -> List[ExpressionNode]:
    return self._params

  @property
  def operands(self) -> List[ExpressionNode]:
    return [self._operand] + self._params

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    if self._operand.to_fhir_path() == expression_to_replace:
      self._operand = replacement
    else:
      self._operand.replace_operand(expression_to_replace, replacement)

    for index, item in enumerate(self._params):
      if item.to_fhir_path() == expression_to_replace:
        self._params[index] = replacement
      else:
        self._params[index].replace_operand(expression_to_replace, replacement)

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_function(self)


class ExistsFunction(FunctionNode):
  """Implementation of the exists() function."""

  NAME = 'exists'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Boolean
    )


class CountFunction(FunctionNode):
  """Implementation of the count() function."""

  NAME = 'count'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Integer
    )


class EmptyFunction(FunctionNode):
  """Implementation of the empty() function."""

  NAME = 'empty'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Boolean
    )


class FirstFunction(FunctionNode):
  """Implementation of the first() function."""

  NAME = 'first'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    super().__init__(
        fhir_context,
        operand,
        params,
        operand.return_type.get_new_cardinality_type(
            _fhir_path_data_types.Cardinality.SCALAR
        ),
    )


class AnyTrueFunction(FunctionNode):
  """Implementation of the anyTrue() function."""

  NAME = 'anyTrue'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    if not operand.return_type.returns_collection() or not isinstance(
        operand.return_type, _fhir_path_data_types._Boolean
    ):
      raise ValueError(
          'anyTrue() must be called on a Collection of booleans. '
          f'Got type of {operand.return_type}.'
      )

    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Boolean
    )


class HasValueFunction(FunctionNode):
  """Implementation of the hasValue() function."""

  NAME = 'hasValue'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Boolean
    )


# TODO(b/220344555): Fully define this placeholder for more than analytic use.
class IdForFunction(FunctionNode):
  """idFor() implementation to get raw ids specific to a reference type.

  For example, observation.subject.idFor('Patient') returns the raw id for
  the patient resource, rather than the typical 'Patient/<id>' string. This
  is convenient for tools that join data based on IDs, such as SQL or
  dataframe tables.
  """

  NAME = 'idFor'
  base_type_str: str
  struct_def_url: str

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    # TODO(b/244184211): Resolve typing for idFor function.
    if not (
        len(params) == 1
        and isinstance(params[0], LiteralNode)
        and fhir_types.is_string(cast(LiteralNode, params[0]).get_value())
    ):
      raise ValueError(
          'IdFor function requires a single parameter of the resource type.'
      )

    if isinstance(
        operand.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      raise ValueError('idFor() does not support operating on a choice type.')
    # Determine the expected FHIR type to use as the node's return type.
    type_param_str = cast(Any, params[0]).get_value().value

    parsed = urllib.parse.urlparse(type_param_str)
    if parsed.scheme and parsed.netloc and parsed.path:
      # It's a URI such as 'http://hl7.org/fhir/StructureDefinition/Patient.'
      self.struct_def_url = type_param_str
      return_type = fhir_context.get_fhir_type_from_string(
          # type_code will be found by inspecting the struct def.
          type_code=None,
          profile=self.struct_def_url,
          element_definition=None,
      )
      self.base_type_str = return_type.base_type
    else:
      # It's a bare type name such as 'Patient.'
      # Trim the FHIR prefix used for primitive types, if applicable.
      self.base_type_str = (
          type_param_str[5:]
          if type_param_str.startswith('FHIR.')
          else type_param_str
      )
      # Ensure the first character is capitalized.
      self.base_type_str = (
          self.base_type_str[:1].upper() + self.base_type_str[1:]
      )
      self.struct_def_url = (
          f'http://hl7.org/fhir/StructureDefinition/{self.base_type_str}'
      )
      return_type = fhir_context.get_fhir_type_from_string(
          type_code=self.base_type_str,
          profile=self.struct_def_url,
          element_definition=None,
      )

    super().__init__(fhir_context, operand, params, return_type)


class OfTypeFunction(FunctionNode):
  """ofType() implementation that returns only members of the given type."""

  NAME = 'ofType'
  struct_def_url: str
  base_type_str: str

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    if not (
        len(params) == 1
        and isinstance(params[0], LiteralNode)
        and fhir_types.is_string(cast(LiteralNode, params[0]).get_value())
    ):
      raise ValueError(
          'ofType function requires a single parameter of the datatype.'
      )

    # Determine the expected FHIR type to use as the node's return type.
    type_param_str = cast(Any, params[0]).get_value().value

    # Trim the FHIR prefix used for primitive types, if applicable.
    self.base_type_str = (
        type_param_str[5:]
        if type_param_str.startswith('FHIR.')
        else type_param_str
    )
    self.struct_def_url = (
        f'http://hl7.org/fhir/StructureDefinition/{self.base_type_str}'
    )

    return_type = _fhir_path_data_types.Empty
    if isinstance(
        operand.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      if (
          self.base_type_str.casefold()
          in cast(
              _fhir_path_data_types.PolymorphicDataType, operand.return_type
          ).fields()
      ):
        return_type = operand.return_type.types[self.base_type_str.casefold()]
    else:
      return_type = fhir_context.get_child_data_type(
          operand.return_type, self.base_type_str
      )

    if _fhir_path_data_types.returns_collection(operand.return_type):
      return_type = return_type.get_new_cardinality_type(
          _fhir_path_data_types.Cardinality.CHILD_OF_COLLECTION
      )

    super().__init__(fhir_context, operand, params, return_type)


class MemberOfFunction(FunctionNode):
  """Implementation of the memberOf() function."""

  # Literal valueset URL and values to check for memberOf operations.
  NAME = 'memberOf'
  value_set_url: str
  value_set_version: Optional[str] = None
  code_values: Optional[FrozenSet[CodeValue]] = None
  code_values_lock = threading.Lock()

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    if not (
        isinstance(operand.return_type, _fhir_path_data_types._String)
        or _fhir_path_data_types.is_coding(operand.return_type)
        or _fhir_path_data_types.is_codeable_concept(operand.return_type)
    ):
      raise ValueError(
          'MemberOf must be called on a string, code, coding, or codeable '
          'concept, not %s'
          % operand.return_type
      )

    if len(params) != 1 or not isinstance(params[0], LiteralNode):
      raise ValueError(
          'MemberOf requires single valueset URL or proto parameter.'
      )

    value = cast(Any, params[0]).get_value()

    # If the parameter is a ValueSet literal, load it into a set for
    # efficient evaluation.
    if annotation_utils.get_structure_definition_url(value) == VALUE_SET_URL:
      self.value_set_url = value.url.value
      self.value_set_version = value.version.value or None
      self.code_values = to_code_values(value)
    elif annotation_utils.is_primitive_type(value) and isinstance(
        value.value, str
    ):
      # The parameter is a URL to a valueset, so preserve it for evaluation
      # engines to resolve.
      self.value_set_url = value.value
      parsed = urllib.parse.urlparse(self.value_set_url)
      if not parsed.scheme and parsed.path:
        raise ValueError(
            'memberOf must be called with a valid URI, not'
            f' {self.value_set_url}'
        )

    else:
      raise ValueError(
          'MemberOf requires single valueset URL or proto parameter.'
      )
    return_type = _fhir_path_data_types.Boolean

    if operand.return_type.returns_collection():
      return_type = return_type.get_new_cardinality_type(
          _fhir_path_data_types.Cardinality.CHILD_OF_COLLECTION
      )
    super().__init__(fhir_context, operand, params, return_type)

  def to_value_set_codes(
      self, fhir_context: context.FhirPathContext
  ) -> Optional[ValueSetCodes]:
    """Builds a representation of the value set given to the memberOf call.

    If memberOf was called with a value set proto, returns a ValueSetCodes
    object using the fields from that proto.
    If memberOf was called with a URL string, attempt to retrieve a value set
    proto from `fhir_context` and use it to build the ValueSetCodes object.
    If the URL string can not be resolved in the given `fhir_context`, returns
    None.

    Args:
      fhir_context: The context to use when looking for value set definitions.

    Returns:
      The value set referenced by the memberOf call or None if the value set URL
      can not be resolved.
    """
    if self.code_values is not None:
      return ValueSetCodes(
          self.value_set_url, self.value_set_version, self.code_values
      )

    value_set_proto = fhir_context.get_value_set(self.value_set_url)
    if value_set_proto is None:
      return None

    return ValueSetCodes(
        self.value_set_url,
        value_set_proto.version.value or None,
        to_code_values(value_set_proto),
    )


class NotFunction(FunctionNode):
  """Implementation of the not_() function."""

  NAME = 'not'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    if operand.return_type != _fhir_path_data_types.Boolean:
      raise ValueError(
          'not() was called on an operand that does not return Boolean(s).'
      )
    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Boolean
    )
    if params:
      raise ValueError(
          'not() function should not have any parameters but has '
          f'{str(len(params))} parameters.'
      )


class WhereFunction(FunctionNode):
  """Implementation of the where() function."""

  NAME = 'where'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    _check_is_predicate(self.NAME, params)
    super().__init__(fhir_context, operand, params, operand.return_type)


class AllFunction(FunctionNode):
  """Implementation of the all() function."""

  NAME = 'all'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    _check_is_predicate(self.NAME, params)
    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Boolean
    )


class MatchesFunction(FunctionNode):
  """Implementation of the matches() function."""

  pattern = None
  NAME = 'matches'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:

    if isinstance(
        operand.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      raise ValueError('matches() does not operate on a choice type.')
    if not params:
      regex = None
    elif not (
        isinstance(params[0], LiteralNode)
        and fhir_types.is_string(cast(LiteralNode, params[0]).get_value())
    ):
      raise ValueError('matches() requires a single string parameter.')
    else:
      regex = cast(Any, params[0]).get_value().value
    self.pattern = re.compile(regex) if regex else None
    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Boolean
    )


class ToIntegerFunction(FunctionNode):
  """Implementation of the toInteger() function.

  The spec for this function is found here:
  https://build.fhir.org/ig/HL7/FHIRPath/#integer-conversion-functions
  """

  NAME = 'toInteger'

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operand: ExpressionNode,
      params: List[ExpressionNode],
  ) -> None:
    if isinstance(
        operand.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      raise ValueError('toInteger() does not operate on a choice type.')
    if params:
      raise ValueError('toInteger() does not accept any parameters.')

    super().__init__(
        fhir_context, operand, params, _fhir_path_data_types.Integer
    )


class EqualityNode(CoercibleBinaryExpressionNode):
  """Implementation of FHIRPath equality and equivalence operators."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operator: _ast.EqualityRelation.Op,
      left: ExpressionNode,
      right: ExpressionNode,
  ) -> None:
    self._operator = operator
    super().__init__(fhir_context, left, right, _fhir_path_data_types.Boolean)

  @property
  def op(self) -> _ast.EqualityRelation.Op:
    return self._operator

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_equality(self)

  def to_fhir_path(self) -> str:
    return (
        f'{self._left.to_fhir_path()} {self._operator.value} {self._right.to_fhir_path()}'
    )


class BooleanOperatorNode(BinaryExpressionNode):
  """Implementation of FHIRPath boolean operations.

  See https://hl7.org/fhirpath/#boolean-logic for behavior definition.
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operator: _ast.BooleanLogic.Op,
      left: ExpressionNode,
      right: ExpressionNode,
  ) -> None:
    self._operator = operator
    super().__init__(fhir_context, left, right, _fhir_path_data_types.Boolean)

  @property
  def op(self) -> _ast.BooleanLogic.Op:
    return self._operator

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_boolean_op(self)

  def to_fhir_path(self) -> str:
    return (
        f'{self._left.to_fhir_path()} {self._operator.value} '
        f'{self._right.to_fhir_path()}'
    )


class ArithmeticNode(CoercibleBinaryExpressionNode):
  """Implementation of FHIRPath arithmetic operations.

  See https://hl7.org/fhirpath/#math-2 for behavior definition.
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operator: _ast.Arithmetic.Op,
      left: ExpressionNode,
      right: ExpressionNode,
  ) -> None:

    if isinstance(
        left.return_type, _fhir_path_data_types.PolymorphicDataType
    ) or isinstance(
        right.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      raise ValueError(
          f'{self.__class__.__name__} does not support polymorphic data types.'
      )

    self._operator = operator
    return_type = left.return_type if left else right.return_type
    super().__init__(fhir_context, left, right, return_type)

  @property
  def op(self) -> _ast.Arithmetic.Op:
    return self._operator

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_arithmetic(self)

  def to_fhir_path(self) -> str:
    return (
        f'{self._left.to_fhir_path()} {self._operator.value} '
        f'{self._right.to_fhir_path()}'
    )


class ComparisonNode(CoercibleBinaryExpressionNode):
  """Implementation of the FHIRPath comparison functions."""

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      operator: _ast.Comparison.Op,
      left: ExpressionNode,
      right: ExpressionNode,
  ) -> None:
    if isinstance(
        left.return_type, _fhir_path_data_types.PolymorphicDataType
    ) or isinstance(
        right.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      raise ValueError(
          f'{self.__class__.__name__} does not support polymorphic data types.'
      )

    self._operator = operator
    super().__init__(fhir_context, left, right, _fhir_path_data_types.Boolean)

  @property
  def op(self) -> _ast.Comparison.Op:
    return self._operator

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_comparison(self)

  def to_fhir_path(self) -> str:
    return (
        f'{self._left.to_fhir_path()} {self._operator.value} '
        f'{self._right.to_fhir_path()}'
    )


class ReferenceNode(ExpressionNode):
  """Implementation of $this keyword and relative paths.

  ReferenceNodes are used as a way to indicate that the FHIRPath has already
  been used in a previous context in the FHIRPath. For example, in the FHIRPath
    Patient.name.all(Patient.name.matches('regex'))

  The second Patient.name gets internally substituted for a ReferenceNode to
  Patient.name. The interpreter can then recognize that Patient.name has already
  been resolved and does not need to reresolved if it so chooses.

  An optional argument `element_of_array` will also set the cardinality of the
  return type to be a CHILD_OF_COLLECTION if the original operand returns a
  collection because some functions inherently map the expression in the params
  to each element of the operand. In the above example, Patient.name is a
  collection according to the FHIR spec, but matches only operates on scalars.
  In order to check the match of each value in name, all() is used on
  Patient.name, the second Patient.name is then an unnested reference to the
  original Patient.name.
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      reference_node: ExpressionNode,
      element_of_array: bool = False,
  ) -> None:
    self._reference_node = reference_node
    # If the reference node/caller is a function, then the actual node being
    # referenced is the first non-function caller.
    while isinstance(self._reference_node, FunctionNode):
      self._reference_node = self._reference_node.parent_node

    return_type = reference_node.return_type
    if element_of_array and return_type.returns_collection():
      return_type = return_type.get_new_cardinality_type(
          _fhir_path_data_types.Cardinality.CHILD_OF_COLLECTION
      )
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self._reference_node.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self._reference_node.get_root_node()

  @property
  def parent_node(self) -> ExpressionNode:
    return self._reference_node

  @property
  def operands(self) -> List[ExpressionNode]:
    return [self._reference_node]

  def replace_operand(
      self, expression_to_replace: str, replacement: 'ExpressionNode'
  ) -> None:
    if self._reference_node.to_fhir_path() == expression_to_replace:
      self._reference_node = replacement

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_reference(self)

  def to_fhir_path(self) -> str:
    return self._reference_node.to_fhir_path()


class MembershipRelationNode(CoercibleBinaryExpressionNode):
  """Parent class for In and Contains Nodes."""

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_membership(self)


class InNode(MembershipRelationNode):
  """Implementation of the FHIRPath in operator.

  The spec for the in operator is taken from:
  https://fhirpath.readthedocs.io/en/latest/fhirpath.html#fhirpath.fhirpath.FHIRPath.in_
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      left: ExpressionNode,
      right: ExpressionNode,
  ) -> None:
    super().__init__(fhir_context, left, right, _fhir_path_data_types.Boolean)

  def to_fhir_path(self) -> str:
    return f'{self._left.to_fhir_path()} in {self._right.to_fhir_path()}'


class ContainsNode(MembershipRelationNode):
  """Implementation of the FHIRPath contains operator.

  This is the converse operation of in.
  The spec for the contains operator is taken from:
  https://fhirpath.readthedocs.io/en/latest/fhirpath.html#fhirpath.fhirpath.FHIRPath.contained
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      left: ExpressionNode,
      right: ExpressionNode,
  ) -> None:
    super().__init__(fhir_context, left, right, _fhir_path_data_types.Boolean)

  def to_fhir_path(self) -> str:
    return f'{self._left.to_fhir_path()} contains {self._right.to_fhir_path()}'


class UnionNode(BinaryExpressionNode):
  """Implementation of the FHIRPath union operator.

  The spec for the union operator is taken from:
  https://build.fhir.org/ig/HL7/FHIRPath/#union-collections
  https://build.fhir.org/ig/HL7/FHIRPath/#unionother-collection
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      left: ExpressionNode,
      right: ExpressionNode,
  ) -> None:
    left_type = left.return_type
    right_type = right.return_type

    if isinstance(left_type, _fhir_path_data_types.Empty.__class__):
      return_type = right_type
    elif isinstance(right_type, _fhir_path_data_types.Empty.__class__):
      return_type = left_type
    elif right_type == left_type:
      return_type = left_type
    else:
      # We're union-ing two different types of collection, so the
      # resulting type is a union of both side's type.
      types_union: Set[_fhir_path_data_types.FhirPathDataType] = set()
      for node_type in (left_type, right_type):
        if isinstance(node_type, _fhir_path_data_types.Collection):
          types_union.update(node_type.types)
        else:
          types_union.add(node_type)
      return_type = _fhir_path_data_types.Collection(types=types_union)

    super().__init__(fhir_context, left, right, return_type)

  def to_fhir_path(self) -> str:
    return f'{self._left.to_fhir_path()} | {self._right.to_fhir_path()}'

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_union(self)


# Implementations of FHIRPath functions.
_FUNCTION_NODE_MAP: Dict[str, Any] = {
    'all': AllFunction,
    'exists': ExistsFunction,
    'count': CountFunction,
    'empty': EmptyFunction,
    'matches': MatchesFunction,
    'not': NotFunction,
    'first': FirstFunction,
    'hasValue': HasValueFunction,
    'idFor': IdForFunction,
    'memberOf': MemberOfFunction,
    'ofType': OfTypeFunction,
    'where': WhereFunction,
    'anyTrue': AnyTrueFunction,
    'toInteger': ToIntegerFunction,
}


class ExpressionNodeBaseVisitor(abc.ABC):
  """Abstract base class that visits the Expression Nodes."""

  def visit(self, node: ExpressionNode) -> Any:
    return node.accept(self)

  def visit_operands(self, node: ExpressionNode) -> Any:
    result: List[Any] = []
    for c in node.operands:
      result.append(c.accept(self))
    return result

  @abc.abstractmethod
  def visit_root(self, root: RootMessageNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_reference(self, reference: ExpressionNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_literal(self, literal: LiteralNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_invoke_expression(self, identifier: InvokeExpressionNode) -> Any:
    pass

  def visit_invoke_reference(self, identifier: InvokeReferenceNode) -> Any:
    """Allows visitors to implement custom Reference logic.

    By default, calls `visit_invoke_expression`. Subclasses may override this
    method to introduce custom logic for handling references.

    This function is called when the 'reference' identifier is invoked against a
    FHIR Reference resource. The visit_invoke_expression function is called for
    all other invocations.

    Args:
      identifier: The identifier on the right hand side of an invocation.

    Returns:
      The result of the reference invocation.
    """
    return self.visit_invoke_expression(identifier)

  @abc.abstractmethod
  def visit_indexer(self, indexer: IndexerNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_arithmetic(self, arithmetic: ArithmeticNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_equality(self, equality: EqualityNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_comparison(self, comparison: ComparisonNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_boolean_op(self, boolean_logic: BooleanOperatorNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_membership(self, relation: MembershipRelationNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_union(self, union: UnionNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_polarity(self, polarity: NumericPolarityNode) -> Any:
    pass

  @abc.abstractmethod
  def visit_function(self, function: FunctionNode) -> Any:
    pass


class FhirPathCompilerVisitor(_ast.FhirPathAstBaseVisitor):
  """AST visitor to compile a FHIRPath expression."""

  def __init__(
      self,
      handler: primitive_handler.PrimitiveHandler,
      fhir_context: context.FhirPathContext,
      data_type: Optional[_fhir_path_data_types.FhirPathDataType] = None,
      root_node_context: Optional[ExpressionNode] = None,
  ) -> None:
    self._handler = handler
    self._context = fhir_context
    self._data_type = data_type
    if root_node_context:
      self._node_context = [root_node_context]
    else:
      self._node_context = [RootMessageNode(self._context, self._data_type)]

  def visit_literal(self, literal: _ast.Literal) -> LiteralNode:
    if literal.value is None:
      return LiteralNode(self._context, None, '{}', _fhir_path_data_types.Empty)
    elif isinstance(literal.value, bool):
      return LiteralNode(
          self._context,
          self._handler.new_boolean(literal.value),
          str(literal.value),
          _fhir_path_data_types.Boolean,
      )
    elif isinstance(literal.value, int):
      return LiteralNode(
          self._context,
          self._handler.new_integer(literal.value),
          str(literal.value),
          _fhir_path_data_types.Integer,
      )
    elif isinstance(literal.value, float):
      return LiteralNode(
          self._context,
          self._handler.new_decimal(literal.value),
          str(literal.value),
          _fhir_path_data_types.Decimal,
      )
    elif isinstance(literal.value, str):
      if literal.is_date_type:
        primitive_cls = self._handler.date_cls
        fhir_type = _fhir_path_data_types.Date
        literal_str = literal.value
        if 'T' in literal.value:
          primitive_cls = self._handler.date_time_cls
          fhir_type = _fhir_path_data_types.DateTime
          # Some datetime strings might not have a timezone specified which
          # messes up the primitive datetime constructor because it expects a
          # timezone.
          datetime_obj = datetime.datetime.fromisoformat(literal.value)
          if not datetime_obj.tzinfo:
            datetime_obj = datetime_obj.replace(tzinfo=zoneinfo.ZoneInfo('UTC'))
            literal_str = datetime_obj.isoformat()
        return LiteralNode(
            self._context,
            self._handler.primitive_wrapper_from_json_value(
                literal_str, primitive_cls
            ).wrapped,
            f'@{literal_str}',
            fhir_type,
        )
      else:
        return LiteralNode(
            self._context,
            self._handler.new_string(literal.value),
            f"'{literal.value}'",
            _fhir_path_data_types.String,
        )
    elif isinstance(literal.value, decimal.Decimal):
      return LiteralNode(
          self._context,
          self._handler.new_decimal(str(literal.value)),
          str(literal.value),
          _fhir_path_data_types.Decimal,
      )
    elif isinstance(literal.value, _ast.Quantity):
      return LiteralNode(
          self._context,
          self._handler.new_quantity(literal.value.value, literal.value.unit),
          str(literal.value),
          _fhir_path_data_types.Quantity,
      )
    else:
      raise ValueError(
          f'Unsupported literal value: {literal} {type(literal.value)}.'
      )

  def visit_identifier(self, identifier: _ast.Identifier) -> Any:
    if identifier.value == '$this':
      return self._node_context[-1]

    return InvokeExpressionNode(
        self._context, identifier.value, self._node_context[-1]
    )

  def visit_indexer(self, indexer: _ast.Indexer, **kwargs: Any) -> Any:
    collection_result = self.visit(indexer.collection)
    index_result = self.visit(indexer.index)
    return IndexerNode(self._context, collection_result, index_result)

  def visit_arithmetic(self, arithmetic: _ast.Arithmetic, **kwargs: Any) -> Any:
    left = self.visit(arithmetic.lhs)
    right = self.visit(arithmetic.rhs)

    return ArithmeticNode(self._context, arithmetic.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_type_expression(
      self, type_expression: _ast.TypeExpression, **kwargs: Any
  ) -> Any:
    raise NotImplementedError('TODO: implement `visit_type_expression`.')

  def visit_equality(
      self, equality: _ast.EqualityRelation, **kwargs: Any
  ) -> Any:
    left = self.visit(equality.lhs)
    right = self.visit(equality.rhs)

    return EqualityNode(self._context, equality.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_comparison(self, comparison: _ast.Comparison, **kwargs: Any) -> Any:
    left = self.visit(comparison.lhs)
    right = self.visit(comparison.rhs)

    return ComparisonNode(self._context, comparison.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_boolean_logic(
      self, boolean_logic: _ast.BooleanLogic, **kwargs: Any
  ) -> Any:
    left = self.visit(boolean_logic.lhs)
    right = self.visit(boolean_logic.rhs)

    return BooleanOperatorNode(self._context, boolean_logic.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_membership(
      self, membership: _ast.MembershipRelation, **kwargs: Any
  ) -> Any:
    left = self.visit(membership.lhs)
    right = self.visit(membership.rhs)

    if membership.op == membership.Op.CONTAINS:
      return ContainsNode(self._context, left, right)
    elif membership.op == membership.Op.IN:
      return InNode(self._context, left, right)
    else:
      raise ValueError(f'Unknown membership operator "{membership.op}".')

  def visit_union(self, union: _ast.UnionOp, **kwargs: Any) -> Any:
    return UnionNode(
        self._context, self.visit(union.lhs), self.visit(union.rhs)
    )

  def visit_polarity(self, polarity: _ast.Polarity) -> ExpressionNode:
    operand_node = self.visit(polarity.operand)

    # If the operand is a literal, produce a new negated literal if needed.
    # Polarity must be applied at expression evaluation time for non-literals.
    if isinstance(operand_node, LiteralNode):
      modified_value = operand_node.get_value()
      if polarity.op == _ast.Polarity.Op.NEGATIVE:
        # Decimal types are stored as strings, so simply add a negation prefix.
        if isinstance(modified_value.value, str):
          modified_value.value = f'-{modified_value.value}'
        else:
          modified_value.value = -1 * modified_value.value
      return LiteralNode(
          self._context,
          modified_value,
          f'{polarity.op}{operand_node.to_fhir_path()}',
          _fhir_path_data_types.Decimal,
      )
    else:
      return NumericPolarityNode(self._context, operand_node, polarity)

  def visit_invocation(self, invocation: _ast.Invocation) -> ExpressionNode:
    # TODO(b/244184211): Placeholder for limited invocation usage.
    # Function invocation

    if isinstance(invocation.rhs, _ast.Function):
      return self.visit_function(invocation.rhs, operand=invocation.lhs)

    # If the invokee is the original Resource, return the root node rather than
    # calling invoke.
    if str(invocation.lhs) == self._node_context[-1].to_fhir_path():
      lhs_result = self._node_context[-1]
    else:
      lhs_result = self.visit(invocation.lhs)

    return InvokeExpressionNode(self._context, str(invocation.rhs), lhs_result)

  def visit_function(
      self, function: _ast.Function, operand: Optional[_ast.Expression] = None
  ) -> FunctionNode:
    function_name = function.identifier.value
    function_class = _FUNCTION_NODE_MAP.get(function_name)
    if function_class is None:
      raise NotImplementedError(f'Function {function_name} not implemented.')

    # Use the given operand if it exists, otherwise this must have been invoked
    # on the root, so that is the effective operand.
    operand_node = (
        self.visit(operand) if operand is not None else self._node_context[-1]
    )
    params: List[ExpressionNode] = []
    # Mapping function like all() and where are functions that apply the params
    # to each element of the operand if the operand is a collection so the
    # return type of the reference would be a reference to an element of the
    # array rather than the whole array itself..
    element_of_array = function_name in _MAPPING_FUNCTIONS
    # For functions, the identifiers can be relative to the operand of the
    # function; not the root FHIR type.
    self._node_context.append(
        ReferenceNode(
            self._context, operand_node, element_of_array=element_of_array
        )
    )
    for param in function.params:
      new_param = self.visit(param)
      params.append(new_param)
    self._node_context.pop()
    return function_class(self._context, operand_node, params)

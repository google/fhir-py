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
import itertools
import re
import threading
from typing import Any, Dict, FrozenSet, List, Optional, Set, cast
import urllib

from google.protobuf import descriptor
from google.protobuf import message
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import quantity
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import annotation_utils
from google.fhir.core.utils import fhir_types
from google.fhir.core.utils import proto_utils

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


def get_messages(parent: message.Message,
                 json_name: str) -> List[message.Message]:
  """Gets the child messages in the field with the given JSON name."""
  target_field = None

  for field in parent.DESCRIPTOR.fields:
    if field.json_name == json_name:
      target_field = field
      break

  if (target_field is None or
      not proto_utils.field_is_set(parent, target_field.name)):
    return []

  results = proto_utils.get_value_at_field(parent, target_field.name)

  # Wrap non-repeated items in an array per FHIRPath specification.
  if target_field.label != descriptor.FieldDescriptor.LABEL_REPEATED:
    return [results]
  else:
    return results


def _is_numeric(
    message_or_descriptor: annotation_utils.MessageOrDescriptorBase) -> bool:
  return (fhir_types.is_decimal(message_or_descriptor) or
          fhir_types.is_integer(message_or_descriptor) or
          fhir_types.is_positive_integer(message_or_descriptor) or
          fhir_types.is_unsigned_integer(message_or_descriptor))


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
  """Helper function to convert a ValueSet proto into a set of code value data types.
  """
  # TODO(b/208900793): Use a protocol for ValueSets to allow type checking.
  expansion = value_set_proto.expansion.contains  # pytype: disable=attribute-error
  codes = [
      CodeValue(code_elem.system.value, code_elem.code.value)
      for code_elem in expansion
  ]
  return frozenset(codes)


@dataclasses.dataclass
class WorkSpaceMessage:
  """Message with parent context, as needed for some FHIRPath expressions."""
  message: message.Message
  parent: Optional['WorkSpaceMessage']


@dataclasses.dataclass
class WorkSpace:
  """Working memory and context for evaluating FHIRPath expressions."""
  primitive_handler: primitive_handler.PrimitiveHandler
  fhir_context: context.FhirPathContext
  message_context_stack: List[WorkSpaceMessage]

  def root_message(self) -> WorkSpaceMessage:
    return self.message_context_stack[0]

  def current_message(self) -> WorkSpaceMessage:
    return self.message_context_stack[-1]

  def push_message(self, workspace_message: WorkSpaceMessage) -> None:
    self.message_context_stack.append(workspace_message)

  def pop_message(self) -> None:
    self.message_context_stack.pop()


class ExpressionNode(abc.ABC):
  """Abstract base class for all FHIRPath expression evaluation."""

  def __init__(self, fhir_context: context.FhirPathContext,
               return_type: _fhir_path_data_types.FhirPathDataType) -> None:
    self._return_type = copy.deepcopy(return_type)
    self._context = fhir_context

  @abc.abstractmethod
  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    """Evaluates the node and returns the resulting messages."""

  @abc.abstractmethod
  def to_fhir_path(self) -> str:
    """Returns the FHIRPath string for this and its children node."""

  def to_path_token(self) -> str:
    """Returns the path of the node itself."""
    return ''

  @property
  def context(self) -> context.FhirPathContext:
    return self._context

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

  @abc.abstractmethod
  def get_parent_node(self) -> 'ExpressionNode':
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
    if self._return_type:
      return self._return_type.fields()
    return set()

  @abc.abstractmethod
  def operands(self) -> List['ExpressionNode']:
    """Returns the operands contributing to this node."""

  @abc.abstractmethod
  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    """Replace any operand that matches the given expression string."""

  def __str__(self) -> str:
    return self.to_fhir_path()

  def _operand_to_string(self,
                         operand: 'ExpressionNode',
                         with_typing: bool,
                         indent: int = 0) -> str:
    """Function to recursively print the operands of the input operand."""
    operand_name = f'{operand} '
    if operand.__class__.__name__ == 'ReferenceNode':
      operand_name = ''
      operand_prints = f'&{operand}'
    else:
      operand_prints = ''.join(
          '\n' + self._operand_to_string(op, with_typing, indent + 1)
          for op in operand.operands())
    type_print = f' type={operand.return_type()}' if with_typing else ''
    return (f'{"| " * indent}+ '
            f'{operand_name}<{operand.__class__.__name__}{type_print}> ('
            f'{operand_prints})')

  def debug_string(self, with_typing: bool = False) -> str:
    """Returns debug string of the current node."""
    return self._operand_to_string(self, with_typing)


def _to_boolean(operand: List[WorkSpaceMessage]) -> Optional[bool]:
  """Converts an evaluation result to a boolean value or None.

  Args:
    operand: an expression operand result to convert to boolean.

  Returns:
    the boolean value, or None if the operand was empty.

  Raises:
    ValueError if it is not an empty result or a single, boolean value.
  """
  if not operand:
    return None
  if len(operand) > 1:
    raise ValueError('Expected a single boolean result but got multiple items.')
  if not fhir_types.is_boolean(operand[0].message):
    raise ValueError('Expected a boolean but got a non-boolean value.')
  return proto_utils.get_value_at_field(operand[0].message, 'value')


def _to_int(operand: List[WorkSpaceMessage]) -> Optional[int]:
  """Converts an evaluation result to an int value or None.

  Args:
    operand: an expression operand result to convert to int.

  Returns:
    the int value, or None if the operand was empty.

  Raises:
    ValueError if it is not an empty result or a single, int value.
  """
  if not operand:
    return None
  if len(operand) > 1:
    raise ValueError('Expected single int result but got multiple items.')
  if not fhir_types.is_integer(operand[0].message):
    raise ValueError('Expected single int but got a non-int value.')
  return proto_utils.get_value_at_field(operand[0].message, 'value')


def _check_is_predicate(function_name: str,
                        params: List[ExpressionNode]) -> None:
  """Raise an exception if expression params are a boolean predicate."""
  if len(params) != 1:
    raise ValueError((f'{function_name} expression require a single parameter,',
                      f' got {len(params)}'))

  if params[0].return_type() != _fhir_path_data_types.Boolean:
    raise ValueError((f'{function_name} expression require a boolean predicate',
                      f' got {params[0].to_fhir_path()}'))


class BinaryExpressionNode(ExpressionNode):
  """Base class for binary expressions."""

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               left: ExpressionNode, right: ExpressionNode,
               return_type: _fhir_path_data_types.FhirPathDataType) -> None:
    self._handler = handler
    self._left = left
    self._right = right
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self._left.get_resource_nodes() + self._right.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self._left.get_root_node()

  def get_parent_node(self) -> ExpressionNode:
    return self._left

  @property
  def left(self) -> ExpressionNode:
    return self._left

  @property
  def right(self) -> ExpressionNode:
    return self._right

  def operands(self) -> List[ExpressionNode]:
    return [self._left, self._right]

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
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

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               left: ExpressionNode, right: ExpressionNode,
               return_type: _fhir_path_data_types.FhirPathDataType) -> None:
    if not _fhir_path_data_types.is_coercible(left.return_type(),
                                              right.return_type()):
      raise ValueError(f'Left and right operands are not coercible to each '
                       f'other. {left.return_type()} {right.return_type()}')
    super().__init__(fhir_context, handler, left, right, return_type)


class StructureBaseNode(ExpressionNode):
  """Returns nodes built from a StructureDefinition."""

  def __init__(
      self, fhir_context: context.FhirPathContext,
      return_type: Optional[_fhir_path_data_types.FhirPathDataType]) -> None:
    self._struct_type = return_type
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return []

  def get_root_node(self) -> 'ExpressionNode':
    return self

  def get_parent_node(self):
    return None

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    return [work_space.root_message()]

  def to_path_token(self) -> str:
    # The FHIRPath of a root structure is simply the base type name,
    # so return that if it exists.
    return self._struct_type.base_type if self._struct_type else ''  # pytype: disable=attribute-error

  def to_fhir_path(self) -> str:
    return self.to_path_token()

  def operands(self) -> List[ExpressionNode]:
    return []

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
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

  def __init__(self, fhir_context: context.FhirPathContext,
               value: Optional[message.Message], fhir_path_str: str,
               return_type: _fhir_path_data_types.FhirPathDataType) -> None:
    if value:
      primitive_type = annotation_utils.is_primitive_type(value)
      valueset_type = (
          annotation_utils.is_resource(value) and
          annotation_utils.get_structure_definition_url(value) == VALUE_SET_URL)
      quantity_type = annotation_utils.get_structure_definition_url(
          value) == QUANTITY_URL
      if not (primitive_type or valueset_type or quantity_type):
        raise ValueError(
            f'LiteralNode should be a primitive, a quantity or a valueset, '
            f'instead, is: {value}')  # pytype: disable=attribute-error

    self._value = value
    self._fhir_path_str = fhir_path_str
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return []

  def get_root_node(self) -> ExpressionNode:
    return self  # maybe return none instead.

  def get_parent_node(self):
    return None

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    # Represent null as an empty list rather than a list with a None element.
    if self._value is None:
      return []

    return [
        WorkSpaceMessage(
            message=self.get_value(), parent=work_space.current_message())
    ]

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_literal(self)

  def get_value(self) -> message.Message:
    """Returns a defensive copy of the literal value."""
    return copy.deepcopy(self._value)

  def to_path_token(self) -> str:
    return self._fhir_path_str

  def to_fhir_path(self) -> str:
    return self.to_path_token()

  def operands(self) -> List[ExpressionNode]:
    return []

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    # No operands to replace
    pass


class InvokeExpressionNode(ExpressionNode):
  """Handles the FHIRPath InvocationExpression."""

  def _resolve_if_choice_type(
      self, fhir_message: message.Message) -> Optional[message.Message]:
    """Resolve to the proper field if given a choice type, return as-is if not.

    Each value in a FHIR choice type is a different field on the protobuf
    representation wrapped under a proto onoeof field.  Therefore, if
    an expression points to a choice type, we should return the populated
    field -- while just returning the field as-is for non-choice types. This
    way we can simply pass nested messages through this class, and return the
    populated item when appropriate.

    Args:
      fhir_message: the evaluation result which may or may not be a choice type

    Returns:
      The result value, resolved to the sub-field if it is a choice type.
    """
    if annotation_utils.is_choice_type(fhir_message):
      choice_field = fhir_message.WhichOneof('choice')
      if choice_field is None:
        return None
      return cast(message.Message,
                  proto_utils.get_value_at_field(fhir_message, choice_field))
    return fhir_message

  def __init__(self, fhir_context: context.FhirPathContext, identifier: str,
               parent_node: ExpressionNode) -> None:
    self._identifier = identifier
    self._parent_node = parent_node
    return_type = None
    if self._identifier == '$this':
      return_type = self._parent_node.return_type()
    else:
      return_type = fhir_context.get_child_data_type(
          self._parent_node.return_type(), self._identifier)

    if not return_type:
      raise ValueError(
          f'Identifier {identifier} cannot be extracted from parent node {self._parent_node}'
      )
    super().__init__(fhir_context, return_type)

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self._parent_node.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self._parent_node.get_root_node()

  def get_parent_node(self) -> ExpressionNode:
    return self._parent_node

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._parent_node.evaluate(work_space)
    results = []
    for operand_message in operand_messages:
      operand_results = get_messages(operand_message.message, self._identifier)
      for operand_result in operand_results:
        resolved_result = self._resolve_if_choice_type(operand_result)
        if resolved_result is not None:
          results.append(
              WorkSpaceMessage(message=resolved_result, parent=operand_message))

    return results

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_invoke_expression(self)

  @property
  def identifier(self) -> str:
    return self._identifier

  @property
  def operand_node(self) -> ExpressionNode:
    return self._parent_node

  def to_path_token(self) -> str:
    if self.identifier == '$this':
      return self._parent_node.to_path_token()
    return self.identifier

  def to_fhir_path(self) -> str:
    # Exclude the root message name from the FHIRPath, following conventions.
    if self._identifier == '$this':
      return self._parent_node.to_fhir_path()
    elif isinstance(self._parent_node, StructureBaseNode) or isinstance(
        self._parent_node, ReferenceNode):
      return self._identifier
    else:
      return self._parent_node.to_fhir_path() + '.' + self._identifier

  def operands(self) -> List[ExpressionNode]:
    return [self._parent_node]

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    if self._parent_node.to_fhir_path() == expression_to_replace:
      self._parent_node = replacement


class IndexerNode(ExpressionNode):
  """Handles the indexing operation."""

  def __init__(self, fhir_context: context.FhirPathContext,
               collection: ExpressionNode, index: LiteralNode) -> None:
    if not isinstance(index.return_type(), _fhir_path_data_types._Integer):
      raise ValueError(f'Expected index type to be Integer. '
                       f'Got {index.return_type()} instead.')
    self._collection = collection
    self._index = index
    super().__init__(fhir_context, collection.return_type())

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return (self.collection.get_resource_nodes() +
            self.index.get_resource_nodes())

  def get_root_node(self) -> ExpressionNode:
    return self.collection.get_root_node()

  def get_parent_node(self) -> ExpressionNode:
    return self.collection

  @property
  def collection(self) -> ExpressionNode:
    return self._collection  # pytype: disable=attribute-error

  @property
  def index(self) -> LiteralNode:
    return self._index  # pytype: disable=attribute-error

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    collection_messages = self._collection.evaluate(work_space)  # pytype: disable=attribute-error
    index_messages = self._index.evaluate(work_space)  # pytype: disable=attribute-error
    index = _to_int(index_messages)
    if index is None:
      raise ValueError('Expected a non-empty index')

    # According to the spec, if the array is empty or the index is out of bounds
    # an empty array is returned.
    # https://hl7.org/fhirpath/#index-integer-collection
    if not collection_messages or index >= len(collection_messages):
      return []

    return [collection_messages[index]]

  def to_fhir_path(self) -> str:
    return f'{self._collection.to_fhir_path()}[{self._index.to_fhir_path()}]'  # pytype: disable=attribute-error

  def operands(self) -> List[ExpressionNode]:
    return [self._collection]  # pytype: disable=attribute-error

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    if self._collection.to_fhir_path() == expression_to_replace:  # pytype: disable=attribute-error
      self._collection = replacement

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_indexer(self)


class NumericPolarityNode(ExpressionNode):
  """Numeric polarity support."""

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, polarity: _ast.Polarity) -> None:
    if operand.return_type() and not _fhir_path_data_types.is_numeric(
        operand.return_type()):
      raise ValueError(
          f'Operand must be of numeric type. {operand.return_type()}')
    self._operand = operand
    self._polarity = polarity
    super().__init__(fhir_context, operand.return_type())

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self._operand.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self._operand.get_root_node()

  def get_parent_node(self) -> ExpressionNode:
    return self._operand

  @property
  def operand(self) -> ExpressionNode:
    return self._operand

  @property
  def op(self) -> str:
    return self._polarity.op

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)
    if not operand_messages:
      return []

    if len(operand_messages) != 1:
      raise ValueError('FHIRPath polarity must have a single value.')

    operand_message = operand_messages[0]
    if not _is_numeric(operand_message.message):
      raise ValueError('Polarity operators allowed only on numeric types.')

    value = decimal.Decimal(
        proto_utils.get_value_at_field(operand_message.message, 'value'))
    if self._polarity.op == _ast.Polarity.Op.NEGATIVE:
      result = value.copy_negate()
    else:
      result = value

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_decimal(str(result)),
            parent=None)
    ]

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_polarity(self)

  def to_fhir_path(self) -> str:
    return f'{str(self._polarity.op)} {self._operand.to_fhir_path()}'

  def operands(self) -> List[ExpressionNode]:
    return [self._operand]

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    if self._operand.to_fhir_path() == expression_to_replace:
      self._operand = replacement


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
      return_type: Optional[_fhir_path_data_types.FhirPathDataType] = None
  ) -> None:
    super().__init__(fhir_context, return_type)
    self._operand = operand
    self._parent_node = operand
    self._params = params

  def get_resource_nodes(self) -> List[ExpressionNode]:
    result = self._parent_node.get_resource_nodes()
    for p in self._params:
      result += p.get_resource_nodes()
    return result

  def get_root_node(self) -> ExpressionNode:
    return self._parent_node.get_root_node()

  def get_parent_node(self) -> ExpressionNode:
    return self._parent_node

  def to_fhir_path(self) -> str:
    param_str = ', '.join([param.to_fhir_path() for param in self._params])
    # Exclude the root message name from the FHIRPath, following conventions.
    if isinstance(self._operand, StructureBaseNode):
      return f'{self.NAME}({param_str})'
    else:
      return f'{self._operand.to_fhir_path()}.{self.NAME}({param_str})'

  def parent_node(self) -> ExpressionNode:
    return self._operand

  def params(self) -> List[ExpressionNode]:
    return self._params

  def operands(self) -> List[ExpressionNode]:
    return [self._operand] + self._params

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
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

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)

    # Exists is true if there messages is non-null and contains at least one
    # item, which maps to Python array truthiness.
    exists = bool(operand_messages)
    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(exists),
            parent=None)
    ]


class CountFunction(FunctionNode):
  """Implementation of the count() function."""

  NAME = 'count'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Integer)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)

    # Counts number of items in operand.
    count = len(operand_messages)
    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_integer(count),
            parent=None)
    ]


class EmptyFunction(FunctionNode):
  """Implementation of the empty() function."""

  NAME = 'empty'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)

    # Determines if operand is empty.
    empty = not operand_messages
    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(empty),
            parent=None)
    ]


class FirstFunction(FunctionNode):
  """Implementation of the first() function."""

  NAME = 'first'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    super().__init__(
        fhir_context, operand, params,
        operand.return_type().get_new_cardinality_type(
            _fhir_path_data_types.Cardinality.SCALAR))

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)
    if operand_messages:
      return [operand_messages[0]]
    else:
      return []


class AnyTrueFunction(FunctionNode):
  """Implementation of the anyTrue() function."""

  NAME = 'anyTrue'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    if (not operand.return_type().returns_collection() or
        not isinstance(operand.return_type(), _fhir_path_data_types._Boolean)):
      raise ValueError('anyTrue() must be called on a Collection of booleans. '
                       f'Got type of {operand.return_type()}.')

    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    child_results = self._operand.evaluate(work_space)  # pytype: disable=attribute-error
    for candidate in child_results:
      work_space.push_message(candidate)
      try:
        if _to_boolean([candidate]):
          return [
              WorkSpaceMessage(
                  message=work_space.primitive_handler.new_boolean(True),
                  parent=None)
          ]
      finally:
        work_space.pop_message()

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(False),
            parent=None)
    ]


class HasValueFunction(FunctionNode):
  """Implementation of the hasValue() function."""

  NAME = 'hasValue'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)

    is_primitive = (
        len(operand_messages) == 1 and
        annotation_utils.is_primitive_type(operand_messages[0].message))
    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(is_primitive),
            parent=None)
    ]


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

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    # TODO(b/244184211): Resolve typing for idFor function.
    if not (len(params) == 1 and isinstance(params[0], LiteralNode) and
            fhir_types.is_string(cast(LiteralNode, params[0]).get_value())):
      raise ValueError(
          'IdFor function requires a single parameter of the resource type.')
    # Determine the expected FHIR type to use as the node's return type.
    type_param_str = cast(Any, params[0]).get_value().value

    # Trim the FHIR prefix used for primitive types, if applicable.
    self.base_type_str = type_param_str[5:] if type_param_str.startswith(
        'FHIR.') else type_param_str
    self.struct_def_url = ('http://hl7.org/fhir/StructureDefinition/'
                           f'{self.base_type_str.capitalize()}')
    return_type = fhir_context.get_fhir_type_from_string(
        self.struct_def_url, element_definition=None)
    super().__init__(fhir_context, operand, params, return_type)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    # TODO(b/220344555): Support this for future non-SQL view users.
    raise NotImplementedError('Currently only supported for SQL-based views.')


class OfTypeFunction(FunctionNode):
  """ofType() implementation that returns only members of the given type."""

  NAME = 'ofType'
  struct_def_url: str
  base_type_str: str

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    if not (len(params) == 1 and isinstance(params[0], LiteralNode) and
            fhir_types.is_string(cast(LiteralNode, params[0]).get_value())):
      raise ValueError(
          'ofType function requires a single parameter of the datatype.')

    # Determine the expected FHIR type to use as the node's return type.
    type_param_str = cast(Any, params[0]).get_value().value

    # Trim the FHIR prefix used for primitive types, if applicable.
    self.base_type_str = type_param_str[5:] if type_param_str.startswith(
        'FHIR.') else type_param_str
    self.struct_def_url = (
        f'http://hl7.org/fhir/StructureDefinition/{self.base_type_str}')

    return_type = _fhir_path_data_types.Empty
    if isinstance(operand.return_type(),
                  _fhir_path_data_types.PolymorphicDataType):
      if self.base_type_str.casefold() in cast(
          _fhir_path_data_types.PolymorphicDataType,
          operand.return_type()).fields():
        return_type = operand.return_type().types()[
            self.base_type_str.casefold()]
    else:
      return_type = fhir_context.get_child_data_type(operand.return_type(),
                                                     self.base_type_str)

    if _fhir_path_data_types.returns_collection(operand.return_type()):
      return_type = return_type.get_new_cardinality_type(
          _fhir_path_data_types.Cardinality.CHILD_OF_COLLECTION)

    super().__init__(fhir_context, operand, params, return_type)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)  # pytype: disable=attribute-error
    results = []

    for operand_message in operand_messages:
      url = annotation_utils.get_structure_definition_url(
          operand_message.message)
      if url is not None and url.casefold() == self.struct_def_url.casefold():
        results.append(operand_message)

    return results


class MemberOfFunction(FunctionNode):
  """Implementation of the memberOf() function."""

  # Literal valueset URL and values to check for memberOf operations.
  NAME = 'memberOf'
  value_set_url: str
  value_set_version: Optional[str] = None
  code_values: Optional[FrozenSet[CodeValue]] = None
  code_values_lock = threading.Lock()

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    if not (isinstance(operand.return_type(), _fhir_path_data_types._String) or
            _fhir_path_data_types.is_coding(operand.return_type()) or
            _fhir_path_data_types.is_codeable_concept(operand.return_type())):
      raise ValueError(
          'MemberOf must be called on a string, code, coding, or codeable '
          'concept, not %s' % operand.return_type())

    if len(params) != 1 or not isinstance(params[0], LiteralNode):
      raise ValueError(
          'MemberOf requires single valueset URL or proto parameter.')

    value = cast(Any, params[0]).get_value()

    # If the parameter is a ValueSet literal, load it into a set for
    # efficient evaluation.
    if annotation_utils.get_structure_definition_url(value) == VALUE_SET_URL:
      self.value_set_url = value.url.value
      self.value_set_version = value.version.value or None
      self.code_values = to_code_values(value)
    elif (annotation_utils.is_primitive_type(value) and
          isinstance(value.value, str)):
      # The parameter is a URL to a valueset, so preserve it for evaluation
      # engines to resolve.
      self.value_set_url = value.value
      parsed = urllib.parse.urlparse(self.value_set_url)
      if not parsed.scheme and parsed.path:
        raise ValueError(
            f'memberOf must be called with a valid URI, not {self.value_set_url}'
        )

    else:
      raise ValueError(
          'MemberOf requires single valueset URL or proto parameter.')
    return_type = _fhir_path_data_types.Boolean

    if operand.return_type().returns_collection():
      return_type = return_type.get_new_cardinality_type(
          _fhir_path_data_types.Cardinality.CHILD_OF_COLLECTION)
    super().__init__(fhir_context, operand, params, return_type)

  def to_value_set_codes(
      self, fhir_context: context.FhirPathContext) -> Optional[ValueSetCodes]:
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
      return ValueSetCodes(self.value_set_url, self.value_set_version,
                           self.code_values)

    value_set_proto = fhir_context.get_value_set(self.value_set_url)
    if value_set_proto is None:
      return None

    return ValueSetCodes(self.value_set_url, value_set_proto.version.value or
                         None, to_code_values(value_set_proto))

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    result = False
    operand_messages = self._operand.evaluate(work_space)  # pytype: disable=attribute-error

    # If the code_values are not present, attempt to get them from FHIR context.
    with self.code_values_lock:
      if self.code_values is None:
        value_set_url = cast(
            Any, self._params[0].evaluate(work_space)[0].message).value  # pytype: disable=attribute-error
        value_set_proto = work_space.fhir_context.get_value_set(value_set_url)

        if value_set_proto is None:
          raise ValueError(f'No value set {value_set_url} found.')
        self.code_values = to_code_values(value_set_proto)

    for workspace_message in operand_messages:
      fhir_message = workspace_message.message
      if fhir_types.is_codeable_concept(fhir_message):
        for coding in cast(Any, fhir_message).coding:
          if CodeValue(coding.system.value,
                       coding.code.value) in self.code_values:
            result = True
            break

      elif fhir_types.is_coding(fhir_message):
        if CodeValue(coding.system.value,
                     coding.code.value) in self.code_values:
          result = True
          break

      # TODO(b/208900793): Add raw code support
      else:
        raise ValueError(
            f'MemberOf not supported on {fhir_message.DESCRIPTOR.full_name}')

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(result),
            parent=None)
    ]


class NotFunction(FunctionNode):
  """Implementation of the not_() function."""

  NAME = 'not'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Boolean)
    if params:
      raise ValueError(('not() function should not have any parameters but has '
                        f'{str(len(params))} parameters.'))

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)
    if not operand_messages:
      return []

    result = True
    for operand_message in operand_messages:
      if not fhir_types.is_boolean(operand_message.message):
        raise ValueError('Boolean operators allowed only on boolean types.')
      result &= proto_utils.get_value_at_field(operand_message.message, 'value')

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(not result),
            parent=None)
    ]


class WhereFunction(FunctionNode):
  """Implementation of the where() function."""

  NAME = 'where'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    _check_is_predicate(self.NAME, params)
    super().__init__(fhir_context, operand, params, operand.return_type())

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    results = []
    child_results = self._operand.evaluate(work_space)
    for candidate in child_results:
      # Iterate through the candidates and evaluate them against the where
      # predicate in a local workspace, keeping those that match.
      work_space.push_message(candidate)
      try:
        predicate_result = self._params[0].evaluate(work_space)
        if _to_boolean(predicate_result):
          results.append(candidate)
      finally:
        work_space.pop_message()
    return results


class AllFunction(FunctionNode):
  """Implementation of the all() function."""

  NAME = 'all'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    _check_is_predicate(self.NAME, params)
    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    all_match = True
    child_results = self._operand.evaluate(work_space)
    for candidate in child_results:
      # Iterate through the candidates and evaluate them against the
      # predicate in a local workspace, and short circuit if one doesn't.
      work_space.push_message(candidate)
      try:
        predicate_result = self._params[0].evaluate(work_space)
        if not _to_boolean(predicate_result):
          all_match = False
          break
      finally:
        work_space.pop_message()

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(all_match),
            parent=None)
    ]


class MatchesFunction(FunctionNode):
  """Implementation of the matches() function."""

  pattern = None
  NAME = 'matches'

  def __init__(self, fhir_context: context.FhirPathContext,
               operand: ExpressionNode, params: List[ExpressionNode]) -> None:
    if not params:
      regex = None
    elif not (isinstance(params[0], LiteralNode) and
              fhir_types.is_string(cast(LiteralNode, params[0]).get_value())):
      raise ValueError('matches() requires a single string parameter.')
    else:
      regex = cast(Any, params[0]).get_value().value
    self.pattern = re.compile(regex) if regex else None
    super().__init__(fhir_context, operand, params,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)
    result = True

    if not self.pattern or not operand_messages:
      return []

    if len(operand_messages) > 1 or not fhir_types.is_string(
        operand_messages[0].message):
      raise ValueError(
          'Input collection contains more than one item or is not of string '
          'type.')

    operand_str = cast(str, operand_messages[0].message).value  # pytype: disable=attribute-error
    if not self.pattern.match(operand_str):
      result = False

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(result),
            parent=None)
    ]


class EqualityNode(CoercibleBinaryExpressionNode):
  """Implementation of FHIRPath equality and equivalence operators."""

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               operator: _ast.EqualityRelation.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    self._operator = operator
    super().__init__(fhir_context, handler, left, right,
                     _fhir_path_data_types.Boolean)

  @property
  def op(self) -> _ast.EqualityRelation.Op:
    return self._operator

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    # TODO(b/234657818): Add support for FHIRPath equivalence operators.
    if (self._operator != _ast.EqualityRelation.Op.EQUAL and
        self._operator != _ast.EqualityRelation.Op.NOT_EQUAL):
      raise NotImplementedError('Implement all equality relations.')

    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)

    if not left_messages or not right_messages:
      return []

    are_equal = True

    if len(left_messages) != len(right_messages):
      are_equal = False
    else:
      are_equal = all(
          _messages_equal(self._handler, left_message, right_message)
          for left_message, right_message in zip(left_messages, right_messages))

    result = (
        are_equal
        if self._operator == _ast.EqualityRelation.Op.EQUAL else not are_equal)
    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(result),
            parent=None)
    ]

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_equality(self)

  def to_fhir_path(self) -> str:
    return f'{self._left.to_fhir_path()} {self._operator.value} {self._right.to_fhir_path()}'


class BooleanOperatorNode(BinaryExpressionNode):
  """Implementation of FHIRPath boolean operations.

  See https://hl7.org/fhirpath/#boolean-logic for behavior definition.
  """

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               operator: _ast.BooleanLogic.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    self._operator = operator
    super().__init__(fhir_context, handler, left, right,
                     _fhir_path_data_types.Boolean)

  @property
  def op(self) -> _ast.BooleanLogic.Op:
    return self._operator

  def _evaluate_expression(self, left: Optional[bool],
                           right: Optional[bool]) -> Optional[bool]:
    """Applies the FHIRPath boolean evaluataion semantics."""
    # Explicit comparison needed for FHIRPath empty/none semantics.
    # pylint: disable=g-bool-id-comparison
    if self._operator == _ast.BooleanLogic.Op.AND:
      # 'None and False' returns False in FHIRPath, unlike Python.
      if ((left is None and right is False) or
          (right is None and left is False)):
        return False
      else:
        return left and right
    elif self._operator == _ast.BooleanLogic.Op.OR:
      return left or right
    elif self._operator == _ast.BooleanLogic.Op.IMPLIES:
      # Handle implies semantics for None as defined by FHIRPath.
      if left is None:
        return True if right else None
      else:
        return (not left) or right
    else:  # self._operator == _ast.BooleanLogic.Op.XOR:
      if left is None or right is None:
        return None
      else:
        return (left and not right) or (right and not left)
    # pylint: enable=g-bool-id-comparison

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)
    left = _to_boolean(left_messages)
    right = _to_boolean(right_messages)

    result = self._evaluate_expression(left, right)

    if result is None:
      return []
    else:
      return [
          WorkSpaceMessage(
              message=work_space.primitive_handler.new_boolean(result),
              parent=None)
      ]

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_boolean_op(self)

  def to_fhir_path(self) -> str:
    return (f'{self._left.to_fhir_path()} {self._operator.value} '
            f'{self._right.to_fhir_path()}')


class ArithmeticNode(CoercibleBinaryExpressionNode):
  """Implementation of FHIRPath arithmetic operations.

  See https://hl7.org/fhirpath/#math-2 for behavior definition.
  """

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               operator: _ast.Arithmetic.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    if not _fhir_path_data_types.is_coercible(left.return_type(),
                                              right.return_type()):
      raise ValueError(f'Arithmetic nodes must be coercible.'
                       f'{left.return_type()} {right.return_type()}')

    self._operator = operator
    return_type = left.return_type() if left else right.return_type()
    super().__init__(fhir_context, handler, left, right, return_type)

  @property
  def op(self) -> _ast.Arithmetic.Op:
    return self._operator

  def _stringify(self, string_messages: List[WorkSpaceMessage]) -> str:
    """Returns empty string for None messages."""
    if not string_messages:
      return ''

    if len(string_messages) != 1:
      raise ValueError(
          'FHIRPath arithmetic must have single elements on each side.')
    string_message = string_messages[0].message

    if string_message is None:
      return ''

    if not fhir_types.is_string(string_message):
      raise ValueError(
          'String concatenation only accepts str or None operands.')
    return cast(str, string_message).value  # pytype: disable=attribute-error

  def _evaluate_numeric_expression(
      self, left: Optional[decimal.Decimal],
      right: Optional[decimal.Decimal]) -> Optional[decimal.Decimal]:
    """Applies the FHIRPath arithmetic evaluataion semantics."""
    # Explicit comparison needed for FHIRPath empty/none semantics.
    # pylint: disable=g-bool-id-comparison
    if left is None or right is None:
      return None

    if self._operator == _ast.Arithmetic.Op.MULTIPLICATION:
      return left * right
    elif self._operator == _ast.Arithmetic.Op.ADDITION:
      return left + right
    elif self._operator == _ast.Arithmetic.Op.SUBTRACTION:
      return left - right

    # Division operators need to check if denominator is 0.
    if right == 0.0:
      return None

    if self._operator == _ast.Arithmetic.Op.DIVISION:
      return left / right
    elif self._operator == _ast.Arithmetic.Op.MODULO:
      return left % right
    else:  # self._operator == _ast.Arithmetic.Op.TRUNCATED_DIVISON:
      return left // right
    # pylint: enable=g-bool-id-comparison

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)
    result = None

    # String concatenation is the only operator that doesn't return an empty
    # array if one of the operands is None so we need to special case it.
    if self._operator == _ast.Arithmetic.Op.STRING_CONCATENATION:
      result = self._stringify(left_messages) + self._stringify(right_messages)
    else:
      # Propagate empty/null values if they exist in operands.
      if not left_messages or not right_messages:
        return []

      if len(left_messages) != 1 or len(right_messages) != 1:
        raise ValueError(
            'FHIRPath arithmetic must have single elements on each side.')

      left = left_messages[0].message
      right = right_messages[0].message

      # TODO(b/226131330): Add support for arithmetic with units.
      left_value = cast(Any, left).value
      right_value = cast(Any, right).value

      if (fhir_types.is_string(left) and fhir_types.is_string(right) and
          self._operator == _ast.Arithmetic.Op.ADDITION):
        result = left_value + right_value
      elif _is_numeric(left) and _is_numeric(right):
        left_value = decimal.Decimal(left_value)
        right_value = decimal.Decimal(right_value)
        result = self._evaluate_numeric_expression(left_value, right_value)
      else:
        raise ValueError(
            (f'Cannot {self._operator.value} {left.DESCRIPTOR.full_name} with '
             f'{right.DESCRIPTOR.full_name}.'))

    if result is None:
      return []
    elif isinstance(result, str):
      return [
          WorkSpaceMessage(
              message=work_space.primitive_handler.new_string(result),
              parent=None)
      ]
    else:
      return [
          WorkSpaceMessage(
              message=work_space.primitive_handler.new_decimal(str(result)),
              parent=None)
      ]

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_arithmetic(self)

  def to_fhir_path(self) -> str:
    return (f'{self._left.to_fhir_path()} {self._operator.value} '
            f'{self._right.to_fhir_path()}')


class ComparisonNode(CoercibleBinaryExpressionNode):
  """Implementation of the FHIRPath comparison functions."""

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               operator: _ast.Comparison.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    self._operator = operator
    super().__init__(fhir_context, handler, left, right,
                     _fhir_path_data_types.Boolean)

  @property
  def op(self) -> _ast.Comparison.Op:
    return self._operator

  def _compare(self, left: Any, right: Any) -> bool:
    if self._operator == _ast.Comparison.Op.LESS_THAN:
      return left < right
    elif self._operator == _ast.Comparison.Op.GREATER_THAN:
      return left > right
    elif self._operator == _ast.Comparison.Op.LESS_THAN_OR_EQUAL:
      return left <= right
    else:  # self._operator == _ast.Comparison.Op.GREATER_THAN_OR_EQUAL:
      return left >= right

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)
    result = None

    # Propagate empty/null values if they exist in operands.
    if not left_messages or not right_messages:
      return []

    if len(left_messages) != 1 or len(right_messages) != 1:
      raise ValueError(
          'FHIRPath comparisons must have single elements on each side.')

    left = left_messages[0].message
    right = right_messages[0].message

    if (annotation_utils.get_structure_definition_url(left) == QUANTITY_URL and
        annotation_utils.get_structure_definition_url(right) == QUANTITY_URL):
      result = self._compare(
          quantity.quantity_from_proto(left),
          quantity.quantity_from_proto(right))
    elif hasattr(left, 'value') and hasattr(right, 'value'):
      left_value = cast(Any, left).value
      right_value = cast(Any, right).value
      # Wrap decimal types to ensure numeric rather than alpha comparison
      if fhir_types.is_decimal(left) and fhir_types.is_decimal(right):
        left_value = decimal.Decimal(left_value)
        right_value = decimal.Decimal(right_value)
      result = self._compare(left_value, right_value)
    elif ((fhir_types.is_date(left) or fhir_types.is_date_time(left)) and
          (fhir_types.is_date(right) or fhir_types.is_date_time(right))):
      # Both left and right are date-related types, so we can compare
      # timestamps.
      left_value = cast(Any, left).value_us
      right_value = cast(Any, right).value_us
      result = self._compare(left_value, right_value)
    else:
      raise ValueError((f'{left.DESCRIPTOR.full_name} not comaprable with '
                        f'{right.DESCRIPTOR.full_name}.'))

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(result),
            parent=None)
    ]

  def accept(self, visitor: 'ExpressionNodeBaseVisitor') -> Any:
    return visitor.visit_comparison(self)

  def to_fhir_path(self) -> str:
    return (f'{self._left.to_fhir_path()} {self._operator.value} '
            f'{self._right.to_fhir_path()}')


class ReferenceNode(ExpressionNode):
  """Implementation of $this keyword and relative paths."""

  def __init__(self, fhir_context: context.FhirPathContext,
               reference_node: ExpressionNode) -> None:
    self._reference_node = reference_node
    # If the reference node/caller is a function, then the actual node being
    # referenced is the first non-function caller.
    while isinstance(self._reference_node, FunctionNode):
      self._reference_node = self._reference_node.get_parent_node()
    super().__init__(fhir_context, reference_node.return_type())

  def get_resource_nodes(self) -> List[ExpressionNode]:
    return self._reference_node.get_resource_nodes()

  def get_root_node(self) -> ExpressionNode:
    return self._reference_node.get_root_node()

  def get_parent_node(self) -> ExpressionNode:
    return self._reference_node

  def operands(self) -> List[ExpressionNode]:
    return [self._reference_node]

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    if self._reference_node.to_fhir_path() == expression_to_replace:
      self._reference_node = replacement

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    return [work_space.current_message()]

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

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               left: ExpressionNode, right: ExpressionNode) -> None:
    super().__init__(fhir_context, handler, left, right,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)
    return _is_element_in_collection(self._handler, work_space, left_messages,
                                     right_messages)

  def to_fhir_path(self) -> str:
    return f'{self._left.to_fhir_path()} in {self._right.to_fhir_path()}'


class ContainsNode(MembershipRelationNode):
  """Implementation of the FHIRPath contains operator.

  This is the converse operation of in.
  The spec for the contains operator is taken from:
  https://fhirpath.readthedocs.io/en/latest/fhirpath.html#fhirpath.fhirpath.FHIRPath.contained
  """

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               left: ExpressionNode, right: ExpressionNode) -> None:
    super().__init__(fhir_context, handler, left, right,
                     _fhir_path_data_types.Boolean)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)
    return _is_element_in_collection(self._handler, work_space, right_messages,
                                     left_messages)

  def to_fhir_path(self) -> str:
    return f'{self._left.to_fhir_path()} contains {self._right.to_fhir_path()}'


class UnionNode(BinaryExpressionNode):
  """Implementation of the FHIRPath union operator.

  The spec for the union operator is taken from:
  https://build.fhir.org/ig/HL7/FHIRPath/#union-collections
  https://build.fhir.org/ig/HL7/FHIRPath/#unionother-collection
  """

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler,
               left: ExpressionNode, right: ExpressionNode) -> None:
    left_type = left.return_type()
    right_type = right.return_type()

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
      return_type = _fhir_path_data_types.Collection(types_union)

    super().__init__(fhir_context, handler, left, right, return_type)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)

    # Build a set of unique messages by using their json_value to
    # determine uniqueness. As in the _messages_equal function, we use
    # the json_value to determine message equality.
    messages_union: Dict[str, WorkSpaceMessage] = {}
    for work_space_message in itertools.chain(left_messages, right_messages):
      json_value = self._handler.primitive_wrapper_from_primitive(
          work_space_message.message).json_value()
      messages_union.setdefault(json_value, work_space_message)

    return list(messages_union.values())

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
}


class ExpressionNodeBaseVisitor(abc.ABC):
  """Abstract base class that visits the Expression Nodes."""

  def visit(self, node: ExpressionNode) -> Any:
    return node.accept(self)

  def visit_children(self, node: ExpressionNode) -> Any:
    result: List[Any] = []
    for c in node.children():  # pytype: disable=attribute-error
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

  def __init__(self,
               handler: primitive_handler.PrimitiveHandler,
               fhir_context: context.FhirPathContext,
               data_type: Optional[
                   _fhir_path_data_types.FhirPathDataType] = None,
               root_node_context: Optional[ExpressionNode] = None) -> None:
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
      return LiteralNode(self._context,
                         self._handler.new_boolean(literal.value),
                         str(literal.value), _fhir_path_data_types.Boolean)
    elif isinstance(literal.value, int):
      return LiteralNode(self._context,
                         self._handler.new_integer(literal.value),
                         str(literal.value), _fhir_path_data_types.Integer)
    elif isinstance(literal.value, float):
      return LiteralNode(self._context,
                         self._handler.new_decimal(literal.value),
                         str(literal.value), _fhir_path_data_types.Decimal)
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
                literal_str, primitive_cls).wrapped, f'@{literal_str}',
            fhir_type)
      else:
        return LiteralNode(self._context,
                           self._handler.new_string(literal.value),
                           f"'{literal.value}'", _fhir_path_data_types.String)
    elif isinstance(literal.value, decimal.Decimal):
      return LiteralNode(self._context,
                         self._handler.new_decimal(str(literal.value)),
                         str(literal.value), _fhir_path_data_types.Decimal)
    elif isinstance(literal.value, _ast.Quantity):
      return LiteralNode(
          self._context,
          self._handler.new_quantity(literal.value.value, literal.value.unit),
          str(literal.value), _fhir_path_data_types.Quantity)
    else:
      raise ValueError(
          f'Unsupported literal value: {literal} {type(literal.value)}.')

  def visit_identifier(self, identifier: _ast.Identifier) -> Any:
    return InvokeExpressionNode(self._context, identifier.value,
                                self._node_context[-1])

  def visit_indexer(self, indexer: _ast.Indexer, **kwargs: Any) -> Any:
    collection_result = self.visit(indexer.collection)
    index_result = self.visit(indexer.index)
    return IndexerNode(self._context, collection_result, index_result)

  def visit_arithmetic(self, arithmetic: _ast.Arithmetic, **kwargs: Any) -> Any:
    left = self.visit(arithmetic.lhs)
    right = self.visit(arithmetic.rhs)

    return ArithmeticNode(self._context, self._handler, arithmetic.op, left,
                          right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_type_expression(self, type_expression: _ast.TypeExpression,
                            **kwargs: Any) -> Any:
    raise NotImplementedError('TODO: implement `visit_type_expression`.')

  def visit_equality(self, equality: _ast.EqualityRelation,
                     **kwargs: Any) -> Any:

    left = self.visit(equality.lhs)
    right = self.visit(equality.rhs)

    return EqualityNode(self._context, self._handler, equality.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_comparison(self, comparison: _ast.Comparison, **kwargs: Any) -> Any:
    left = self.visit(comparison.lhs)
    right = self.visit(comparison.rhs)

    return ComparisonNode(self._context, self._handler, comparison.op, left,
                          right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_boolean_logic(self, boolean_logic: _ast.BooleanLogic,
                          **kwargs: Any) -> Any:
    left = self.visit(boolean_logic.lhs)
    right = self.visit(boolean_logic.rhs)

    return BooleanOperatorNode(self._context, self._handler, boolean_logic.op,
                               left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_membership(self, membership: _ast.MembershipRelation,
                       **kwargs: Any) -> Any:
    left = self.visit(membership.lhs)
    right = self.visit(membership.rhs)

    if membership.op == membership.Op.CONTAINS:
      return ContainsNode(self._context, self._handler, left, right)
    elif membership.op == membership.Op.IN:
      return InNode(self._context, self._handler, left, right)
    else:
      raise ValueError(f'Unknown membership operator "{membership.op}".')

  def visit_union(self, union: _ast.UnionOp, **kwargs: Any) -> Any:
    return UnionNode(self._context, self._handler, self.visit(union.lhs),
                     self.visit(union.rhs))

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
      return LiteralNode(self._context, modified_value,
                         f'{polarity.op}{operand_node.to_fhir_path()}',
                         _fhir_path_data_types.Decimal)
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

  def visit_function(self,
                     function: _ast.Function,
                     operand: Optional[_ast.Expression] = None) -> FunctionNode:
    function_name = function.identifier.value
    function_class = _FUNCTION_NODE_MAP.get(function_name)
    if function_class is None:
      raise NotImplementedError(f'Function {function_name} not implemented.')

    # Use the given operand if it exists, otherwise this must have been invoked
    # on the root, so that is the effective operand.
    operand_node = (
        self.visit(operand) if operand is not None else self._node_context[-1])
    params: List[ExpressionNode] = []
    # For functions, the identifiers can be relative to the operand of the
    # function; not the root FHIR type.
    self._node_context.append(ReferenceNode(self._context, operand_node))
    for param in function.params:
      new_param = self.visit(param)
      params.append(new_param)
    self._node_context.pop()
    return function_class(self._context, operand_node, params)


def _messages_equal(handler: primitive_handler.PrimitiveHandler,
                    left: WorkSpaceMessage, right: WorkSpaceMessage) -> bool:
  """Returns true if left and right are equal."""
  # If left and right are the same types, simply compare the protos.
  if (left.message.DESCRIPTOR is right.message.DESCRIPTOR or
      left.message.DESCRIPTOR.full_name == right.message.DESCRIPTOR.full_name):
    if (annotation_utils.get_structure_definition_url(left.message)
        == QUANTITY_URL and annotation_utils.get_structure_definition_url(
            right.message) == QUANTITY_URL):
      return quantity.quantity_from_proto(
          left.message) == quantity.quantity_from_proto(right.message)
    return left.message == right.message

  # Left and right are different types, but may still be logically equal if
  # they are primitives and we are comparing a literal value to a FHIR proto
  # with an enum field. We can compare their JSON values to check that.
  if (annotation_utils.is_primitive_type(left.message) and
      annotation_utils.is_primitive_type(right.message)):
    left_wrapper = handler.primitive_wrapper_from_primitive(left.message)
    right_wrapper = handler.primitive_wrapper_from_primitive(right.message)
    return left_wrapper.json_value() == right_wrapper.json_value()

  return False


def _is_element_in_collection(
    handler: primitive_handler.PrimitiveHandler, work_space: WorkSpace,
    element: List[WorkSpaceMessage],
    collection: List[WorkSpaceMessage]) -> List[WorkSpaceMessage]:
  """Indicates if `element` is a member of `collection`."""
  # If the element is empty, the result is empty.
  if not element:
    return []

  # If the element has multiple items, an error is returned.
  if len(element) != 1:
    raise ValueError(
        'Right hand side of "contains" operator must be a single element.')

  # If the element operand is a collection with a single item, the
  # operator returns true if the item is in the collection using
  # equality semantics.
  # If the collection is empty, the result is false.
  result = any(
      _messages_equal(handler, element[0], item) for item in collection)

  return [
      WorkSpaceMessage(
          message=work_space.primitive_handler.new_boolean(result), parent=None)
  ]

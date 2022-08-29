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
import decimal
import re
import threading
from typing import Any, Dict, FrozenSet, List, Optional, Set, cast

from google.protobuf import descriptor
from google.protobuf import message
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import context
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import annotation_utils
from google.fhir.core.utils import fhir_types
from google.fhir.core.utils import proto_utils

VALUE_SET_URL = 'http://hl7.org/fhir/StructureDefinition/ValueSet'


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

  # Wrap non-repeated items in an array per FHIRPath specificaiton.
  if target_field.label != descriptor.FieldDescriptor.LABEL_REPEATED:
    return [results]
  else:
    return results


def _get_child_data_type(
    parent: Optional[_fhir_path_data_types.FhirPathDataType],
    fhir_context: context.FhirPathContext,
    json_name: str) -> Optional[_fhir_path_data_types.FhirPathDataType]:
  """Returns the data types of the given child field from the parent."""
  if parent is None:
    return None

  if isinstance(parent, _fhir_path_data_types.StructureDataType):
    structdef = fhir_context.get_structure_definition(parent.url)
    elem_path = parent.backbone_element_path + '.' + json_name if parent.backbone_element_path else json_name
    elem = _utils.get_element(structdef, elem_path)
    if elem is None:
      return None
    if _utils.is_backbone_element(elem):
      return _fhir_path_data_types.StructureDataType(structdef.url.value,
                                                     structdef.type.value,
                                                     elem_path)
    else:
      if not elem.type or not elem.type[0].code.value:
        raise ValueError(f'Malformed ElementDefinition in struct {parent.url}')
      type_code = elem.type[0].code.value

      # If this is a primitive, simply return the corresponding primitive type.
      primitive_type = _fhir_path_data_types.primitive_type_from_type_code(
          type_code)
      if primitive_type is not None:
        return primitive_type

      # Load the structure definition for the non-primitive type.
      child_structdef = fhir_context.get_structure_definition(
          elem.type[0].code.value)
      return _fhir_path_data_types.StructureDataType(child_structdef.url.value,
                                                     child_structdef.type.value)
  else:
    return None


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
  """Helper function to convert a ValueSet proto into a set of code value data types."""
  # TODO: Use a protocol for ValueSets to allow type checking.
  # pytype: disable=attribute-error
  expansion = value_set_proto.expansion.contains
  codes = [
      CodeValue(code_elem.system.value, code_elem.code.value)
      for code_elem in expansion
  ]
  return frozenset(codes)


@dataclasses.dataclass
class WorkSpaceMessage:
  """Message with parent context, as needed for some FHIRPath expressions."""
  parent: Optional['WorkSpaceMessage']
  message: message.Message


@dataclasses.dataclass
class WorkSpace:
  """Working memory and context for evaluating FHIRPath expressions."""
  primitive_handler: primitive_handler.PrimitiveHandler
  fhir_context: context.FhirPathContext
  message_context_stack: List[WorkSpaceMessage]

  def current_message(self) -> WorkSpaceMessage:
    return self.message_context_stack[-1]

  def push_message(self, workspace_message: WorkSpaceMessage) -> None:
    self.message_context_stack.append(workspace_message)

  def pop_message(self) -> None:
    self.message_context_stack.pop()


class ExpressionNode(abc.ABC):
  """Abstract base class for all FHIRPath expression evaluation."""

  def __init__(
      self,
      return_type: Optional[_fhir_path_data_types.FhirPathDataType] = None
  ) -> None:
    self._return_type = return_type

  @abc.abstractmethod
  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    """Evaluates the node and returns the resulting messages."""
    raise NotImplementedError('Subclasses *must* implement `evaluate`.')

  @abc.abstractmethod
  def to_fhir_path(self) -> str:
    """Returns the FHIRPath string for this and its children node."""
    raise NotImplementedError('Subclasses *must* implement `to_fhir_path`.')

  def return_type(self) -> Optional[_fhir_path_data_types.FhirPathDataType]:
    """The descriptor of the items returned by the expression, if known."""
    return self._return_type

  def fields(self) -> Optional[Set[str]]:
    """Returns known fields from this expression, or none if they are unknown.

    These are names pulled directly from the FHIR spec, and used in the FHIRPath
    and the JSON representation of the structure.
    """
    # Base implementation has no known fields.
    return None

  @abc.abstractmethod
  def operands(self) -> List['ExpressionNode']:
    """Returns the operands contributing to this node."""
    raise NotImplementedError('Subclasses *must* implement `operands`.')

  @abc.abstractmethod
  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    """Replace any operand that matches the given expresion string."""
    raise NotImplementedError('Subclasses *must* implement `replace_operand`.')

  def __str__(self) -> str:
    return self.to_fhir_path()

  def _operand_to_string(self,
                         operand: 'ExpressionNode',
                         indent: int = 0) -> str:
    """Function to recursively print the operands of the input operand."""
    operand_prints = ''.join('\n' + self._operand_to_string(op, indent + 1)
                             for op in operand.operands())
    return (f'{"| " * indent}+ '
            f'{operand} <{operand.__class__.__name__}> ('
            f'{operand_prints})')

  def debug_string(self) -> str:
    """Returns debug string of the current node."""
    return self._operand_to_string(self)


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


class BinaryExpressionNode(ExpressionNode):
  """Base class for binary expressions."""

  def __init__(self, handler: primitive_handler.PrimitiveHandler,
               left: ExpressionNode, right: ExpressionNode) -> None:
    super().__init__(None)
    self._handler = handler
    self._left = left
    self._right = right

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


class RootMessageNode(ExpressionNode):
  """Returns the root node of the workspace."""

  def __init__(
      self,
      return_type: Optional[_fhir_path_data_types.StructureDataType]) -> None:
    super().__init__(return_type)
    self._struct_type = return_type

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    return [
        WorkSpaceMessage(
            message=work_space.current_message().message,
            parent=work_space.current_message())
    ]

  def to_fhir_path(self) -> str:
    # The FHIRPath of a root structure is simply the base type name,
    # so return that if it exists.
    return self._struct_type.base_type if self._struct_type else ''

  def operands(self) -> List[ExpressionNode]:
    return []

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    # No operands to replace
    pass


class LiteralNode(ExpressionNode):
  """Node expressing a literal FHIRPath value."""

  def __init__(self, value: message.Message, fhir_path_str: str) -> None:
    super().__init__(None)
    self._value = value
    self._fhir_path_str = fhir_path_str

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    return [
        WorkSpaceMessage(
            message=self.get_value(), parent=work_space.current_message())
    ]

  def get_value(self) -> message.Message:
    """Returns a defensive copy of the literal value."""
    return copy.deepcopy(self._value)

  def to_fhir_path(self) -> str:
    return self._fhir_path_str

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
               operand_node: ExpressionNode) -> None:
    super().__init__(
        _get_child_data_type(operand_node.return_type(), fhir_context,
                             identifier))
    self._identifier = identifier
    self._operand_node = operand_node

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand_node.evaluate(work_space)
    results = []
    for operand_message in operand_messages:
      operand_results = get_messages(operand_message.message, self._identifier)
      for operand_result in operand_results:
        resolved_result = self._resolve_if_choice_type(operand_result)
        if resolved_result is not None:
          results.append(
              WorkSpaceMessage(message=resolved_result, parent=operand_message))

    return results

  @property
  def identifier(self) -> str:
    return self._identifier

  @property
  def operand_node(self) -> ExpressionNode:
    return self._operand_node

  def to_fhir_path(self) -> str:
    # Exclude the root message name from the FHIRPath, following conventions.
    if isinstance(self._operand_node, RootMessageNode):
      return self._identifier
    else:
      return self._operand_node.to_fhir_path() + '.' + self._identifier

  def operands(self) -> List[ExpressionNode]:
    return [self._operand_node]

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    if self._operand_node.to_fhir_path() == expression_to_replace:
      self._operand_node = replacement


class IndexerNode(ExpressionNode):
  """Handles the indexing operation."""

  def __init__(self, collection: ExpressionNode, index: LiteralNode) -> None:
    super().__init__(None)
    self._collection = collection
    self._index = index

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    collection_messages = self._collection.evaluate(work_space)
    index_messages = self._index.evaluate(work_space)
    index = _to_int(index_messages)
    if index is None:
      raise ValueError('Expected a non-empty index')

    # According to the spec, if the array is emtpy or the index is out of bounds
    # an emtpy array is returned.
    # https://hl7.org/fhirpath/#index-integer-collection
    if not collection_messages or index >= len(collection_messages):
      return []

    return [collection_messages[index]]

  def to_fhir_path(self) -> str:
    return f'{self._collection.to_fhir_path()}[{self._index.to_fhir_path()}]'

  def operands(self) -> List[ExpressionNode]:
    return [self._operand_node]

  def replace_operand(self, expression_to_replace: str,
                      replacement: 'ExpressionNode') -> None:
    if self._operand_node.to_fhir_path() == expression_to_replace:
      self._operand_node = replacement


class NumericPolarityNode(ExpressionNode):
  """Numeric polarity support."""

  def __init__(self, operand: ExpressionNode, polarity: _ast.Polarity) -> None:
    super().__init__(None)
    self._operand = operand
    self._polarity = polarity

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

  def to_fhir_path(self) -> str:
    return f'{str(self._polarity.op)}{self._operand.to_fhir_path()}'

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

  def __init__(self, name: str, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__(None)
    self._name = name
    self._operand = operand
    self._params = params

  def to_fhir_path(self) -> str:
    param_str = ', '.join([param.to_fhir_path() for param in self._params])
    # Exclude the root message name from the FHIRPath, following conventions.
    if isinstance(self._operand, RootMessageNode):
      return f'{self._name}({param_str})'
    else:
      return f'{self._operand.to_fhir_path()}.{self._name}({param_str})'

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


class ExistsFunction(FunctionNode):
  """Implementation of the exists() function."""

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('exists', operand, params)

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

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('count', operand, params)

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

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('empty', operand, params)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)

    # Determines if operand is empty.
    empty = not operand_messages
    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_integer(empty),
            parent=None)
    ]


class FirstFunction(FunctionNode):
  """Implementation of the first() function."""

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('first', operand, params)

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)
    if operand_messages:
      return [operand_messages[0]]
    else:
      return []


class HasValueFunction(FunctionNode):
  """Implementation of the hasValue() function."""

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('hasValue', operand, params)

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


# TODO: Fully define this placeholder for more than analytic use.
class IdForFunction(FunctionNode):
  """idFor() implementation to get raw ids specific to a reference type.

  For example, observation.subject.idFor('Patient') returns the raw id for
  the patient resource, rather than the typical 'Patient/<id>' string. This
  is convenient for tools that join data based on IDs, such as SQL or
  dataframe tables.
  """

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('idFor', operand, params)
    if not (len(params) == 1 and isinstance(params[0], LiteralNode) and
            fhir_types.is_string(cast(LiteralNode, params[0]).get_value())):
      raise ValueError(
          'IdFor function requires a single parameter of the resource type.')

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    # TODO: Support this for future non-SQL view users.
    raise NotImplementedError('Currently only supported for SQL-based views.')


class OfTypeFunction(FunctionNode):
  """ofType() implementation that returns only members of the given type."""

  struct_def_url: str

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('ofType', operand, params)
    if not (len(params) == 1 and isinstance(params[0], LiteralNode) and
            fhir_types.is_string(cast(LiteralNode, params[0]).get_value())):
      raise ValueError(
          'ofType function requires a single parameter of the datatype.')

    fhir_type = cast(Any, params[0]).get_value().value

    # Trim the FHIR prefix used for primitive types
    fhir_type = fhir_type[5:] if fhir_type.startswith('FHIR.') else fhir_type
    self.struct_def_url = f'http://hl7.org/fhir/StructureDefinition/{fhir_type}'

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    operand_messages = self._operand.evaluate(work_space)
    results = []

    for operand_message in operand_messages:
      if annotation_utils.get_structure_definition_url(
          operand_message.message) == self.struct_def_url:
        results.append(operand_message)

    return results


class MemberOfFunction(FunctionNode):
  """Implementation of the memberOf() function."""

  # Literal valueset URL and values to check for memberOf operations.
  value_set_url: str
  value_set_version: Optional[str] = None
  code_values: Optional[FrozenSet[CodeValue]] = None
  code_values_lock = threading.Lock()

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('memberOf', operand, params)
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
    else:
      raise ValueError(
          'MemberOf requires single valueset URL or proto parameter.')

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
    operand_messages = self._operand.evaluate(work_space)

    # If the code_values are not present, attempt to get them from FHIR context.
    with self.code_values_lock:
      if self.code_values is None:
        value_set_url = cast(
            Any, self._params[0].evaluate(work_space)[0].message).value
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

      # TODO: Add raw code support
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

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('not', operand, params)
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

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    if len(params) != 1:
      raise ValueError('Where expressions require a single parameter.')

    super().__init__('where', operand, params)

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

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    if len(params) != 1:
      raise ValueError('"All" expressions require a single parameter.')

    super().__init__('all', operand, params)

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

  def __init__(self, operand: ExpressionNode,
               params: List[ExpressionNode]) -> None:
    super().__init__('matches', operand, params)
    if not (len(params) == 1 and isinstance(params[0], LiteralNode) and
            fhir_types.is_string(cast(LiteralNode, params[0]).get_value())):
      raise ValueError('matches() requires a single string parameter.')

    regex = cast(Any, params[0]).get_value().value
    self.pattern = re.compile(regex) if regex else None

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

    operand_str = cast(str, operand_messages[0].message).value
    if not self.pattern.match(operand_str):
      result = False

    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(result),
            parent=None)
    ]


class EqualityNode(BinaryExpressionNode):
  """Implementation of FHIRPath equality and equivalence operators."""

  def __init__(self, handler: primitive_handler.PrimitiveHandler,
               operator: _ast.EqualityRelation.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    super().__init__(handler, left, right)
    self._operator = operator

    # TODO: Add support for FHIRPath equivalence operators.
    if (operator != _ast.EqualityRelation.Op.EQUAL and
        operator != _ast.EqualityRelation.Op.NOT_EQUAL):
      raise NotImplementedError('Implement all equality relations.')

  def are_equal(self, left: WorkSpaceMessage, right: WorkSpaceMessage) -> bool:
    """Returns true if left and right are equal."""
    # If left and right are the same types, simply compare the protos.
    if (left.message.DESCRIPTOR is right.message.DESCRIPTOR or
        left.message.DESCRIPTOR.full_name
        == right.message.DESCRIPTOR.full_name):
      return left.message == right.message

    # Left and right are different types, but may still be logically equal if
    # they are primitives and we are comparing a literal value to a FHIR proto
    # with an enum field. We can compare their JSON values to check that.
    if (annotation_utils.is_primitive_type(left.message) and
        annotation_utils.is_primitive_type(right.message)):
      left_wrapper = self._handler.primitive_wrapper_from_primitive(
          left.message)
      right_wrapper = self._handler.primitive_wrapper_from_primitive(
          right.message)
      return left_wrapper.json_value() == right_wrapper.json_value()

    return False

  def evaluate(self, work_space: WorkSpace) -> List[WorkSpaceMessage]:
    left_messages = self._left.evaluate(work_space)
    right_messages = self._right.evaluate(work_space)

    if not left_messages or not right_messages:
      return []

    are_equal = True

    if len(left_messages) != len(right_messages):
      are_equal = False
    else:
      for left_message, right_message in zip(left_messages, right_messages):
        if not self.are_equal(left_message, right_message):
          are_equal = False
          break

    result = (
        are_equal
        if self._operator == _ast.EqualityRelation.Op.EQUAL else not are_equal)
    return [
        WorkSpaceMessage(
            message=work_space.primitive_handler.new_boolean(result),
            parent=None)
    ]

  def to_fhir_path(self) -> str:
    return f'{self._left.to_fhir_path()} {self._operator.value} {self._right.to_fhir_path()}'


class BooleanOperatorNode(BinaryExpressionNode):
  """Implementation of FHIRPath boolean operations.

  See https://hl7.org/fhirpath/#boolean-logic for behavior definition.
  """

  def __init__(self, handler: primitive_handler.PrimitiveHandler,
               operator: _ast.BooleanLogic.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    super().__init__(handler, left, right)
    self._operator = operator

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

  def to_fhir_path(self) -> str:
    return (f'{self._left.to_fhir_path()} {self._operator.value} '
            f'{self._right.to_fhir_path()}')


class ArithmeticNode(BinaryExpressionNode):
  """Implementation of FHIRPath arithmetic operations.

  See https://hl7.org/fhirpath/#math-2 for behavior definition.
  """

  def __init__(self, handler: primitive_handler.PrimitiveHandler,
               operator: _ast.Arithmetic.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    super().__init__(handler, left, right)
    self._operator = operator

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
    return cast(str, string_message).value

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

      # TODO: Add support for arithmetic with units.
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

  def to_fhir_path(self) -> str:
    return (f'{self._left.to_fhir_path()} {self._operator.value} '
            f'{self._right.to_fhir_path()}')


class ComparisonNode(BinaryExpressionNode):
  """Implementation of the FHIRPath comparison functions."""

  def __init__(self, handler: primitive_handler.PrimitiveHandler,
               operator: _ast.Comparison.Op, left: ExpressionNode,
               right: ExpressionNode) -> None:
    super().__init__(handler, left, right)
    self._operator = operator

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

    if hasattr(left, 'value') and hasattr(right, 'value'):
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

  def to_fhir_path(self) -> str:
    return (f'{self._left.to_fhir_path()} {self._operator.value} '
            f'{self._right.to_fhir_path()}')


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
    'where': WhereFunction
}


# TODO: Complete implementation.
class FhirPathCompilerVisitor(_ast.FhirPathAstBaseVisitor):
  """AST visitor to compile a FHIRPath expression."""

  def __init__(
      self,
      handler: primitive_handler.PrimitiveHandler,
      fhir_context: context.FhirPathContext,
      data_type: Optional[_fhir_path_data_types.FhirPathDataType] = None
  ) -> None:
    self._handler = handler
    self._context = fhir_context
    self._data_type = data_type

  def visit_literal(self, literal: _ast.Literal) -> LiteralNode:

    if isinstance(literal.value, bool):
      return LiteralNode(
          self._handler.new_boolean(literal.value), str(literal.value))
    elif isinstance(literal.value, int):
      return LiteralNode(
          self._handler.new_integer(literal.value), str(literal.value))
    elif isinstance(literal.value, str):
      if literal.is_date_type:
        primitive_cls = (
            self._handler.date_time_cls
            if 'T' in literal.value else self._handler.date_cls)
        return LiteralNode(
            self._handler.primitive_wrapper_from_json_value(
                literal.value, primitive_cls).wrapped, f'@{literal.value}')
      else:
        return LiteralNode(
            self._handler.new_string(literal.value), f"'{literal.value}'")
    elif isinstance(literal.value, decimal.Decimal):
      return LiteralNode(
          self._handler.new_decimal(str(literal.value)), str(literal.value))
    else:
      raise ValueError(f'Unsupported literal value: {literal}.')

  def visit_identifier(self,
                       identifier: _ast.Identifier) -> InvokeExpressionNode:
    return InvokeExpressionNode(self._context, identifier.value,
                                RootMessageNode(self._data_type))

  def visit_indexer(self, indexer: _ast.Indexer, **kwargs: Any) -> Any:
    collection_result = self.visit(indexer.collection)
    index_result = self.visit(indexer.index)
    return IndexerNode(collection_result, index_result)

  def visit_arithmetic(self, arithmetic: _ast.Arithmetic, **kwargs: Any) -> Any:
    left = self.visit(arithmetic.lhs)
    right = self.visit(arithmetic.rhs)

    return ArithmeticNode(self._handler, arithmetic.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_type_expression(self, type_expression: _ast.TypeExpression,
                            **kwargs: Any) -> Any:
    raise NotImplementedError('TODO: implement `visit_type_expression`.')

  def visit_equality(self, equality: _ast.EqualityRelation,
                     **kwargs: Any) -> Any:

    left = self.visit(equality.lhs)
    right = self.visit(equality.rhs)

    return EqualityNode(self._handler, equality.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_comparison(self, comparison: _ast.Comparison, **kwargs: Any) -> Any:
    left = self.visit(comparison.lhs)
    right = self.visit(comparison.rhs)

    return ComparisonNode(self._handler, comparison.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_boolean_logic(self, boolean_logic: _ast.BooleanLogic,
                          **kwargs: Any) -> Any:
    left = self.visit(boolean_logic.lhs)
    right = self.visit(boolean_logic.rhs)

    return BooleanOperatorNode(self._handler, boolean_logic.op, left, right)  # pytype: disable=wrong-arg-types  # enable-nested-classes

  def visit_membership(self, membership: _ast.MembershipRelation,
                       **kwargs: Any) -> Any:
    raise NotImplementedError('TODO: implement `visit_membership`.')

  def visit_union(self, union: _ast.UnionOp, **kwargs: Any) -> Any:
    raise NotImplementedError('TODO: implement `visit_union`.')

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
      return LiteralNode(modified_value,
                         f'{str(polarity.op)}{operand_node.to_fhir_path()}')
    else:
      return NumericPolarityNode(operand_node, polarity)

  def visit_invocation(self,
                       invocation: _ast.Invocation) -> InvokeExpressionNode:
    # TODO: Placeholder for limited invocation usage.
    # Function invocation
    if isinstance(invocation.rhs, _ast.Function):
      return self.visit_function(invocation.rhs, operand=invocation.lhs)

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
        self.visit(operand)
        if operand is not None else RootMessageNode(self._data_type))
    params = [self.visit(param) for param in function.params]
    return function_class(operand_node, params)

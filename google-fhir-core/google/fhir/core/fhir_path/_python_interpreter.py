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
"""Functionality to compute FHIR in Python from FHIRPath expressions."""

import dataclasses
import decimal
import itertools
from typing import Any, Dict, List, Optional, cast

from google.protobuf import descriptor
from google.protobuf import message
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import quantity
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import annotation_utils
from google.fhir.core.utils import fhir_types
from google.fhir.core.utils import proto_utils


def get_children_from_message(
    parent: message.Message, json_name: str
) -> List[message.Message]:
  """Gets the child messages in the field with the given JSON name."""
  target_field = None

  for field in parent.DESCRIPTOR.fields:
    if field.json_name == json_name:
      target_field = field
      break

  if target_field is None or not proto_utils.field_is_set(
      parent, target_field.name
  ):
    return []

  results = proto_utils.get_value_at_field(parent, target_field.name)

  # Wrap non-repeated items in an array per FHIRPath specification.
  if target_field.label != descriptor.FieldDescriptor.LABEL_REPEATED:
    return [results]
  else:
    return results


def _is_numeric(
    message_or_descriptor: annotation_utils.MessageOrDescriptorBase,
) -> bool:
  return (
      fhir_types.is_decimal(message_or_descriptor)
      or fhir_types.is_integer(message_or_descriptor)
      or fhir_types.is_positive_integer(message_or_descriptor)
      or fhir_types.is_unsigned_integer(message_or_descriptor)
  )


def _evaluate_arithmetic_operator(
    op: _ast.Arithmetic.Op,
    left: Optional[decimal.Decimal],
    right: Optional[decimal.Decimal],
) -> Optional[decimal.Decimal]:
  """Applies the FHIRPath arithmetic evaluataion semantics."""
  # Explicit comparison needed for FHIRPath empty/none semantics.
  # pylint: disable=g-bool-id-comparison
  if left is None or right is None:
    return None

  if op == _ast.Arithmetic.Op.MULTIPLICATION:
    return left * right
  elif op == _ast.Arithmetic.Op.ADDITION:
    return left + right
  elif op == _ast.Arithmetic.Op.SUBTRACTION:
    return left - right

  # Division operators need to check if denominator is 0.
  if right == 0.0:
    return None

  if op == _ast.Arithmetic.Op.DIVISION:
    return left / right
  elif op == _ast.Arithmetic.Op.MODULO:
    return left % right
  else:  # op == _ast.Arithmetic.Op.TRUNCATED_DIVISON:
    return left // right
  # pylint: enable=g-bool-id-comparison


def _evaluate_boolean_operator(
    op: _ast.BooleanLogic.Op, left: Optional[bool], right: Optional[bool]
) -> Optional[bool]:
  """Applies the FHIRPath boolean evaluataion semantics."""
  # Explicit comparison needed for FHIRPath empty/none semantics.
  # pylint: disable=g-bool-id-comparison
  if op == _ast.BooleanLogic.Op.AND:
    # 'None and False' returns False in FHIRPath, unlike Python.
    if (left is None and right is False) or (right is None and left is False):
      return False
    else:
      return left and right
  elif op == _ast.BooleanLogic.Op.OR:
    return left or right
  elif op == _ast.BooleanLogic.Op.IMPLIES:
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


def _evaluate_comparison_operator(
    op: _ast.Comparison.Op, left: Any, right: Any
) -> bool:
  """Applies the FHIRPath comparison evaluation semantics."""
  if op == _ast.Comparison.Op.LESS_THAN:
    return left < right
  elif op == _ast.Comparison.Op.GREATER_THAN:
    return left > right
  elif op == _ast.Comparison.Op.LESS_THAN_OR_EQUAL:
    return left <= right
  else:  # op == _ast.Comparison.Op.GREATER_THAN_OR_EQUAL:
    return left >= right


def _resolve_if_choice_type(
    fhir_message: message.Message,
) -> Optional[message.Message]:
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
    return cast(
        message.Message,
        proto_utils.get_value_at_field(fhir_message, choice_field),
    )
  return fhir_message


@dataclasses.dataclass
class WorkSpaceMessage:
  """Message with parent context, as needed for some FHIRPath expressions."""

  message: message.Message
  parent: Optional['WorkSpaceMessage']


@dataclasses.dataclass
class WorkSpace:
  """Working memory and context for evaluating FHIRPath expressions."""

  message_context_stack: List[WorkSpaceMessage]

  def root_message(self) -> WorkSpaceMessage:
    return self.message_context_stack[0]

  def current_message(self) -> WorkSpaceMessage:
    return self.message_context_stack[-1]

  def push_message(self, workspace_message: WorkSpaceMessage) -> None:
    self.message_context_stack.append(workspace_message)

  def pop_message(self) -> None:
    self.message_context_stack.pop()


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


def _to_string(string_messages: List[WorkSpaceMessage]) -> str:
  """Returns empty string for None messages."""
  if not string_messages:
    return ''

  if len(string_messages) != 1:
    raise ValueError(
        'FHIRPath arithmetic must have single elements on each side.'
    )
  string_message = string_messages[0].message

  if string_message is None:
    return ''

  if not fhir_types.is_string(string_message):
    raise ValueError('String concatenation only accepts str or None operands.')
  return cast(str, string_message).value  # pytype: disable=attribute-error


def _messages_equal(
    handler: primitive_handler.PrimitiveHandler,
    left: WorkSpaceMessage,
    right: WorkSpaceMessage,
) -> bool:
  """Returns true if left and right are equal."""
  # If left and right are the same types, simply compare the protos.
  if (
      left.message.DESCRIPTOR is right.message.DESCRIPTOR
      or left.message.DESCRIPTOR.full_name == right.message.DESCRIPTOR.full_name
  ):
    if (
        annotation_utils.get_structure_definition_url(left.message)
        == _evaluation.QUANTITY_URL
        and annotation_utils.get_structure_definition_url(right.message)
        == _evaluation.QUANTITY_URL
    ):
      return quantity.quantity_from_proto(
          left.message
      ) == quantity.quantity_from_proto(right.message)
    return left.message == right.message

  # Left and right are different types, but may still be logically equal if
  # they are primitives and we are comparing a literal value to a FHIR proto
  # with an enum field. We can compare their JSON values to check that.
  if annotation_utils.is_primitive_type(
      left.message
  ) and annotation_utils.is_primitive_type(right.message):
    left_wrapper = handler.primitive_wrapper_from_primitive(left.message)
    right_wrapper = handler.primitive_wrapper_from_primitive(right.message)
    return left_wrapper.json_value() == right_wrapper.json_value()

  return False


def _is_element_in_collection(
    handler: primitive_handler.PrimitiveHandler,
    element: List[WorkSpaceMessage],
    collection: List[WorkSpaceMessage],
) -> List[WorkSpaceMessage]:
  """Indicates if `element` is a member of `collection`."""
  # If the element is empty, the result is empty.
  if not element:
    return []

  # If the element has multiple items, an error is returned.
  if len(element) != 1:
    raise ValueError(
        'Right hand side of "contains" operator must be a single element.'
    )

  # If the element operand is a collection with a single item, the
  # operator returns true if the item is in the collection using
  # equality semantics.
  # If the collection is empty, the result is false.
  result = any(
      _messages_equal(handler, element[0], item) for item in collection
  )

  return [WorkSpaceMessage(message=handler.new_boolean(result), parent=None)]


class PythonInterpreter(_evaluation.ExpressionNodeBaseVisitor):
  """Traverses the ExpressionNode tree and evaluates expressions in python."""

  def __init__(self, handler: primitive_handler.PrimitiveHandler):
    self._primitive_handler = handler
    self._work_space: WorkSpace = None

  def evaluate(
      self, expression: _evaluation.ExpressionNode, work_space: WorkSpace
  ) -> List[WorkSpaceMessage]:
    self._work_space = work_space
    return self.visit(expression)

  def visit_root(
      self, root: _evaluation.RootMessageNode
  ) -> List[WorkSpaceMessage]:
    return [self._work_space.root_message()]

  def visit_reference(
      self, reference: _evaluation.ExpressionNode
  ) -> List[WorkSpaceMessage]:
    return [self._work_space.current_message()]

  def visit_literal(
      self, literal: _evaluation.LiteralNode
  ) -> List[WorkSpaceMessage]:
    # Represent null as an empty list rather than a list with a None element.
    value = literal.get_value()
    if value is None:
      return []

    return [
        WorkSpaceMessage(
            message=value, parent=self._work_space.current_message()
        )
    ]

  def visit_invoke_expression(
      self, identifier: _evaluation.InvokeExpressionNode
  ) -> List[WorkSpaceMessage]:
    operand_messages = self.visit(identifier.parent_node)
    results = []
    for operand_message in operand_messages:
      operand_results = get_children_from_message(
          operand_message.message, identifier.identifier
      )
      for operand_result in operand_results:
        resolved_result = _resolve_if_choice_type(operand_result)
        if resolved_result is not None:
          results.append(
              WorkSpaceMessage(message=resolved_result, parent=operand_message)
          )

    return results

  def visit_indexer(
      self, indexer: _evaluation.IndexerNode
  ) -> List[WorkSpaceMessage]:
    collection_messages = self.visit(indexer.collection)
    index_messages = self.visit(indexer.index)
    index = _to_int(index_messages)
    if index is None:
      raise ValueError('Expected a non-empty index')

    # According to the spec, if the array is empty or the index is out of bounds
    # an empty array is returned.
    # https://hl7.org/fhirpath/#index-integer-collection
    if not collection_messages or index >= len(collection_messages):
      return []

    return [collection_messages[index]]

  def visit_arithmetic(
      self, arithmetic: _evaluation.ArithmeticNode
  ) -> List[WorkSpaceMessage]:
    left_messages = self.visit(arithmetic.left)
    right_messages = self.visit(arithmetic.right)
    result = None

    # String concatenation is the only operator that doesn't return an empty
    # array if one of the operands is None so we need to special case it.
    if arithmetic.op == _ast.Arithmetic.Op.STRING_CONCATENATION:
      result = _to_string(left_messages) + _to_string(right_messages)
    else:
      # Propagate empty/null values if they exist in operands.
      if not left_messages or not right_messages:
        return []

      if len(left_messages) != 1 or len(right_messages) != 1:
        raise ValueError(
            'FHIRPath arithmetic must have single elements on each side.'
        )

      left = left_messages[0].message
      right = right_messages[0].message

      # TODO(b/226131330): Add support for arithmetic with units.
      left_value = cast(Any, left).value
      right_value = cast(Any, right).value

      if (
          fhir_types.is_string(left)
          and fhir_types.is_string(right)
          and arithmetic.op == _ast.Arithmetic.Op.ADDITION
      ):
        result = left_value + right_value
      elif _is_numeric(left) and _is_numeric(right):
        left_value = decimal.Decimal(left_value)
        right_value = decimal.Decimal(right_value)
        result = _evaluate_arithmetic_operator(
            arithmetic.op, left_value, right_value
        )
      else:
        raise ValueError(
            f'Cannot {arithmetic.op.value} {left.DESCRIPTOR.full_name} with '
            f'{right.DESCRIPTOR.full_name}.'
        )

    if result is None:
      return []
    elif isinstance(result, str):
      return [
          WorkSpaceMessage(
              message=self._primitive_handler.new_string(result), parent=None
          )
      ]
    else:
      return [
          WorkSpaceMessage(
              message=self._primitive_handler.new_decimal(str(result)),
              parent=None,
          )
      ]
    pass

  def visit_equality(
      self, equality: _evaluation.EqualityNode
  ) -> List[WorkSpaceMessage]:
    # TODO(b/234657818): Add support for FHIRPath equivalence operators.
    if (
        equality.op != _ast.EqualityRelation.Op.EQUAL
        and equality.op != _ast.EqualityRelation.Op.NOT_EQUAL
    ):
      raise NotImplementedError('Implement all equality relations.')

    left_messages = self.visit(equality.left)
    right_messages = self.visit(equality.right)

    if not left_messages or not right_messages:
      return []

    are_equal = True

    if len(left_messages) != len(right_messages):
      are_equal = False
    else:
      are_equal = all(
          _messages_equal(self._primitive_handler, left_message, right_message)
          for left_message, right_message in zip(left_messages, right_messages)
      )

    result = (
        are_equal
        if equality.op == _ast.EqualityRelation.Op.EQUAL
        else not are_equal
    )
    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(result), parent=None
        )
    ]

  def visit_comparison(
      self, comparison: _evaluation.ComparisonNode
  ) -> List[WorkSpaceMessage]:
    left_messages = self.visit(comparison.left)
    right_messages = self.visit(comparison.right)
    result = None

    # Propagate empty/null values if they exist in operands.
    if not left_messages or not right_messages:
      return []

    if len(left_messages) != 1 or len(right_messages) != 1:
      raise ValueError(
          'FHIRPath comparisons must have single elements on each side.'
      )

    left = left_messages[0].message
    right = right_messages[0].message

    if (
        annotation_utils.get_structure_definition_url(left)
        == _evaluation.QUANTITY_URL
        and annotation_utils.get_structure_definition_url(right)
        == _evaluation.QUANTITY_URL
    ):
      result = _evaluate_comparison_operator(
          comparison.op,
          quantity.quantity_from_proto(left),
          quantity.quantity_from_proto(right),
      )
    elif hasattr(left, 'value') and hasattr(right, 'value'):
      left_value = cast(Any, left).value
      right_value = cast(Any, right).value
      # Wrap decimal types to ensure numeric rather than alpha comparison
      if fhir_types.is_decimal(left) and fhir_types.is_decimal(right):
        left_value = decimal.Decimal(left_value)
        right_value = decimal.Decimal(right_value)
      result = _evaluate_comparison_operator(
          comparison.op, left_value, right_value
      )
    elif (fhir_types.is_date(left) or fhir_types.is_date_time(left)) and (
        fhir_types.is_date(right) or fhir_types.is_date_time(right)
    ):
      # Both left and right are date-related types, so we can compare
      # timestamps.
      left_value = cast(Any, left).value_us
      right_value = cast(Any, right).value_us
      result = _evaluate_comparison_operator(
          comparison.op, left_value, right_value
      )
    else:
      raise ValueError(
          f'{left.DESCRIPTOR.full_name} not comaprable with '
          f'{right.DESCRIPTOR.full_name}.'
      )

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(result), parent=None
        )
    ]

  def visit_boolean_op(
      self, boolean_logic: _evaluation.BooleanOperatorNode
  ) -> List[WorkSpaceMessage]:
    left_messages = self.visit(boolean_logic.left)
    right_messages = self.visit(boolean_logic.right)
    left = _to_boolean(left_messages)
    right = _to_boolean(right_messages)

    result = _evaluate_boolean_operator(boolean_logic.op, left, right)

    if result is None:
      return []
    else:
      return [
          WorkSpaceMessage(
              message=self._primitive_handler.new_boolean(result), parent=None
          )
      ]

  def visit_membership(
      self, relation: _evaluation.MembershipRelationNode
  ) -> List[WorkSpaceMessage]:
    left_messages = self.visit(relation.left)
    right_messages = self.visit(relation.right)
    if isinstance(relation, _evaluation.InNode):
      return _is_element_in_collection(
          self._primitive_handler, left_messages, right_messages
      )
    else:  # _evaluation.ContainsNode
      return _is_element_in_collection(
          self._primitive_handler, right_messages, left_messages
      )

  def visit_union(self, union: _evaluation.UnionNode) -> List[WorkSpaceMessage]:
    left_messages = self.visit(union.left)
    right_messages = self.visit(union.right)

    # Build a set of unique messages by using their json_value to
    # determine uniqueness. As in the _messages_equal function, we use
    # the json_value to determine message equality.
    messages_union: Dict[str, WorkSpaceMessage] = {}
    for work_space_message in itertools.chain(left_messages, right_messages):
      json_value = self._primitive_handler.primitive_wrapper_from_primitive(
          work_space_message.message
      ).json_value()
      messages_union.setdefault(json_value, work_space_message)

    return list(messages_union.values())

  def visit_polarity(
      self, polarity: _evaluation.NumericPolarityNode
  ) -> List[WorkSpaceMessage]:
    operand_messages = self.visit(polarity.parent_node)
    if not operand_messages:
      return []

    if len(operand_messages) != 1:
      raise ValueError('FHIRPath polarity must have a single value.')

    operand_message = operand_messages[0]
    if not _is_numeric(operand_message.message):
      raise ValueError('Polarity operators allowed only on numeric types.')

    value = decimal.Decimal(
        proto_utils.get_value_at_field(operand_message.message, 'value')
    )
    if polarity.op == _ast.Polarity.Op.NEGATIVE:
      result = value.copy_negate()
    else:
      result = value

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_decimal(str(result)),
            parent=None,
        )
    ]

  def visit_function(
      self, function: _evaluation.FunctionNode
  ) -> List[WorkSpaceMessage]:
    parent_result = self.visit(function.parent_node)
    func_name = f'_visit_{function.NAME}'
    if not hasattr(self, func_name):
      raise NotImplementedError(f'{function.NAME} is not supported in python.')
    return getattr(self, func_name)(function, parent_result)

  def _visit_exists(
      self,
      function: _evaluation.ExistsFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    # Exists is true if there messages is non-null and contains at least one
    # item, which maps to Python array truthiness.
    exists = bool(operand_result)
    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(exists), parent=None
        )
    ]

  def _visit_count(
      self,
      function: _evaluation.CountFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    # Counts number of items in operand.
    count = len(operand_result)
    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_integer(count), parent=None
        )
    ]

  def _visit_empty(
      self,
      function: _evaluation.EmptyFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    # Determines if operand is empty.
    empty = not operand_result
    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(empty), parent=None
        )
    ]

  def _visit_first(
      self,
      function: _evaluation.FirstFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    if operand_result:
      return [operand_result[0]]
    else:
      return []

  def _visit_anyTrue(
      self,
      function: _evaluation.AnyTrueFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    for candidate in operand_result:
      self._work_space.push_message(candidate)
      try:
        if _to_boolean([candidate]):
          return [
              WorkSpaceMessage(
                  message=self._primitive_handler.new_boolean(True), parent=None
              )
          ]
      finally:
        self._work_space.pop_message()

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(False), parent=None
        )
    ]

  def _visit_hasValue(
      self,
      function: _evaluation.HasValueFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    is_primitive = len(
        operand_result
    ) == 1 and annotation_utils.is_primitive_type(operand_result[0].message)
    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(is_primitive),
            parent=None,
        )
    ]

  def _visit_ofType(
      self,
      function: _evaluation.OfTypeFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    results = []

    for operand_message in operand_result:
      url = annotation_utils.get_structure_definition_url(
          operand_message.message
      )
      if (
          url is not None
          and url.casefold() == function.struct_def_url.casefold()
      ):
        results.append(operand_message)

    return results

  def _visit_memberOf(
      self,
      function: _evaluation.MemberOfFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    result = False
    params_result = [self.visit(p) for p in function.params()]
    # If the code_values are not present, attempt to get them from FHIR context.
    with function.code_values_lock:
      if function.code_values is None:
        value_set_url_message = params_result[0][0].message
        value_set_url = cast(Any, value_set_url_message).value
        value_set_proto = function.context.get_value_set(value_set_url)

        if value_set_proto is None:
          raise ValueError(f'No value set {value_set_url} found.')
        function.code_values = _evaluation.to_code_values(value_set_proto)

    for workspace_message in operand_result:
      fhir_message = workspace_message.message
      if fhir_types.is_codeable_concept(fhir_message):
        for coding in cast(Any, fhir_message).coding:
          if (
              _evaluation.CodeValue(coding.system.value, coding.code.value)
              in function.code_values
          ):
            result = True
            break

      elif fhir_types.is_coding(fhir_message):
        if (
            _evaluation.CodeValue(coding.system.value, coding.code.value)
            in function.code_values
        ):
          result = True
          break

      # TODO(b/208900793): Add raw code support
      else:
        raise ValueError(
            f'MemberOf not supported on {fhir_message.DESCRIPTOR.full_name}'
        )

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(result), parent=None
        )
    ]

  def _visit_not(
      self,
      function: _evaluation.NotFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    if not operand_result:
      return []

    result = True
    for operand_message in operand_result:
      if not fhir_types.is_boolean(operand_message.message):
        raise ValueError('Boolean operators allowed only on boolean types.')
      result &= proto_utils.get_value_at_field(operand_message.message, 'value')

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(not result), parent=None
        )
    ]

  def _visit_where(
      self,
      function: _evaluation.WhereFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    results = []
    for candidate in operand_result:
      # Iterate through the candidates and evaluate them against the where
      # predicate in a local workspace, keeping those that match.
      self._work_space.push_message(candidate)
      try:
        predicate_result = self.visit(function.params()[0])
        if _to_boolean(predicate_result):
          results.append(candidate)
      finally:
        self._work_space.pop_message()
    return results

  def _visit_all(
      self,
      function: _evaluation.AllFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    all_match = True
    for candidate in operand_result:
      # Iterate through the candidates and evaluate them against the
      # predicate in a local workspace, and short circuit if one doesn't.
      self._work_space.push_message(candidate)
      try:
        predicate_result = self.visit(function.params()[0])
        if not _to_boolean(predicate_result):
          all_match = False
          break
      finally:
        self._work_space.pop_message()

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(all_match), parent=None
        )
    ]

  def _visit_matches(
      self,
      function: _evaluation.MatchesFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    result = True

    if not function.pattern or not operand_result:
      return []

    if len(operand_result) > 1 or not fhir_types.is_string(
        operand_result[0].message
    ):
      raise ValueError(
          'Input collection contains more than one item or is not of string '
          'type.'
      )

    operand_str = cast(Any, operand_result[0].message).value
    if not function.pattern.match(operand_str):
      result = False

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_boolean(result), parent=None
        )
    ]

  def _visit_toInteger(
      self,
      function: _evaluation.ToIntegerFunction,
      operand_result: List[WorkSpaceMessage],
  ) -> List[WorkSpaceMessage]:
    if not operand_result:
      return []

    # If the input collection contains multiple items, the evaluation
    # of the expression will end and signal an error to the calling
    # environment.
    if len(operand_result) > 1:
      raise ValueError(
          'toInteger() cannot be called on collections containing '
          'multiple items.'
      )

    # If the input collection contains a single item, this function
    # will return a single integer if:
    #
    # the item is an Integer
    # the item is a String and is convertible to an integer
    # the item is a Boolean, where true results in a 1 and false results in a 0.
    #
    # If the item is not one the above types, the result is empty.
    operand = operand_result[0].message

    is_integer = (
        fhir_types.is_integer(operand)
        or fhir_types.is_positive_integer(operand)
        or fhir_types.is_unsigned_integer(operand)
    )
    if not (
        is_integer
        or fhir_types.is_string(operand)
        or fhir_types.is_boolean(operand)
    ):
      return []

    operand_value = proto_utils.get_value_at_field(operand, 'value')
    try:
      as_int = int(operand_value)
    except ValueError:
      return []

    return [
        WorkSpaceMessage(
            message=self._primitive_handler.new_integer(as_int), parent=None
        )
    ]

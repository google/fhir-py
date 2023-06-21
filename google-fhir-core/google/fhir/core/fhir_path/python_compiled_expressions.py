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
"""Functionality to execute a FHIRPath expression in Python."""

import decimal
from typing import List

from google.protobuf import message
from google.fhir.core import codes
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _python_interpreter
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import fhir_types
from google.fhir.core.utils import proto_utils


# TODO(b/208900793): support other result types and messages, and introspection
# on the actual returned type.
class EvaluationResult:
  """The result of a FHIRPath expression evaluation.

  Users should inspect and convert this to the expected target type based
  on their evaluation needs.
  """

  def __init__(
      self,
      messages: List[message.Message],
      work_space: _python_interpreter.WorkSpace,
  ):
    self._messages = messages
    self._work_space = work_space

  @property
  def messages(self) -> List[message.Message]:
    return self._messages

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

    # TODO(b/208900793): Check primitive type rather than assuming value.
    return proto_utils.get_value_at_field(self._messages[0], 'value')

  def as_bool(self) -> bool:
    """Returns the result as a boolean.

    Raises:
      ValueError if the `EvaluationResult` is not a single boolean.
    """
    if len(self._messages) != 1:
      raise ValueError('FHIRPath did not evaluate to a single boolean.')

    # TODO(b/208900793): Check primitive type rather than assuming value.
    return proto_utils.get_value_at_field(self._messages[0], 'value')

  def as_int(self) -> int:
    """Returns the result as an integer.

    Raises:
      ValueError if the `EvaluationResult` is not a single integer.
    """
    if len(self._messages) != 1:
      raise ValueError('FHIRPath did not evaluate to a single integer.')

    # TODO(b/208900793): Check primitive type rather than assuming value.
    return proto_utils.get_value_at_field(self._messages[0], 'value')

  def as_decimal(self) -> decimal.Decimal:
    """Returns the result as a decimal.

    Raises:
      ValueError if the `EvaluationResult` is not a single decimal.
    """
    if len(self._messages) != 1:
      raise ValueError('FHIRPath did not evaluate to a single decimal.')

    # TODO(b/208900793): Check primitive type rather than assuming value.
    return decimal.Decimal(
        proto_utils.get_value_at_field(self._messages[0], 'value')
    )


class PythonCompiledExpression:
  """Compiled FHIRPath expression for evaluation in python."""

  def __init__(
      self,
      root_node: _evaluation.ExpressionNode,
      handler: primitive_handler.PrimitiveHandler,
      fhir_path: str,
  ):
    self._root_node = root_node
    self._primitive_handler = handler
    self._fhir_path = fhir_path
    self._python_interpreter = _python_interpreter.PythonInterpreter(
        handler=self._primitive_handler
    )

  @classmethod
  def from_builder(
      cls, builder: expressions.Builder
  ) -> 'PythonCompiledExpression':
    return PythonCompiledExpression(
        builder.node, builder.primitive_handler, builder.fhir_path
    )

  @classmethod
  def compile(
      cls,
      fhir_path: str,
      handler: primitive_handler.PrimitiveHandler,
      structdef_url: str,
      fhir_context: context.FhirPathContext,
  ) -> 'PythonCompiledExpression':
    """Compiles the FHIRPath expression that targets the given structure."""

    structdef = fhir_context.get_structure_definition(structdef_url)
    data_type = _fhir_path_data_types.StructureDataType.from_proto(structdef)

    ast = _ast.build_fhir_path_ast(fhir_path)
    visitor = _evaluation.FhirPathCompilerVisitor(
        handler, fhir_context, data_type
    )

    root = visitor.visit(ast)
    return PythonCompiledExpression(root, handler, fhir_path)

  def evaluate(self, resource: message.Message) -> EvaluationResult:
    return self._evaluate(
        _python_interpreter.WorkSpaceMessage(message=resource, parent=None)
    )

  @property
  def fhir_path(self) -> str:
    """The FHIRPath expression as a string."""
    return self._fhir_path

  def _evaluate(
      self, workspace_message: _python_interpreter.WorkSpaceMessage
  ) -> EvaluationResult:
    work_space = _python_interpreter.WorkSpace(
        message_context_stack=[workspace_message]
    )
    results = self._python_interpreter.evaluate(self._root_node, work_space)
    return EvaluationResult(
        messages=[result.message for result in results], work_space=work_space
    )

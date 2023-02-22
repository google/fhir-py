# Copyright 2023 Google LLC
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

"""Support for the MessageNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class MessageNode(operator_expression_node.OperatorExpressionNode):
  """The MessageNode operator is used to support errors, warnings, messages, and tracing in an ELM evaluation environment."""

  def __init__(
      self=None,
      signature=None,
      source=None,
      condition=None,
      code=None,
      severity=None,
      message=None,
  ):
    super().__init__(signature)
    self.source = source
    self.condition = condition
    self.code = code
    self.severity = severity
    self.message = message

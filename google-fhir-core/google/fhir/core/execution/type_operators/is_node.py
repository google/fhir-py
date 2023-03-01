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

"""Support for the IsNode."""
from google.fhir.core.execution.expressions import unary_expression_node


class IsNode(unary_expression_node.UnaryExpressionNode):
  """Of a result to be tested."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      is_type=None,
      is_type_specifier=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.is_type = is_type
    self.is_type_specifier = is_type_specifier

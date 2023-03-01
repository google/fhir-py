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

"""Support for the AsNode."""
from google.fhir.core.execution.expressions import unary_expression_node


class AsNode(unary_expression_node.UnaryExpressionNode):
  """The AsNode operator allows the result of an expression to be cast as a given target type."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      as_type=None,
      strict=False,
      as_type_specifier=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.as_type = as_type
    self.strict = strict
    self.as_type_specifier = as_type_specifier

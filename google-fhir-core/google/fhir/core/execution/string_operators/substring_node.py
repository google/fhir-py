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

"""Support for the SubstringNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class SubstringNode(operator_expression_node.OperatorExpressionNode):
  """The SubstringNode operator returns the string within stringToSub, starting at the 0-based index startIndex, and consisting of length characters."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      string_to_sub=None,
      start_index=None,
      length=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.string_to_sub = string_to_sub
    self.start_index = start_index
    self.length = length

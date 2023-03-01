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

"""Support for the SliceNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class SliceNode(operator_expression_node.OperatorExpressionNode):
  """The SliceNode operator returns a portion of the elements in a list, beginning at the start index and ending just before the ending index."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      source=None,
      start_index=None,
      end_index=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.source = source
    self.start_index = start_index
    self.end_index = end_index

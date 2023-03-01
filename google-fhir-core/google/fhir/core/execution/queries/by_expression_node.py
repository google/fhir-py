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

"""Support for the ByExpressionNode."""
from google.fhir.core.execution import sort_by_item_node


class ByExpressionNode(sort_by_item_node.SortByItemNode):
  """The ByExpressionNode element specifies that the sort should be performed using the given expression and direction."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      direction=None,
      expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier, direction)
    self.expression = expression

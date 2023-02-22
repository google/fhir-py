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

"""Support for the ByColumnNode."""
from google.fhir.core.execution import sort_by_item_node


class ByColumnNode(sort_by_item_node.SortByItemNode):
  """The ByColumnNode element specifies that the sort should be performed using the given column and direction."""

  def __init__(self=None, direction=None, path=None):
    super().__init__(direction)
    self.path = path

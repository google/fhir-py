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

"""Support for the ForEachNode."""
from google.fhir.core.execution.expressions import expression_node


class ForEachNode(expression_node.ExpressionNode):
  """The ForEachNode expression iterates over the list of elements in the source element, and returns a list with the same number of elements, where each element in the new list is the result of evaluating the element expression for each element in the source list."""

  def __init__(self=None, scope=None, source=None, element=None):
    super().__init__()
    self.scope = scope
    self.source = source
    self.element = element

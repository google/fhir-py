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

"""Support for the ListNode."""
from google.fhir.core.execution.expressions import expression_node


class ListNode(expression_node.ExpressionNode):
  """ListNode, whose elements are the result of evaluating the arguments to the list selector, in order."""

  def __init__(self=None, type_specifier=None, element=None):
    super().__init__()
    self.type_specifier = type_specifier
    if element is None:
      self.element = []
    else:
      self.element = element

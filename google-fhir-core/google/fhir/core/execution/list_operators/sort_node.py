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

"""Support for the SortNode."""
from google.fhir.core.execution.expressions import expression_node


class SortNode(expression_node.ExpressionNode):
  """The SortNode operator returns a list with all the elements in source, sorted as described by the by element."""

  def __init__(self=None, source=None, by=None):
    super().__init__()
    self.source = source
    if by is None:
      self.by = []
    else:
      self.by = by

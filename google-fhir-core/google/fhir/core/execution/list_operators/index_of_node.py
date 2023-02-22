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

"""Support for the IndexOfNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class IndexOfNode(operator_expression_node.OperatorExpressionNode):
  """The IndexOfNode operator returns the 0-based index of the given element in the given source list."""

  def __init__(self=None, signature=None, source=None, element=None):
    super().__init__(signature)
    self.source = source
    self.element = element

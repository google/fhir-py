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

"""Support for the FirstNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class FirstNode(operator_expression_node.OperatorExpressionNode):
  """The FirstNode operator returns the first element in a list."""

  def __init__(self=None, signature=None, order_by=None, source=None):
    super().__init__(signature)
    self.order_by = order_by
    self.source = source

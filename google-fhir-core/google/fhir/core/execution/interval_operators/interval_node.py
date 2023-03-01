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

"""Support for the IntervalNode."""
from google.fhir.core.execution.expressions import expression_node


class IntervalNode(expression_node.ExpressionNode):
  """The IntervalNode selector defines an interval value."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      low_closed=True,
      high_closed=True,
      low=None,
      low_closed_expression=None,
      high=None,
      high_closed_expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.low_closed = low_closed
    self.high_closed = high_closed
    self.low = low
    self.low_closed_expression = low_closed_expression
    self.high = high
    self.high_closed_expression = high_closed_expression

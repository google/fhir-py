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

"""Support for the DateTimeNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class DateTimeNode(operator_expression_node.OperatorExpressionNode):
  """The DateTimeNode operator constructs a DateTimeNode value from the given components."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      year=None,
      month=None,
      day=None,
      hour=None,
      minute=None,
      second=None,
      millisecond=None,
      timezone_offset=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.year = year
    self.month = month
    self.day = day
    self.hour = hour
    self.minute = minute
    self.second = second
    self.millisecond = millisecond
    self.timezone_offset = timezone_offset

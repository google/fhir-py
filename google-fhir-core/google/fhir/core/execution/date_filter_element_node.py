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

"""Support for the DateFilterElementNode."""
from google.fhir.core.execution import element_node


class DateFilterElementNode(element_node.ElementNode):
  """Specifies a date-valued filter criteria for use within a retrieve, specified as either a date-valued [property], a date-value [lowproperty] and [highproperty] or a [search], and an expression that evaluates to a date or time type, an interval of a date or time type, or a time-valued quantity."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      property_=None,
      low_property=None,
      high_property=None,
      search=None,
      value=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.property_ = property_
    self.low_property = low_property
    self.high_property = high_property
    self.search = search
    self.value = value

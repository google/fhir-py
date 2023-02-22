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

"""Support for the CodeFilterElementNode."""
from google.fhir.core.execution import element_node


class CodeFilterElementNode(element_node.ElementNode):
  """Specifies a terminology filter criteria for use within a retrieve, specified as either [property] [comparator] [value] or [search] [comparator] [value]."""

  def __init__(
      self=None,
      property_=None,
      value_set_property=None,
      search=None,
      comparator=None,
      value=None,
  ):
    super().__init__()
    self.property_ = property_
    self.value_set_property = value_set_property
    self.search = search
    self.comparator = comparator
    self.value = value

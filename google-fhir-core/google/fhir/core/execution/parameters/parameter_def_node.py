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

"""Support for the ParameterDefNode."""
from google.fhir.core.execution import element_node


class ParameterDefNode(element_node.ElementNode):
  """Defines a parameter that can be referenced by name anywhere within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      parameter_type=None,
      access_level='Public',
      default=None,
      parameter_type_specifier=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.parameter_type = parameter_type
    self.access_level = access_level
    self.default = default
    self.parameter_type_specifier = parameter_type_specifier

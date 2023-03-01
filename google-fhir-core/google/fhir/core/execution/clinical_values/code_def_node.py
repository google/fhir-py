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

"""Support for the CodeDefNode."""
from google.fhir.core.execution import element_node


class CodeDefNode(element_node.ElementNode):
  """Defines a code identifier that can then be used to reference single codes anywhere within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      id_=None,
      display=None,
      access_level='Public',
      code_system=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.id_ = id_
    self.display = display
    self.access_level = access_level
    self.code_system = code_system

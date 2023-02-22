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

"""Support for the ConceptDefNode."""
from google.fhir.core.execution import element_node


class ConceptDefNode(element_node.ElementNode):
  """Defines a concept identifier that can then be used to reference single concepts anywhere within an expression."""

  def __init__(
      self=None, name=None, display=None, access_level=None, code=None
  ):
    super().__init__()
    self.name = name
    self.display = display
    self.access_level = access_level
    if code is None:
      self.code = []
    else:
      self.code = code

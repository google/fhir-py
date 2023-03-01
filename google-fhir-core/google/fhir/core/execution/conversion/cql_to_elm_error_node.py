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

"""Support for the CqlToElmErrorNode."""
from google.fhir.core.execution import locator_node


class CqlToElmErrorNode(locator_node.LocatorNode):
  """Represents CQL to ELM conversion errors."""

  def __init__(
      self,
      library_system=None,
      library_id=None,
      library_version=None,
      start_line=None,
      start_char=None,
      end_line=None,
      end_char=None,
      message=None,
      error_type=None,
      error_severity=None,
      target_include_library_system=None,
      target_include_library_id=None,
      target_include_library_version_id=None,
  ):
    super().__init__(
        library_system,
        library_id,
        library_version,
        start_line,
        start_char,
        end_line,
        end_char,
    )
    self.message = message
    self.error_type = error_type
    self.error_severity = error_severity
    self.target_include_library_system = target_include_library_system
    self.target_include_library_id = target_include_library_id
    self.target_include_library_version_id = target_include_library_version_id

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

"""Support for the LibraryNode."""
from google.fhir.core.execution import element_node


class LibraryNode(element_node.ElementNode):
  """A LibraryNode is an instance of a CQL-ELM library."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      identifier=None,
      schema_identifier=None,
      usings=None,
      includes=None,
      parameters=None,
      code_systems=None,
      value_sets=None,
      codes=None,
      concepts=None,
      contexts=None,
      statements=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.identifier = identifier
    self.schema_identifier = schema_identifier
    self.usings = usings
    self.includes = includes
    self.parameters = parameters
    self.code_systems = code_systems
    self.value_sets = value_sets
    self.codes = codes
    self.concepts = concepts
    self.contexts = contexts
    self.statements = statements

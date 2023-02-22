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

"""Support for the ParameterTypeSpecifierNode."""
from google.fhir.core.execution.type_specifiers import type_specifier_node


class ParameterTypeSpecifierNode(type_specifier_node.TypeSpecifierNode):
  """A type which is generic class parameter such as T in MyGeneric."""

  def __init__(self=None, parameter_name=None):
    super().__init__()
    self.parameter_name = parameter_name

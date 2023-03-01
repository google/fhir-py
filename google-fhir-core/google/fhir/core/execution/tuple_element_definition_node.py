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

"""Support for the TupleElementDefinitionNode."""
from google.fhir.core.execution import element_node


class TupleElementDefinitionNode(element_node.ElementNode):
  """TupleElementDefinitionNode defines the name and type of a single element within a TupleTypeSpecifier."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      type_=None,
      element_type=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.type_ = type_
    self.element_type = element_type

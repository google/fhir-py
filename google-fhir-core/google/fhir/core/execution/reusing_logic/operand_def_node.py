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

"""Support for the OperandDefNode."""
from google.fhir.core.execution import element_node


class OperandDefNode(element_node.ElementNode):
  """Defines an operand to a function that can be referenced by name anywhere within the body of a function definition."""

  def __init__(
      self=None, name=None, operand_type=None, operand_type_specifier=None
  ):
    super().__init__()
    self.name = name
    self.operand_type = operand_type
    self.operand_type_specifier = operand_type_specifier

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

"""Support for the FunctionRefNode."""
from google.fhir.core.execution.reusing_logic import expression_ref_node


class FunctionRefNode(expression_ref_node.ExpressionRefNode):
  """Defines an expression that invokes a previously defined function."""

  def __init__(
      self=None, name=None, library_name=None, signature=None, operand=None
  ):
    super().__init__(name, library_name)
    if signature is None:
      self.signature = []
    else:
      self.signature = signature
    if operand is None:
      self.operand = []
    else:
      self.operand = operand

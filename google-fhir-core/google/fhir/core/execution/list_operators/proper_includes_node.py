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

"""Support for the ProperIncludesNode."""
from google.fhir.core.execution.expressions import binary_expression_node


class ProperIncludesNode(binary_expression_node.BinaryExpressionNode):
  """The ProperIncludesNode operator returns true if the first operand includes the second, and is strictly larger."""

  def __init__(self=None, signature=None, operand=None, precision=None):
    super().__init__(signature, operand)
    self.precision = precision

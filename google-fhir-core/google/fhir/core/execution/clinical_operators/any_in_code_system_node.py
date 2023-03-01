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

"""Support for the AnyInCodeSystemNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class AnyInCodeSystemNode(operator_expression_node.OperatorExpressionNode):
  """The AnyInCodeSystemNode operator returns true if any of the given codes are in the given code system."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      codes=None,
      codesystem=None,
      codesystem_expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.codes = codes
    self.codesystem = codesystem
    self.codesystem_expression = codesystem_expression

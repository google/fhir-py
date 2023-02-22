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

"""Support for the AnyInValueSetNode."""
from google.fhir.core.execution.expressions import operator_expression_node


class AnyInValueSetNode(operator_expression_node.OperatorExpressionNode):
  """The AnyInValueSetNode operator returns true if any of the given codes are in the given value set."""

  def __init__(
      self=None,
      signature=None,
      codes=None,
      valueset=None,
      valueset_expression=None,
  ):
    super().__init__(signature)
    self.codes = codes
    self.valueset = valueset
    self.valueset_expression = valueset_expression

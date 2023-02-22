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

"""Support for the CollapseNode."""
from google.fhir.core.execution.expressions import binary_expression_node


class CollapseNode(binary_expression_node.BinaryExpressionNode):
  """The CollapseNode operator returns the unique set of intervals that completely covers the ranges present in the given list of intervals."""

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

"""Support for the IdentifierRefNode."""
from google.fhir.core.execution.expressions import expression_node


class IdentifierRefNode(expression_node.ExpressionNode):
  """Defines an expression that references an identifier that is either unresolved, or has been resolved to an attribute in an unambiguous iteration scope such as a sort."""

  def __init__(self=None, name=None, library_name=None):
    super().__init__()
    self.name = name
    self.library_name = library_name

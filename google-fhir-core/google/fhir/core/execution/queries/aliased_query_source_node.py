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

"""Support for the AliasedQuerySourceNode."""
from google.fhir.core.execution import element_node


class AliasedQuerySourceNode(element_node.ElementNode):
  """The AliasedQuerySourceNode element defines a single source for inclusion in a query scope."""

  def __init__(self=None, alias=None, expression=None):
    super().__init__()
    self.alias = alias
    self.expression = expression

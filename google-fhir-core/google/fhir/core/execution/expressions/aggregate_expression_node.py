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

"""Support for the AggregateExpressionNode."""
from google.fhir.core.execution.expressions import expression_node


class AggregateExpressionNode(expression_node.ExpressionNode):
  """Aggregate expressions perform operations on lists of data, either directly on a list of scalars, or indirectly on a list of objects, with a reference to a property present on each object in the list."""

  def __init__(self=None, path=None, signature=None, source=None):
    super().__init__()
    self.path = path
    if signature is None:
      self.signature = []
    else:
      self.signature = signature
    self.source = source

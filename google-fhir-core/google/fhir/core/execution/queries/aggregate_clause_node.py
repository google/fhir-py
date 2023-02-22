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

"""Support for the AggregateClauseNode."""
from google.fhir.core.execution import element_node


class AggregateClauseNode(element_node.ElementNode):
  """The AggregateClauseNode element defines the result of the query in terms of an aggregation expression performed for each item in the query."""

  def __init__(
      self=None, identifier=None, distinct=None, expression=None, starting=None
  ):
    super().__init__()
    self.identifier = identifier
    self.distinct = distinct
    self.expression = expression
    self.starting = starting

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

"""Support for the RelationshipClauseNode."""
from google.fhir.core.execution.queries import aliased_query_source_node


class RelationshipClauseNode(aliased_query_source_node.AliasedQuerySourceNode):
  """The RelationshipClauseNode element allows related sources to be used to restrict the elements included from another source in a query scope."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      alias=None,
      expression=None,
      such_that=None,
  ):
    super().__init__(result_type_name, result_type_specifier, alias, expression)
    self.such_that = such_that

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

"""Support for the RetrieveNode."""
from google.fhir.core.execution.expressions import expression_node


class RetrieveNode(expression_node.ExpressionNode):
  """The retrieve expression defines clinical data that will be used by the artifact."""

  def __init__(
      self=None,
      data_type=None,
      template_id=None,
      id_property=None,
      id_search=None,
      context_property=None,
      context_search=None,
      code_property=None,
      code_search=None,
      code_comparator=None,
      value_set_property=None,
      date_property=None,
      date_low_property=None,
      date_high_property=None,
      date_search=None,
      included_in=None,
      id_=None,
      codes=None,
      date_range=None,
      context=None,
      include=None,
      code_filter=None,
      date_filter=None,
      other_filter=None,
  ):
    super().__init__()
    self.data_type = data_type
    self.template_id = template_id
    self.id_property = id_property
    self.id_search = id_search
    self.context_property = context_property
    self.context_search = context_search
    self.code_property = code_property
    self.code_search = code_search
    self.code_comparator = code_comparator
    self.value_set_property = value_set_property
    self.date_property = date_property
    self.date_low_property = date_low_property
    self.date_high_property = date_high_property
    self.date_search = date_search
    self.included_in = included_in
    self.id_ = id_
    self.codes = codes
    self.date_range = date_range
    self.context = context
    if include is None:
      self.include = []
    else:
      self.include = include
    if code_filter is None:
      self.code_filter = []
    else:
      self.code_filter = code_filter
    if date_filter is None:
      self.date_filter = []
    else:
      self.date_filter = date_filter
    if other_filter is None:
      self.other_filter = []
    else:
      self.other_filter = other_filter

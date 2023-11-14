#
# Copyright 2021 Google LLC
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
"""Library for running FHIR view definitions against a FHIR search response.

This module allows users to run FHIR Views against a Bundle resource response
from a FHIR search. Users can retrieve the results as a DataFrame.
"""

from typing import Any, Dict, List, Mapping
import pandas
from google.protobuf import message
from google.fhir.r4.proto.core.resources import bundle_and_contained_resource_pb2
from google.fhir.core.fhir_path import python_compiled_expressions
from google.fhir.core.utils import proto_utils
from google.fhir.core.utils import resource_utils
from google.fhir.r4 import fhir_path
from google.fhir.views import views


class FhirSearchRunner:
  """FHIR Views runner used to create views over a FHIR search response."""

  def __init__(self, client, search_query: str) -> None:
    """Initializer.

    Initializes the FhirSearchRunner with user provided FHIR Client, and search
    query.

    Args:
      client: FHIR Client for the FHIR server where queries will be run against.
        Views will be created from the response.
      search_query: Query used to fetch the subset of data from the FHIR server.
    """
    self._client = client
    self._search_query = search_query

  def to_dataframe(self, view: views.View) -> pandas.DataFrame:
    """Returns a Pandas dataframe of the results, if Pandas is installed.

    Args:
      view: the view that defines the desired format of the flattened FHIR
        output.

    Returns:
      pandas.DataFrame: dataframe of the view contents.
    """
    resources = self._fetch_bundle_entries()
    flattened_view = self._flatten_fhir_to_view(view, resources)

    return pandas.json_normalize(flattened_view)

  def _fetch_bundle_entries(self):
    """Returns a list of entries from the FHIR search response bundle."""
    # TODO(b/271913842): Handle errors from client or missing client.
    return self._client.search(self._search_query).entry

  def _flatten_fhir_to_view(
      self,
      view: views.View,
      entries: List[bundle_and_contained_resource_pb2.Bundle.Entry],
  ):
    """Returns a list of flattened resources."""
    flat_resources = []
    select_expr = self._get_select_compiled_expressions(view)
    constraints = self._get_constraint_compiled_expressions(view)

    for entry in entries:
      resource = resource_utils.get_contained_resource(entry.resource)
      if not self._should_include_resource_in_view(constraints, resource):
        continue

      flat_resources.append(self._flatten_resource(resource, select_expr))
    return flat_resources

  def _get_select_compiled_expressions(self, view):
    select_expr = {}
    for builder in view.get_select_expressions():
      select_expr[builder.column_name] = self._compile_expression(
          view, builder.fhir_path
      )
    return select_expr

  def _get_constraint_compiled_expressions(self, view):
    constraints = []
    for builder in view.get_constraint_expressions():
      constraints.append(self._compile_expression(view, builder.fhir_path))
    return constraints

  def _compile_expression(self, view, fhir_path_expression):
    return fhir_path.compile_expression(
        view.get_structdef_url(),
        view.get_fhir_path_context(),
        fhir_path_expression,
    )

  def _should_include_resource_in_view(self, constraints, resource) -> bool:
    for expr in constraints:
      result = expr.evaluate(resource)
      if not result.as_bool():
        return False
    return True

  def _flatten_resource(
      self,
      resource: message.Message,
      select_expr: Mapping[
          str, python_compiled_expressions.PythonCompiledExpression
      ],
  ) -> Dict[str, Any]:
    """Returns a dictionary representing a resource.

    Each key matches a column name from the view config select provided by the
    user. The corresponding value is the value found in the resource or a list
    of matching values in the resource.

    Args:
      resource: a singular resource from the bundle returned from the FHIR
        server.
      select_expr: a dictionary representing the column name and compiled fhir
        path for each select expression.
    """
    flat_resource = {}
    for col_name, expr in select_expr.items():
      # TODO(b/271913842): Handle failures from the evaluation.
      messages = expr.evaluate(resource).messages

      # TODO(b/302105011): Handle results with more than one value, no value,
      # or other types within the interpreter.
      if len(messages) > 1:
        flat_resource[col_name] = []
        for msg in messages:
          # TODO(b/208900793): Check primitive type rather than assuming value.
          flat_resource[col_name].append(
              proto_utils.get_value_at_field(msg, 'value')
          )
      elif len(messages) == 1:
        # TODO(b/208900793): Check primitive type rather than assuming value.
        flat_resource[col_name] = proto_utils.get_value_at_field(
            messages[0], 'value'
        )
      else:
        flat_resource[col_name] = None

    return flat_resource

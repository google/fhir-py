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
"""Library for running FHIR view definitions against Spark.

This module allows users to run FHIR Views against Spark. Users may retrieve
results through the Spark library used here, or create Spark views that
can be consumed by other tools.
"""

import re
from typing import Dict, Optional

from sqlalchemy import engine

from google.fhir.core.fhir_path import _spark_interpreter
from google.fhir.views import runner_utils
from google.fhir.views import views


class SparkRunner:
  """FHIR Views runner used to perform queries against Spark."""

  def __init__(
      self,
      query_engine: engine.Engine,
      fhir_dataset: str,
      view_dataset: Optional[str] = None,
      value_set_codes_table: Optional[str] = None,
      snake_case_resource_tables: bool = False,
  ) -> None:
    """Initializes the SparkRunner with provided SQLAlcehmy Engine and Dataset.

    Args:
      query_engine: SQLAlchemy Engine with which to perform queries.
      fhir_dataset: Dataset with FHIR data that the views will query.
      view_dataset: Optional dataset with views will be created via the
        `to_spark_view` method, if used. It will use the fhir_dataset if no
        view_dataset is specified
      value_set_codes_table: A table containing value set expansions. If
        provided, memberOf queries may be made against value set URLs described
        by this table. If `value_set_codes_table` is a string, it must included
        a project ID if not in the client's default project, dataset ID and
        table ID, each separated by a '.'. The table must match the schema
        described by
        https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support
      snake_case_resource_tables: Whether to use snake_case names for resource
        tables in Spark for compatiblity with some exports. Defaults to
        False.
    """
    self._engine = query_engine
    self._fhir_dataset = fhir_dataset
    self._view_dataset = (view_dataset
                          if view_dataset is not None else self._fhir_dataset)
    self._value_set_codes_table = (
        value_set_codes_table
        if value_set_codes_table is not None
        else 'value_set_codes'
    )
    self._snake_case_resource_tables = snake_case_resource_tables

  def to_sql(
      self,
      view: views.View,
      limit: Optional[int] = None,
      include_patient_id_col: bool = True,
  ) -> str:
    """Returns the SQL used to run the given view in Spark.

    Args:
      view: the view used to generate the SQL.
      limit: optional limit to attach to the generated SQL.
      include_patient_id_col: whether to include a __patientId__ column to
        indicate the patient the resource is associated with.

    Returns:
      The SQL used to run the given view.
    """
    encoder = _spark_interpreter.SparkSqlInterpreter()

    dataset = f'{self._fhir_dataset}'
    table_names = self._view_table_names(view)
    sql_generator = runner_utils.RunnerSqlGenerator(
        view, encoder, dataset, table_names
    )

    sql_statement = sql_generator.build_sql_statement(include_patient_id_col)

    valuesets_clause = sql_generator.build_valueset_expression(
        self._value_set_codes_table
    )

    if limit is not None and limit < 1:
      raise ValueError('Query limits must be positive integers.')
    limit_clause = '' if limit is None else f' LIMIT {limit}'

    return f'{valuesets_clause}{sql_statement}{limit_clause}'

  def _view_table_names(self, view: views.View) -> Dict[str, str]:
    """Generates the table names for each resource in the view."""
    names = {}
    for structdef_url in view.get_structdef_urls():
      last_slash_index = structdef_url.rfind('/')
      name = (
          structdef_url
          if last_slash_index == -1
          else structdef_url[last_slash_index + 1 :]
      )
      if self._snake_case_resource_tables:
        name = (
            re.sub(pattern=r'([A-Z]+)', repl=r'_\1', string=name)
            .lower()
            .lstrip('_')
        )
      names[structdef_url] = name
    return names

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
from typing import Dict, Optional, cast

import pandas
from sqlalchemy import engine

from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _spark_interpreter
from google.fhir.core.fhir_path import expressions
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
    encoder = _spark_interpreter.SparkSqlInterpreter(
        value_set_codes_table='VALUESET_VIEW',
    )

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

  def to_dataframe(
      self, view: views.View, limit: Optional[int] = None
  ) -> pandas.DataFrame:
    """Returns a Pandas dataframe of the results.

    Args:
      view: the view that defines the query to run.
      limit: optional limit of the number of items to return.

    Returns:
      pandas.DataFrame: dataframe of the view contents.

    Raises:
      ValueError propagated from the Spark client if pandas is not installed.
    """
    df = pandas.read_sql_query(
        sql=self.to_sql(view, limit=limit, include_patient_id_col=False),
        con=self._engine,
    )
    return runner_utils.clean_dataframe(df, view.get_select_expressions())

  def summarize_codes(
      self, view: views.View, code_expr: expressions.Builder
  ) -> pandas.DataFrame:
    """Returns a summary count of distinct code values for the given expression.

    This method is primarily intended for exploratory data analysis, so users
    new to a dataset can quickly see the most common code values in the system.

    Here is an example usage:

    >>> obs = views.view_of('Observation')
    >>> obs_codes_count_df = runner.summarize_codes(obs, obs.code)
    >>> obs_category_count_df = runner.summarize_codes(obs, obs.category)

    It also works for nested fields, like:

    >>> pat = views.view_of('Patient')
    >>> rel_count_df = runner.summarize_codes(pat, pat.contact.relationship)

    Args:
      view: the view containing code values to summarize.
      code_expr: a FHIRPath expression referencing a codeable concept, coding,
        or code field to count.

    Returns:
      A Pandas dataframe containing 'system', 'code', 'display', and 'count'
      columns for codeable concept and coding fields. 'system' and 'display'
      columns are omitted when summarzing raw code fields, since they do not
      have system or display values.

      The datframe is ordered by count is in descending order.
    """
    expr_array_query = runner_utils.RunnerSqlGenerator(
        view=view,
        encoder=_spark_interpreter.SparkSqlInterpreter(),
        dataset=f'{self._fhir_dataset}',
        table_names=self._view_table_names(view),
    ).build_select_for_summarize_code(code_expr)

    node_type = code_expr.get_node().return_type()
    if node_type and isinstance(node_type, _fhir_path_data_types.Collection):
      node_type = list(cast(_fhir_path_data_types.Collection, node_type).types)[
          0
      ]

    # Create a counting aggregation for the appropriate code-like structure.
    if node_type.url == runner_utils.CODEABLE_CONCEPT:
      count_query = (
          f'WITH c AS ({expr_array_query}) '
          'SELECT codings.system, codings.code, '
          'codings.display, COUNT(*) count '
          'FROM c '
          'LATERAL VIEW EXPLODE(c.target) AS concepts '
          'LATERAL VIEW EXPLODE(concepts.coding) AS codings '
          'GROUP BY 1, 2, 3 ORDER BY count DESC'
      )
    elif node_type.url == runner_utils.CODING:
      count_query = (
          f'WITH c AS ({expr_array_query}) '
          'SELECT codings.system, codings.code, '
          'codings.display, COUNT(*) count '
          'FROM c '
          'LATERAL VIEW EXPLODE(c.target) AS codings '
          'GROUP BY 1, 2, 3 ORDER BY count DESC'
      )
    elif (
        node_type.url == runner_utils.CODE
        or node_type.url == runner_utils.STRING
    ):
      # Assume simple strings are just code values. Since code is a type of
      # string, the current expression typing analysis may produce a string
      # type here so we accept both string and code.
      count_query = (
          f'WITH c AS ({expr_array_query}) '
          'SELECT code, COUNT(*) count '
          'FROM c LATERAL VIEW EXPLODE(c.target) as code '
          'GROUP BY 1 ORDER BY count DESC'
      )
    else:
      raise ValueError(
          'Field must be a FHIR CodeableConcept, Coding, or Code; '
          f'got {node_type.url}.'
      )

    return pandas.read_sql_query(sql=count_query, con=self._engine)

  def create_database_view(self, view: views.View, view_name: str) -> None:
    """Creates a Spark view with the given name in the runner's view_dataset.

    Args:
      view: the FHIR view that creates
      view_name: the view name passed to the CREATE OR REPLACE VIEW statement.
    """
    view_sql = (
        f'CREATE OR REPLACE VIEW {self._view_dataset}.{view_name} AS\n'
        f'{self.to_sql(view, include_patient_id_col=False)}'
    )
    self._engine.execute(view_sql).fetchall()

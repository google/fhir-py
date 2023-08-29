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
"""Library for running FHIR view definitions against BigQuery.

This module allows users to run FHIR Views against BigQuery. Users may retrieve
results through the BigQuery library used here, or create BigQuery views that
can be consumed by other tools.
"""

import re
from typing import Dict, Iterable, Optional, Union, cast

from google.cloud import bigquery
import pandas

from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.fhir_path import _bigquery_interpreter
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import fhir_path
from google.fhir.r4.terminology import terminology_service_client
from google.fhir.r4.terminology import value_sets
from google.fhir.views import bigquery_value_set_manager
from google.fhir.views import column_expression_builder
from google.fhir.views import runner_utils
from google.fhir.views import views


class BigQueryRunner:
  """FHIR Views runner used to perform queries against BigQuery."""

  @classmethod
  def _to_dataset_ref(
      cls,
      client: bigquery.client.Client,
      dataset: Union[str, bigquery.dataset.DatasetReference],
  ) -> bigquery.dataset.DatasetReference:
    """Converts the dataset to a DatasetReference object, if necessary."""
    if isinstance(dataset, bigquery.dataset.DatasetReference):
      return dataset
    return bigquery.dataset.DatasetReference.from_string(
        dataset, client.project
    )

  def __init__(
      self,
      client: bigquery.client.Client,
      fhir_dataset: Union[str, bigquery.dataset.DatasetReference],
      view_dataset: Optional[
          Union[str, bigquery.dataset.DatasetReference]
      ] = None,
      as_of: Optional[str] = None,
      value_set_codes_table: Optional[
          Union[bigquery.table.Table, bigquery.table.TableReference, str]
      ] = None,
      snake_case_resource_tables: bool = False,
      internal_default_to_v2_runner: bool = False,
  ) -> None:
    """Initializer.

    Initializes the BigQueryRunner with user provided BigQuery Client, Dataset,
    and an optional TIMESTAMP string from within the past 7 days. If a timestamp
    is provided, queries are made against a snapshot of the BigQuery data from
    that point in time.

    Args:
      client: BigQuery Client with which to perform queries.
      fhir_dataset: BigQuery dataset with FHIR data that the views will query.
      view_dataset: Optional BigQuery dataset with views will be created via the
        `to_bigquery_view` method, if used. It will use the fhir_dataset if no
        view_dataset is specified
      as_of: If provided, a timestamp string that specifies a snapshot defined
        by an absolute point in time against which to perform queries. BigQuery
        maintains a 7-day history so this time needs to be within 7 days of the
        current timestamp.
      value_set_codes_table: A table containing value set expansions. If
        provided, memberOf queries may be made against value set URLs described
        by this table. If `value_set_codes_table` is a string, it must included
        a project ID if not in the client's default project, dataset ID and
        table ID, each separated by a '.'. The table must match the schema
        described by
        https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support
      snake_case_resource_tables: Whether to use snake_case names for resource
        tables in BigQuery for compatiblity with some exports. Defaults to
        False.
      internal_default_to_v2_runner: Internal only. Whether to use the
        refactored SQL generation logic by default. This will be removed prior
        to the 1.0 release.
    """
    super().__init__()
    self._client = client
    self._fhir_dataset = self._to_dataset_ref(client, fhir_dataset)
    self._view_dataset = (
        self._to_dataset_ref(client, view_dataset)
        if view_dataset is not None
        else self._fhir_dataset
    )
    self._as_of = as_of
    self._snake_case_resource_tables = snake_case_resource_tables
    self._internal_default_to_v2_runner = internal_default_to_v2_runner

    if value_set_codes_table is None:
      self._value_set_codes_table = bigquery.table.TableReference(
          self._view_dataset, 'value_set_codes'
      )
    elif isinstance(value_set_codes_table, str):
      self._value_set_codes_table = bigquery.table.TableReference.from_string(
          value_set_codes_table, default_project=client.project
      )
    else:
      self._value_set_codes_table = value_set_codes_table

    self._value_set_manager = (
        bigquery_value_set_manager.BigQueryValueSetManager(
            client, self._value_set_codes_table
        )
    )

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

  def to_sql(
      self,
      view: views.View,
      limit: Optional[int] = None,
      include_patient_id_col: bool = True,
      internal_v2: Optional[bool] = None,
  ) -> str:
    """Returns the SQL used to run the given view in BigQuery.

    Args:
      view: the view used to generate the SQL.
      limit: optional limit to attach to the generated SQL.
      include_patient_id_col: whether to include a __patientId__ column to
        indicate the patient the resource is associated with.
      internal_v2: For incremental development use only and will be removed
        prior to a 1.0 release.

    Returns:
      The SQL used to run the given view.
    """
    if internal_v2 is None:
      internal_v2 = self._internal_default_to_v2_runner

    if len(view.get_structdef_urls()) > 1 and not internal_v2:
      raise ValueError(
          'Cross Resource views are only allowed in '
          f'v2. {view.get_structdef_urls()}'
      )

    sql_generator = self._build_sql_generator(internal_v2, view)
    sql_statement = sql_generator.build_sql_statement(include_patient_id_col)

    view_table_name = (
        f'{self._value_set_codes_table.project}'
        f'.{self._value_set_codes_table.dataset_id}'
        f'.{self._value_set_codes_table.table_id}'
    )
    # Build the expression containing valueset content, which may be empty.
    valuesets_clause = sql_generator.build_valueset_expression(view_table_name)

    if limit is not None and limit < 1:
      raise ValueError('Query limits must be positive integers.')
    limit_clause = '' if limit is None else f' LIMIT {limit}'

    return f'{valuesets_clause}{sql_statement}{limit_clause}'

  def to_dataframe(
      self, view: views.View, limit: Optional[int] = None
  ) -> pandas.DataFrame:
    """Returns a Pandas dataframe of the results, if Pandas is installed.

    Args:
      view: the view that defines the query to run.
      limit: optional limit of the number of items to return.

    Returns:
      pandas.DataFrame: dataframe of the view contents.

    Raises:
      ValueError propagated from the BigQuery client if pandas is not installed.
    """
    df = self.run_query(view, limit).result().to_dataframe()
    return runner_utils.clean_dataframe(df, view.get_select_expressions())

  def create_database_view(self, view: views.View, view_name: str) -> None:
    """Creates a BigQuery view with the given name in the runner's view_dataset.

    Args:
      view: the FHIR view that creates
      view_name: the view name passed to the CREATE OR REPLACE VIEW statement.

    Raises:
      google.cloud.exceptions.GoogleAPICallError if the job failed.
    """
    dataset = f'{self._view_dataset.project}.{self._view_dataset.dataset_id}'
    view_sql = (
        f'CREATE OR REPLACE VIEW `{dataset}.{view_name}` AS\n'
        f'{self.to_sql(view, include_patient_id_col=False)}'
    )
    self._client.query(view_sql).result()

  def run_query(
      self, view: views.View, limit: Optional[int] = None
  ) -> bigquery.QueryJob:
    """Runs query for the view and returns the corresponding BigQuery job.

    Args:
      view: the view that defines the query to run.
      limit: optional limit of the number of items to return.

    Returns:
      bigquery.QueryJob: the job for the running query.
    """
    return self._client.query(
        self.to_sql(view, limit=limit, include_patient_id_col=False)
    )

  def summarize_codes(
      self,
      view: views.View,
      code_expr: column_expression_builder.ColumnExpressionBuilder,
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
      code_expr: a ColumnExpressionBuilder referencing a codeable concept,
        coding, or code field to count.

    Returns:
      A Pandas dataframe containing 'system', 'code', 'display', and 'count'
      columns for codeable concept and coding fields. 'system' and 'display'
      columns are omitted when summarzing raw code fields, since they do not
      have system or display values.

      The datframe is ordered by count is in descending order.
    """
    expr_array_query = self._build_sql_generator(
        internal_v2=False, view=view
    ).build_select_for_summarize_code(code_expr)

    node_type = code_expr.node.return_type
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
          'FROM c, '
          'UNNEST(c.target) concepts, UNNEST(concepts.coding) as codings '
          'GROUP BY 1, 2, 3 ORDER BY count DESC'
      )
    elif node_type.url == runner_utils.CODING:
      count_query = (
          f'WITH c AS ({expr_array_query}) '
          'SELECT codings.system, codings.code, '
          'codings.display, COUNT(*) count '
          'FROM c, '
          'UNNEST(c.target) codings '
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
          'FROM c, UNNEST(c.target) as code '
          'GROUP BY 1 ORDER BY count DESC'
      )
    else:
      raise ValueError(
          'Field must be a FHIR CodeableConcept, Coding, or Code; '
          f'got {node_type.url}.'
      )

    return self._client.query(count_query).result().to_dataframe()

  def _build_sql_generator(self, internal_v2: bool, view: views.View):
    """Build a RunnerSqlGenerator depending on the runner version."""
    fhir_context = view.get_fhir_path_context()
    url = list(view.get_structdef_urls())[0]
    struct_def = fhir_context.get_structure_definition(url)
    deps = fhir_context.get_dependency_definitions(url)
    deps.append(struct_def)
    encoder = fhir_path.FhirPathStandardSqlEncoder(
        deps,
        options=fhir_path.SqlGenerationOptions(
            value_set_codes_table='VALUESET_VIEW'
        ),
    )
    if internal_v2:
      encoder = _bigquery_interpreter.BigQuerySqlInterpreter(
          value_set_codes_table='VALUESET_VIEW',
      )

    # URLs to various expressions and tables:
    dataset = f'{self._fhir_dataset.project}.{self._fhir_dataset.dataset_id}'
    table_names = self._view_table_names(view)
    return runner_utils.RunnerSqlGenerator(view, encoder, dataset, table_names)

  # TODO(b/201107372): Update FHIR-agnostic types to a protocol.
  def materialize_value_sets(
      self,
      value_set_protos: Iterable[value_set_pb2.ValueSet],
      batch_size: int = 500,
  ) -> None:
    """Materialize the given value sets into the value_set_codes_table.

    Then writes these expanded codes into the database
    named after the `value_set_codes_table` provided at class initialization.
    Builds a valueset_codes table as described by
    https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support

    The table will be created if it does not already exist.

    The function will avoid inserting duplicate rows if some of the codes are
    already present in the given table. It will not attempt to perform an
    'upsert' or modify any existing rows.

    Note that value sets provided to this function should already be expanded,
    in that they contain the code values to write. Users should also see
    `materialize_value_set_expansion` below to retrieve an expanded set from
    a terminology server.

    Args:
      value_set_protos: An iterable of FHIR ValueSet protos.
      batch_size: The maximum number of rows to insert in a single query.
    """
    self._value_set_manager.materialize_value_sets(value_set_protos, batch_size)

  def materialize_value_set_expansion(
      self,
      urls: Iterable[str],
      expander: Union[
          terminology_service_client.TerminologyServiceClient,
          value_sets.ValueSetResolver,
      ],
      terminology_service_url: Optional[str] = None,
      batch_size: int = 500,
  ) -> None:
    """Expands a sequence of value set and materializes their expanded codes.

    Expands the given value set URLs to obtain the set of codes they describe.
    Then writes these expanded codes into the database
    named after the `value_set_codes_table` provided at class initialization.
    Builds a valueset_codes table as described by
    https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support

    The table will be created if it does not already exist.

    The function will avoid inserting duplicate rows if some of the codes are
    already present in the given table. It will not attempt to perform an
    'upsert' or modify any existing rows.

    Provided as a utility function for user convenience. If `urls` is a large
    set of URLs, callers may prefer to use multi-processing and/or
    multi-threading to perform expansion and table insertion of the URLs
    concurrently. This function performs all expansions and table insertions
    serially.

    Args:
      urls: The urls for value sets to expand and materialize.
      expander: The ValueSetResolver or TerminologyServiceClient to perform
        value set expansion. A ValueSetResolver may be used to attempt to avoid
        some network requests by expanding value sets locally. A
        TerminologyServiceClient will use external terminology services to
        perform all value set expansions.
      terminology_service_url: If `expander` is a TerminologyServiceClient, the
        URL of the terminology service to use when expanding value set URLs. If
        not given, the client will attempt to infer the correct terminology
        service to use for each value set URL based on its domain.
      batch_size: The maximum number of rows to insert in a single query.

    Raises:
      TypeError: If a `terminology_service_url` is given but `expander` is not a
      TerminologyServiceClient.
    """
    self._value_set_manager.materialize_value_set_expansion(
        urls, expander, terminology_service_url, batch_size
    )

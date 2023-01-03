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

import itertools
import re
from typing import Collection, Iterable, Optional, Union, cast, Dict, List

from google.cloud import bigquery
import numpy
import pandas
import sqlalchemy
import sqlalchemy_bigquery

from google.protobuf import message
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.fhir_path import _bigquery_interpreter
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import expressions
from google.fhir.core.fhir_path import fhir_path
from google.fhir.r4.terminology import terminology_service_client
from google.fhir.r4.terminology import value_set_tables
from google.fhir.r4.terminology import value_sets
from google.fhir.views import views

_CODEABLE_CONCEPT = 'http://hl7.org/fhir/StructureDefinition/CodeableConcept'
_CODING = 'http://hl7.org/fhir/StructureDefinition/Coding'
_CODE = 'http://hl7.org/fhir/StructureDefinition/Code'
_STRING = 'http://hl7.org/fhirpath/System.String'

# Timestamp format to convert ISO strings into BigQuery Timestamp types.
_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%E*S%Ez'

# ISO format of dates used by FHIR.
_DATE_FORMAT = '%Y-%m-%d'


class BigQueryRunner:
  """FHIR Views runner used to perform queries against BigQuery."""

  @classmethod
  def _to_dataset_ref(
      cls, client: bigquery.client.Client,
      dataset: Union[str, bigquery.dataset.DatasetReference]
  ) -> bigquery.dataset.DatasetReference:
    """Converts the dataset to a DatasetReference object, if necessary."""
    if isinstance(dataset, bigquery.dataset.DatasetReference):
      return dataset
    return bigquery.dataset.DatasetReference.from_string(
        dataset, client.project)

  def __init__(
      self,
      client: bigquery.client.Client,
      fhir_dataset: Union[str, bigquery.dataset.DatasetReference],
      view_dataset: Optional[Union[str,
                                   bigquery.dataset.DatasetReference]] = None,
      as_of: Optional[str] = None,
      value_set_codes_table: Optional[Union[bigquery.table.Table,
                                            bigquery.table.TableReference,
                                            str]] = None,
      snake_case_resource_tables: bool = False,
      internal_default_to_v2_runner: bool = False) -> None:
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
        if view_dataset is not None else self._fhir_dataset)
    self._as_of = as_of
    self._snake_case_resource_tables = snake_case_resource_tables
    self._internal_default_to_v2_runner = internal_default_to_v2_runner

    if value_set_codes_table is None:
      self._value_set_codes_table = bigquery.table.TableReference(
          self._view_dataset, 'value_set_codes')
    elif isinstance(value_set_codes_table, str):
      self._value_set_codes_table = bigquery.table.TableReference.from_string(
          value_set_codes_table, default_project=client.project)
    else:
      self._value_set_codes_table = value_set_codes_table

  def _create_valueset_expression(self, view: views.View) -> str:
    """Returns the expression for valuesets, if needed."""
    fhir_context = view.get_fhir_path_context()
    memberof_nodes = _memberof_nodes_from_view(view)
    value_set_rows = []
    include_value_set_codes_table = False
    for node in memberof_nodes:
      value_set_codes = node.to_value_set_codes(fhir_context)
      if value_set_codes is None:
        # If codes for the value set referenced in the memberof call can not be
        # found, fall back to the value_set_codes table.
        include_value_set_codes_table = True
        continue

      if value_set_codes.version:
        version_sql = f'"{value_set_codes.version}"'
      else:
        version_sql = 'NULL'
      # Sort the code values for more readable and consistent queries.
      for code_value in sorted(value_set_codes.codes):
        row = (f'SELECT "{value_set_codes.url}" as valueseturi, '
               f'{version_sql} as valuesetversion, '
               f'"{code_value.system}" as system, '
               f'"{code_value.value}" as code')
        value_set_rows.append(row)

    # Include the entire value_set_codes_table_name in addition to any other
    # custom value set definitions provided by callers.
    if include_value_set_codes_table:
      table_name = (f'{self._value_set_codes_table.project}'
                    f'.{self._value_set_codes_table.dataset_id}'
                    f'.{self._value_set_codes_table.table_id}')
      value_set_rows.append('SELECT valueseturi, valuesetversion, system, code '
                            f'FROM {table_name}')
    if value_set_rows:
      rows_expression = '\nUNION ALL '.join(value_set_rows)
      return f'WITH VALUESET_VIEW AS ({rows_expression})\n'

    return ''

  def _view_table_names(self, view: views.View) -> Dict[str, str]:
    """Generates the table names for each resource in the view."""
    names = {}
    for structdef_url in view.get_structdef_urls():
      last_slash_index = structdef_url.rfind('/')
      name = (
          structdef_url
          if last_slash_index == -1 else structdef_url[last_slash_index + 1:])
      if self._snake_case_resource_tables:
        name = re.sub(
            pattern=r'([A-Z]+)', repl=r'_\1', string=name).lower().lstrip('_')
      names[structdef_url] = name
    return names

  def _datetime_sql(self, expr: expressions.Builder, raw_sql: str) -> str:
    """Wraps raw sql if the result is datetime."""
    # Dates and datetime types are stored as strings to preseve completeness
    # of the underlying data, but views converts to date and datetime types
    # for ease of use.

    node_type = expr.get_node().return_type()

    # Use date format constants drawn from the FHIR Store export conventions
    # for simplicity. If users encounter different formats in practice, we
    # could allow these formats to be overridden when constructing the runner
    # or check the string format explicitily on each row.
    if node_type == _fhir_path_data_types.DateTime:
      raw_sql = f'PARSE_TIMESTAMP("{_TIMESTAMP_FORMAT}", {raw_sql})'
    elif node_type == _fhir_path_data_types.Date:
      raw_sql = f'PARSE_DATE("{_DATE_FORMAT}", {raw_sql})'

    return raw_sql

  def _join_sql_statements(self, sql_statements: List[str]) -> str:
    if len(sql_statements) > 2:
      raise NotImplementedError(
          'Cross resource join for more than two resources '
          'is not currently supported.')

    if len(sql_statements) == 1:
      return sql_statements[0]

    return (f'SELECT * , __patientId__ FROM\n(({sql_statements[0]})'
            f'\nINNER JOIN\n({self._join_sql_statements(sql_statements[1:])})'
            f'\nUSING(__patientId__))')

  def _encode(self,
              expr: expressions.Builder,
              select_scalars_as_array: bool,
              structure_definition: message.Message,
              element_definition: message.Message,
              internal_v2: bool,
              encoder: fhir_path.FhirPathStandardSqlEncoder,
              use_resource_alias: bool = False) -> str:
    """Encodes the expression in googleSQL."""
    if internal_v2:
      interpreter = _bigquery_interpreter.BigQuerySqlInterpreter(
          use_resource_alias=use_resource_alias)
      return interpreter.encode(
          expr, select_scalars_as_array=select_scalars_as_array)
    else:
      return encoder.encode(
          structure_definition=structure_definition,
          element_definition=element_definition,
          fhir_path_expression=expr.to_expression().fhir_path,
          select_scalars_as_array=select_scalars_as_array)

  def to_sql(self,
             view: views.View,
             limit: Optional[int] = None,
             include_patient_id_col: bool = True,
             internal_v2: Optional[bool] = None) -> str:
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
      raise ValueError(f'Cross Resource views are only allowed in '
                       f'v2. {view.get_structdef_urls()}')

    fhir_context = view.get_fhir_path_context()
    url = list(view.get_structdef_urls())[0]
    struct_def = fhir_context.get_structure_definition(url)
    elem_def = next(elem for elem in struct_def.snapshot.element
                    if elem.path.value == struct_def.name.value)

    deps = fhir_context.get_dependency_definitions(url)
    deps.append(struct_def)
    encoder = fhir_path.FhirPathStandardSqlEncoder(deps)

    inner_sql_statements = []

    # URLs to various expressions and tables:
    dataset = f'{self._fhir_dataset.project}.{self._fhir_dataset.dataset_id}'
    table_names = self._view_table_names(view)
    url_to_field_names = view.get_url_to_field_names()
    url_to_constraint_indexes = view.get_url_to_constraint_indexes()
    all_select_expressions = view.get_select_expressions()
    all_constraints = view.get_constraint_expressions()

    # Sort to always generate the urls in the same order for testing purposes.
    sorted_urls = sorted(view.get_structdef_urls())

    # Build a separate SQL query for each url first and then INNER JOIN on
    # patient id at the very end. This way, any where clause filters occur first
    # on the respective query.
    for url in sorted_urls:
      inner_select_expressions = []
      if url in url_to_field_names:
        for field_name in url_to_field_names[url]:
          # If no fields have been specified, then return all fields on the
          # resource table.
          if field_name == views.BASE_BUILDER_KEY:
            # There shouldn't be any more field names.
            inner_select_expressions = ['*']
            break

          expr = all_select_expressions[field_name]
          select_expression = self._encode(
              expr,
              select_scalars_as_array=False,
              structure_definition=struct_def,
              element_definition=elem_def,
              encoder=encoder,
              internal_v2=internal_v2)
          if not internal_v2:
            select_expression = self._datetime_sql(expr, select_expression)
          inner_select_expressions.append(
              f'{select_expression} AS {field_name}')

      if include_patient_id_col or len(view.get_structdef_urls()) > 1:
        # Auto generate the __patientId__ field for the view if it exists for
        # every unique resource.
        expr = view.get_patient_id_expression(url)
        if expr:
          expression = self._encode(
              expr,
              select_scalars_as_array=False,
              structure_definition=struct_def,
              element_definition=elem_def,
              encoder=encoder,
              internal_v2=internal_v2)

          inner_select_expressions.append(f'{expression} AS __patientId__')

      inner_where_expressions = []
      if url in url_to_constraint_indexes:
        for index in url_to_constraint_indexes[url]:
          expr = all_constraints[index]
          where_expression = self._encode(
              expr,
              select_scalars_as_array=True,
              structure_definition=struct_def,
              element_definition=elem_def,
              encoder=encoder,
              internal_v2=internal_v2)

          # TODO(b/208900793): Remove LOGICAL_AND(UNNEST) when the SQL generator
          # can return single values and it's safe to do so for non-repeated
          # fields.
          inner_where_expressions.append(
              '(SELECT LOGICAL_AND(logic_)\n'
              f'FROM UNNEST({where_expression}) AS logic_)')

      # Build statement for the resource.
      table_alias = ''

      # Check if the view has builders that reference multiple resources.
      has_mixed_resource_builders = (
          len(view.get_multiresource_field_names()) +
          len(view.get_multiresource_constraint_indexes())) != 0

      if has_mixed_resource_builders:
        # For views with mixed resource builders, * and table_names[url] will
        # double the number of rows produced.
        if '*' in inner_select_expressions:
          inner_select_expressions = inner_select_expressions[1:]
        inner_select_expressions.append(table_names[url])
        table_alias = f' {table_names[url]}'

      inner_select_clause = (
          f'SELECT {",".join(inner_select_expressions)} '
          f'FROM `{dataset}`.{table_names[url]}{table_alias}')
      inner_where_clause = ''
      if inner_where_expressions:
        inner_where_clause = f'\nWHERE {" AND ".join(inner_where_expressions)}'
      inner_sql_statements.append(f'{inner_select_clause}{inner_where_clause}')

    # First perform all the joins.
    sql_statement = self._join_sql_statements(inner_sql_statements)

    # Then build the query for the builders that reference multiple resources
    # now that the resources are all in one table.
    multiresource_select_expressions = []
    for field_name in view.get_multiresource_field_names():
      expr = all_select_expressions[field_name]
      select_expression = self._encode(
          expr,
          select_scalars_as_array=False,
          structure_definition=struct_def,
          element_definition=elem_def,
          encoder=encoder,
          internal_v2=internal_v2,
          use_resource_alias=True)
      multiresource_select_expressions.append(
          f'{select_expression} AS {field_name}')

    multiresource_where_expressions = []
    for index in view.get_multiresource_constraint_indexes():
      expr = all_constraints[index]
      where_expression = self._encode(
          expr,
          select_scalars_as_array=True,
          structure_definition=struct_def,
          element_definition=elem_def,
          encoder=encoder,
          internal_v2=internal_v2,
          use_resource_alias=True)

      # TODO(b/208900793): Remove LOGICAL_AND(UNNEST) when the SQL generator
      # can return single values and it's safe to do so for non-repeated
      # fields.
      multiresource_where_expressions.append(
          '(SELECT LOGICAL_AND(logic_)\n'
          f'FROM UNNEST({where_expression}) AS logic_)')

    if multiresource_select_expressions:
      sql_statement = ('SELECT *, '
                       f'{",".join(multiresource_select_expressions)} '
                       f'FROM ({sql_statement})')
    if multiresource_where_expressions:
      sql_statement = (f'{sql_statement}'
                       '\nWHERE '
                       f'{" AND ".join(multiresource_where_expressions)}')

    # Build the expression containing valueset content, which may be empty.
    valuesets_clause = self._create_valueset_expression(view)

    if limit is not None and limit < 1:
      raise ValueError('Query limits must be positive integers.')
    limit_clause = '' if limit is None else f' LIMIT {limit}'

    return f'{valuesets_clause}{sql_statement}{limit_clause}'

  def to_dataframe(self,
                   view: views.View,
                   limit: Optional[int] = None) -> pandas.DataFrame:
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

    # If the view has expressions, we can narrow the non-scalar column list by
    # checking only for list or struct columns.
    select_columns = set(view.get_select_expressions().keys())
    # Ignore the __base__ expression that exists by default.
    select_columns.discard(views.BASE_BUILDER_KEY)

    if select_columns:
      non_scalar_cols = [
          col for (col, expr) in view.get_select_expressions().items()
          if expr.return_type.returns_collection() or expr.return_type.fields()
      ]
    else:
      # No fields were specified, so we must check any 'object' field
      # in the dataframe.
      non_scalar_cols = df.select_dtypes(include=['object']).columns.tolist()

    # Helper function to recursively trim `None` values and empty arrays.
    def trim_structs(item):
      if isinstance(item, numpy.ndarray):
        if not item.any():
          return None
        else:
          return [trim_structs(child) for child in item]

      if isinstance(item, dict):
        result = {}
        for (key, value) in item.items():
          trimmed_value = trim_structs(value)
          if trimmed_value is not None:
            result[key] = trimmed_value
        return result

      return item

    for col in non_scalar_cols:
      df[col] = df[col].map(trim_structs)

    return df

  def create_bigquery_view(self, view: views.View, view_name: str) -> None:
    """Creates a BigQuery view with the given name in the runner's view_dataset.

    Args:
      view: the FHIR view that creates
      view_name: the view name passed to the CREATE OR REPLACE VIEW statement.

    Raises:
      google.cloud.exceptions.GoogleAPICallError if the job failed.
    """
    dataset = f'{self._view_dataset.project}.{self._view_dataset.dataset_id}'
    view_sql = (f'CREATE OR REPLACE VIEW `{dataset}.{view_name}` AS\n'
                f'{self.to_sql(view, include_patient_id_col=False)}')
    self._client.query(view_sql).result()

  def run_query(self,
                view: views.View,
                limit: Optional[int] = None) -> bigquery.QueryJob:
    """Runs query for the view and returns the corresponding BigQuery job.

    Args:
      view: the view that defines the query to run.
      limit: optional limit of the number of items to return.

    Returns:
      bigquery.QueryJob: the job for the running query.
    """
    return self._client.query(
        self.to_sql(view, limit=limit, include_patient_id_col=False))

  def summarize_codes(self, view: views.View,
                      code_expr: expressions.Builder) -> pandas.DataFrame:
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
    node_type = code_expr.get_node().return_type()
    if node_type and isinstance(node_type, _fhir_path_data_types.Collection):
      node_type = list(cast(_fhir_path_data_types.Collection,
                            node_type).types)[0]

    # TODO(b/239733067): Add constraint filtering to code summarization.
    if view.get_constraint_expressions():
      raise NotImplementedError(
          'Summarization of codes with view constraints not yet implemented.')

    fhir_context = view.get_fhir_path_context()
    # Workaround for v1 until it gets deprecated.
    if len(view.get_structdef_urls()) > 1:
      raise NotImplementedError(
          'Summarization of codes with multiple resource views not yet implemented.'
      )

    url = list(view.get_structdef_urls())[0]
    struct_def = fhir_context.get_structure_definition(url)
    elem_def = next(elem for elem in struct_def.snapshot.element
                    if elem.path.value == struct_def.name.value)

    deps = fhir_context.get_dependency_definitions(url)
    deps.append(struct_def)
    encoder = fhir_path.FhirPathStandardSqlEncoder(deps)

    select_expression = encoder.encode(
        structure_definition=struct_def,
        element_definition=elem_def,
        fhir_path_expression=code_expr.to_expression().fhir_path,
        select_scalars_as_array=True)

    # Build the select expression from the FHIR resource table.
    table_names = self._view_table_names(view)
    dataset = f'{self._fhir_dataset.project}.{self._fhir_dataset.dataset_id}'

    if len(table_names.keys()) == 1:
      # Query to get the array of code-like fields we will aggregate by.
      expr_array_query = (f'SELECT {select_expression} as target '
                          f'FROM `{dataset}`.{table_names[url]}')

    # Create a counting aggregation for the appropriate code-like structure.
    if node_type.url == _CODEABLE_CONCEPT:
      count_query = (
          f'WITH c AS ({expr_array_query}) '
          f'SELECT codings.system, codings.code, '
          f'codings.display, COUNT(*) count '
          f'FROM c, '
          f'UNNEST(c.target) concepts, UNNEST(concepts.coding) as codings '
          f'GROUP BY 1, 2, 3 ORDER BY count DESC')
    elif node_type.url == _CODING:
      count_query = (f'WITH c AS ({expr_array_query}) '
                     f'SELECT codings.system, codings.code, '
                     f'codings.display, COUNT(*) count '
                     f'FROM c, '
                     f'UNNEST(c.target) codings '
                     f'GROUP BY 1, 2, 3 ORDER BY count DESC')
    elif node_type.url == _CODE or node_type.url == _STRING:
      # Assume simple strings are just code values. Since code is a type of
      # string, the current expression typing analysis may produce a string
      # type here so we accept both string and code.
      count_query = (f'WITH c AS ({expr_array_query}) '
                     f'SELECT code, COUNT(*) count '
                     f'FROM c, UNNEST(c.target) as code '
                     f'GROUP BY 1 ORDER BY count DESC')
    else:
      raise ValueError(
          f'Field must be a FHIR CodeableConcept, Coding, or Code; '
          f'got {node_type.url}.')

    return self._client.query(count_query).result().to_dataframe()

  def _create_valueset_codes_table_if_not_exists(self) -> bigquery.table.Table:
    """Creates a table for storing value set code mappings.

    Creates a table named after the `value_set_codes_table` provided at class
    initialization as described by
    https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support

    If the table already exists, no action is taken.

    Returns:
      An bigquery.Table object representing the created table.
    """
    schema = [
        bigquery.SchemaField('valueseturi', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('valuesetversion', 'STRING', mode='NULLABLE'),
        bigquery.SchemaField('system', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('code', 'STRING', mode='REQUIRED'),
    ]
    table = bigquery.Table(self._value_set_codes_table, schema=schema)
    table.clustering_fields = ['valueseturi', 'code']
    return self._client.create_table(table, exists_ok=True)

  # TODO(b/201107372): Update FHIR-agnostic types to a protocol.
  def materialize_value_sets(self,
                             value_set_protos: Iterable[value_set_pb2.ValueSet],
                             batch_size: int = 500) -> None:
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
    bq_table = self._create_valueset_codes_table_if_not_exists()

    sa_table = _bq_table_to_sqlalchemy_table(bq_table)
    queries = value_set_tables.valueset_codes_insert_statement_for(
        value_set_protos, sa_table, batch_size=batch_size)

    # Render the query objects as strings and use the client to execute them.
    for query in queries:
      query_string = str(
          query.compile(
              dialect=(sqlalchemy_bigquery.BigQueryDialect()),
              compile_kwargs={'literal_binds': True}))
      self._client.query(query_string).result()

  def materialize_value_set_expansion(
      self,
      urls: Iterable[str],
      expander: Union[terminology_service_client.TerminologyServiceClient,
                      value_sets.ValueSetResolver],
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
    if terminology_service_url is not None and not isinstance(
        expander, terminology_service_client.TerminologyServiceClient):
      raise TypeError(
          '`terminology_service_url` can only be given if `expander` is a '
          'TerminologyServiceClient')

    if terminology_service_url is not None and isinstance(
        expander, terminology_service_client.TerminologyServiceClient):
      expanded_value_sets = (
          expander.expand_value_set_url_using_service(url,
                                                      terminology_service_url)
          for url in urls)
    else:
      expanded_value_sets = (expander.expand_value_set_url(url) for url in urls)

    self.materialize_value_sets(expanded_value_sets, batch_size=batch_size)


def _memberof_nodes_from_view(
    view: views.View) -> Collection[_evaluation.MemberOfFunction]:
  """Retrieves all MemberOfFunction in the given `view`."""
  nodes = []
  for builder in itertools.chain(view.get_select_expressions().values(),
                                 view.get_constraint_expressions()):
    # pylint: disable=protected-access
    nodes.extend(_memberof_nodes_from_node(builder._node))

  return nodes


def _memberof_nodes_from_node(
    node: _evaluation.ExpressionNode
) -> Collection[_evaluation.MemberOfFunction]:
  """Retrieves MemberOfFunction nodes among the given `node` and its operands."""
  nodes = []
  if isinstance(node, _evaluation.MemberOfFunction):
    nodes.append(node)

  # Recursively get valuesets from operands, which will terminate at
  # primitive leafs or message-level nodes.
  for operand_node in node.operands():
    nodes.extend(_memberof_nodes_from_node(operand_node))

  return nodes


def _bq_table_to_sqlalchemy_table(
    bq_table: bigquery.table.Table) -> sqlalchemy.sql.selectable.TableClause:
  """Converts a BigQuery client Table to an sqlalchemy Table."""
  table_name = f'{bq_table.project}.{bq_table.dataset_id}.{bq_table.table_id}'
  columns = [sqlalchemy.column(column.name) for column in bq_table.schema]
  return sqlalchemy.table(table_name, *columns)

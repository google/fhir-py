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
"""Utility class to generate SQL expressions for different encoders."""

import itertools
from typing import Collection, Mapping, MutableSequence, Optional, Sequence, Tuple, Union

import numpy
import pandas as pd

from google.fhir.core.fhir_path import _bigquery_interpreter
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _spark_interpreter
from google.fhir.core.fhir_path import fhir_path
from google.fhir.views import column_expression_builder
from google.fhir.views import views

CODEABLE_CONCEPT = 'http://hl7.org/fhir/StructureDefinition/CodeableConcept'
CODING = 'http://hl7.org/fhir/StructureDefinition/Coding'
CODE = 'http://hl7.org/fhir/StructureDefinition/Code'
STRING = 'http://hl7.org/fhirpath/System.String'

PATIENT_ID_GENERATED_COLUMN_NAME = '__patientId__'


class RunnerSqlGenerator:
  """Generates SQL for the different encoders."""

  def __init__(
      self,
      view: views.View,
      encoder: Union[
          _bigquery_interpreter.BigQuerySqlInterpreter,
          _spark_interpreter.SparkSqlInterpreter,
          fhir_path.FhirPathStandardSqlEncoder,
      ],
      dataset: str,
      table_names: Mapping[str, str],
  ):
    """Initializes the class with provided arguments.

    Args:
      view: the view to generate SQL from
      encoder: the translator to use to generate SQL
      dataset: the dataset to query from
      table_names: the table names for each resource in the view
    """
    self._view = view
    self._encoder = encoder
    self._dataset = dataset
    self._table_names = table_names
    self._v1_extras: Optional[FhirPathInterpreterVariables] = (
        FhirPathInterpreterVariables(view)
        if isinstance(encoder, fhir_path.FhirPathStandardSqlEncoder)
        else None
    )

  def build_sql_statement(
      self,
      include_patient_id_col: bool,
  ) -> str:
    """Build SQL statement.

      Build a separate SQL query for each url first and then INNER JOIN on
      patient id at the very end. This way, any where clause filters occur first
      on the respective query.

    Args:
      include_patient_id_col: whether to include a __patientId__ column to
        indicate the patient the resource is associated with.

    Returns:
     SQL string representation of the view
    """
    inner_sql_statements = []

    # Sort to always generate the urls in the same order for testing purposes.
    sorted_urls = sorted(self._view.get_structdef_urls())

    for url in sorted_urls:
      inner_select_builders = self._get_inner_select_builders(url)
      patient_id_builder = self._get_patient_id_builder(
          url, include_patient_id_col
      )
      inner_select_expressions = self._build_inner_select_expressions(
          url, inner_select_builders, patient_id_builder
      )
      inner_where_expressions = self._build_inner_where_expressions(url)

      # Build statement for the resource.
      inner_sql_statements.append(
          self._build_inner_sql_statement(
              url, inner_select_expressions, inner_where_expressions
          )
      )

    sql_statement = self._join_sql_statements(inner_sql_statements)

    # Then build the query for the builders that reference multiple resources
    # now that the resources are all in one table.
    multiresource_select_expressions = (
        self._build_multiresource_select_expressions()
    )
    multiresource_where_expressions = (
        self._build_multiresource_where_expressions()
    )

    if multiresource_select_expressions:
      sql_statement = (
          'SELECT *, '
          f'{",".join(multiresource_select_expressions)} '
          f'FROM ({sql_statement})'
      )
    if multiresource_where_expressions:
      sql_statement = (
          f'{sql_statement}'
          '\nWHERE '
          f'{" AND ".join(multiresource_where_expressions)}'
      )

    return f'{sql_statement}'

  def _get_inner_select_builders(
      self,
      url: str,
  ) -> MutableSequence[column_expression_builder.ColumnExpressionBuilder]:
    """Get inner select builders. Include a __patientId__ builder if needed."""
    inner_select_builders = []

    if url in self._view.get_url_to_field_indexes():
      for index in self._view.get_url_to_field_indexes()[url]:
        inner_select_builders.append(self._view.get_select_expressions()[index])

    return inner_select_builders

  def _get_patient_id_builder(
      self, url: str, include_patient_id_col: bool
  ) -> Optional[column_expression_builder.ColumnExpressionBuilder]:
    if include_patient_id_col or len(self._view.get_structdef_urls()) > 1:
      # Auto generate the __patientId__ field for the view if it exists for
      # every unique resource.
      builder = self._view.get_patient_id_expression(url)
      if builder:
        return builder.alias(PATIENT_ID_GENERATED_COLUMN_NAME)
    return None

  def _build_inner_select_expressions(
      self,
      url: str,
      select_builders: MutableSequence[
          column_expression_builder.ColumnExpressionBuilder
      ],
      patient_id_builder: Optional[
          column_expression_builder.ColumnExpressionBuilder
      ],
  ) -> MutableSequence[str]:
    """Build inner select expressions."""
    inner_select_expressions = []

    if url == self._view.get_root_resource_url() and not select_builders:
      # If there're no select builders, it means selecting all fields from root.
      inner_select_expressions.append('*')

    if patient_id_builder:
      select_builders.append(patient_id_builder)

    for builder in select_builders:
      select_expression = self._encode(
          builder=builder, select_scalars_as_array=False
      )

      inner_select_expressions.append(
          f'{select_expression} AS {builder.column_name}'
      )
    return inner_select_expressions

  def _build_inner_where_expressions(self, url: str) -> MutableSequence[str]:
    """Build inner where expressions."""
    inner_where_expressions = []
    url_to_constraint_indexes = self._view.get_url_to_constraint_indexes()

    if url in url_to_constraint_indexes:
      for index in url_to_constraint_indexes[url]:
        expr = self._view.get_constraint_expressions()[index]
        where_expression = self._encode(
            builder=expr, select_scalars_as_array=True
        )

        inner_where_expressions.append(
            self._encoder.wrap_where_expression(where_expression)
        )
    return inner_where_expressions

  def _build_inner_sql_statement(
      self,
      url: str,
      inner_select_expressions: MutableSequence[str],
      inner_where_expressions: MutableSequence[str],
  ) -> str:
    """Build inner SQL statement from list of select and where statements."""
    table_alias = ''

    # Check if the view has builders that reference multiple resources.
    has_mixed_resource_builders = (
        len(self._view.get_multiresource_field_indexes())
        + len(self._view.get_multiresource_constraint_indexes())
    ) != 0

    if has_mixed_resource_builders:
      # For views with mixed resource builders, * and table_names[url] will
      # double the number of rows produced.
      if '*' in inner_select_expressions:
        inner_select_expressions = inner_select_expressions[1:]
      inner_select_expressions.append(self._table_names[url])
      table_alias = f' {self._table_names[url]}'

    inner_select_clause = (
        f'SELECT {",".join(inner_select_expressions)} '
        f'FROM `{self._dataset}`.{self._table_names[url]}{table_alias}'
    )
    inner_where_clause = ''
    if inner_where_expressions:
      inner_where_clause = f'\nWHERE {" AND ".join(inner_where_expressions)}'

    return f'{inner_select_clause}{inner_where_clause}'

  def _join_sql_statements(self, sql_statements: Sequence[str]) -> str:
    """Joins all the passed SQL statements into one SQL statement."""
    if len(sql_statements) > 2:
      raise NotImplementedError(
          'Cross resource join for more than two resources '
          'is not currently supported.'
      )

    if len(sql_statements) == 1:
      return sql_statements[0]

    return (
        f'SELECT * , __patientId__ FROM\n(({sql_statements[0]})'
        f'\nINNER JOIN\n({self._join_sql_statements(sql_statements[1:])})'
        '\nUSING(__patientId__))'
    )

  def _build_multiresource_select_expressions(self) -> Sequence[str]:
    """Returns a list of select expressions for multi resource selections."""

    multiresource_select_expressions = []
    for index in self._view.get_multiresource_field_indexes():
      builder = self._view.get_select_expressions()[index]
      select_expression = self._encode(
          builder=builder,
          select_scalars_as_array=False,
          use_resource_alias=True,
      )
      multiresource_select_expressions.append(
          f'{select_expression} AS {builder.column_name}'
      )
    return multiresource_select_expressions

  def _build_multiresource_where_expressions(self) -> Sequence[str]:
    """Returns a list of where expressions for multi resource constraints."""

    multiresource_where_expressions = []
    for index in self._view.get_multiresource_constraint_indexes():
      expr = self._view.get_constraint_expressions()[index]
      where_expression = self._encode(
          builder=expr, select_scalars_as_array=True, use_resource_alias=True
      )

      multiresource_where_expressions.append(
          self._encoder.wrap_where_expression(where_expression)
      )
    return multiresource_where_expressions

  def build_valueset_expression(self, view_table_name: str) -> str:
    """Returns the expression for valuesets, if needed."""
    # TODO(b/269329295): This is a very similar to the behavior of
    # FhirPathStandardSqlEncoder when passing it a package manager with
    # value sets to use for backing memberOf expressions. We should
    # reconcile the two approaches.
    fhir_context = self._view.get_fhir_path_context()
    memberof_nodes = _memberof_nodes_from_view(self._view)
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
        row = (
            f'SELECT "{value_set_codes.url}" as valueseturi, '
            f'{version_sql} as valuesetversion, '
            f'"{code_value.system}" as system, '
            f'"{code_value.value}" as code'
        )
        value_set_rows.append(row)

    # Include the entire value_set_codes_table_name in addition to any other
    # custom value set definitions provided by callers.
    if include_value_set_codes_table:
      value_set_rows.append(
          'SELECT valueseturi, valuesetversion, system, code FROM'
          f' {view_table_name}'
      )
    if value_set_rows:
      rows_expression = '\nUNION ALL '.join(value_set_rows)
      return f'WITH VALUESET_VIEW AS ({rows_expression})\n'

    return ''

  def build_select_for_summarize_code(
      self, code_expr: column_expression_builder.ColumnExpressionBuilder
  ) -> str:
    """Builds select statement for use in summarize_codes functions for runners."""
    # TODO(b/239733067): Add constraint filtering to code summarization.
    if self._view.get_constraint_expressions():
      raise NotImplementedError(
          'Summarization of codes with view constraints not yet implemented.'
      )

    # Workaround for v1 until it gets deprecated.
    if (
        len(self._view.get_structdef_urls()) > 1
        or len(self._table_names.keys()) != 1
    ):
      raise NotImplementedError(
          'Summarization of codes with multiple resource views not yet'
          ' implemented.'
      )

    select_expression = self._encode(
        builder=code_expr, select_scalars_as_array=True
    )

    url = list(self._view.get_structdef_urls())[0]
    return (
        f'SELECT {select_expression} as target '
        f'FROM `{self._dataset}`.{self._table_names[url]}'
    )

  def _encode(
      self,
      builder: column_expression_builder.ColumnExpressionBuilder,
      select_scalars_as_array: bool,
      use_resource_alias: bool = False,
  ) -> str:
    """Encodes the expression to SQL."""
    if self._v1_extras:
      sql_statemet = self._encoder.encode(
          structure_definition=self._v1_extras.struct_def,
          element_definition=self._v1_extras.elem_def,
          fhir_path_expression=builder.fhir_path,
          select_scalars_as_array=select_scalars_as_array,
      )
      return fhir_path.wrap_datetime_sql(builder.builder, sql_statemet)
    else:
      return self._encoder.encode(
          builder=builder.builder,
          select_scalars_as_array=select_scalars_as_array,
          use_resource_alias=use_resource_alias,
      )


class FhirPathInterpreterVariables:
  """Holds variables used for the FhirPath runner only, given a view."""

  def __init__(self, view: views.View):
    fhir_context = view.get_fhir_path_context()
    url = list(view.get_structdef_urls())[0]
    self.struct_def = fhir_context.get_structure_definition(url)
    self.elem_def = next(
        elem
        for elem in self.struct_def.snapshot.element
        if elem.path.value == self.struct_def.name.value
    )


def _memberof_nodes_from_view(
    view: views.View,
) -> Collection[_evaluation.MemberOfFunction]:
  """Retrieves all MemberOfFunction in the given `view`."""
  nodes = []
  for builder in itertools.chain(
      view.get_select_expressions(), view.get_constraint_expressions()
  ):
    nodes.extend(_memberof_nodes_from_node(builder.node))

  return nodes


def _memberof_nodes_from_node(
    node: _evaluation.ExpressionNode,
) -> Collection[_evaluation.MemberOfFunction]:
  """Retrieves MemberOfFunction nodes among the given `node` and its operands."""
  nodes = []
  if isinstance(node, _evaluation.MemberOfFunction):
    nodes.append(node)

  # Recursively get valuesets from operands, which will terminate at
  # primitive leafs or message-level nodes.
  for operand_node in node.operands:
    nodes.extend(_memberof_nodes_from_node(operand_node))

  return nodes


def clean_dataframe(
    df: pd.DataFrame,
    select_expressions: Tuple[
        column_expression_builder.ColumnExpressionBuilder, ...
    ],
) -> pd.DataFrame:
  """Cleans dataframe retrieved from backend.

  Args:
    df: Dataframe to clean
    select_expressions: If the view has expressions, we can narrow the
      non-scalar column list by checking only for list or struct columns.

  Returns:
    Cleaned dataframe
  """
  if select_expressions:
    non_scalar_cols = [
        builder.column_name
        for builder in select_expressions
        if builder.return_type.returns_collection()
        or builder.return_type.fields()
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
      for key, value in item.items():
        trimmed_value = trim_structs(value)
        if trimmed_value is not None:
          result[key] = trimmed_value
      return result

    return item

  for col in non_scalar_cols:
    df[col] = df[col].map(trim_structs)

  return df

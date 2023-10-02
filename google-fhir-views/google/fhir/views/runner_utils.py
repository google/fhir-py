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
import re
from typing import Collection, MutableSequence, Optional, Tuple, Union

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
      snake_case_resource_tables: bool = False,
  ):
    """Initializes the class with provided arguments.

    Args:
      view: the view to generate SQL from
      encoder: the translator to use to generate SQL
      dataset: the dataset to query from
      snake_case_resource_tables: Whether to use snake_case names for resource
        tables in the data storidge for compatiblity with some exports. Defaults
        to False.
    """
    self._view = view
    self._encoder = encoder
    self._dataset = dataset
    self._table_name = self._get_view_resource_table_name(
        snake_case_resource_tables
    )
    self._v1_extras: Optional[FhirPathInterpreterVariables] = (
        FhirPathInterpreterVariables(view)
        if isinstance(encoder, fhir_path.FhirPathStandardSqlEncoder)
        else None
    )

  def build_sql_statement(self) -> str:
    """Build SQL statement.

    Returns:
     SQL string representation of the view
    """
    select_expressions = self._build_select_expressions(
        self._view.get_select_expressions()
    )
    where_expressions = self._build_where_expressions(
        self._view.get_constraint_expressions()
    )

    return self._build_sql_statement(select_expressions, where_expressions)

  def _build_select_expressions(
      self,
      select_builders: Tuple[
          column_expression_builder.ColumnExpressionBuilder, ...
      ],
  ) -> MutableSequence[str]:
    """Build select expressions."""
    select_expressions = []

    if not select_builders:
      # If there're no select builders, it means selecting all fields from root.
      select_expressions.append('*')

    for builder in select_builders:
      select_expression = self._encode(
          builder=builder, select_scalars_as_array=False
      )

      select_expressions.append(f'{select_expression} AS {builder.column_name}')
    return select_expressions

  def _build_where_expressions(
      self,
      constraint_builders: Tuple[
          column_expression_builder.ColumnExpressionBuilder, ...
      ],
  ) -> MutableSequence[str]:
    """Build where expressions."""
    where_expressions = []

    for builder in constraint_builders:
      where_expression = self._encode(
          builder=builder, select_scalars_as_array=True
      )

      where_expressions.append(
          self._encoder.wrap_where_expression(where_expression)
      )
    return where_expressions

  def _build_sql_statement(
      self,
      select_expressions: MutableSequence[str],
      where_expressions: MutableSequence[str],
  ) -> str:
    """Build SQL statement from list of select and where statements."""
    select_clause = (
        f'SELECT {",".join(select_expressions)} '
        f'FROM `{self._dataset}`.{self._table_name}'
    )
    where_clause = ''
    if where_expressions:
      where_clause = f'\nWHERE {" AND ".join(where_expressions)}'

    return f'{select_clause}{where_clause}'

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

    select_expression = self._encode(
        builder=code_expr, select_scalars_as_array=True
    )

    return (
        f'SELECT {select_expression} as target '
        f'FROM `{self._dataset}`.{self._table_name}'
    )

  def _encode(
      self,
      builder: column_expression_builder.ColumnExpressionBuilder,
      select_scalars_as_array: bool,
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
      )

  def _get_view_resource_table_name(
      self, snake_case_resource_tables: bool
  ) -> str:
    """Returns the name of the table to query for the given view."""
    url = self._view.get_structdef_url()
    last_slash_index = url.rfind('/')
    name = url if last_slash_index == -1 else url[last_slash_index + 1 :]
    if snake_case_resource_tables:
      return (
          re.sub(pattern=r'([A-Z]+)', repl=r'_\1', string=name)
          .lower()
          .lstrip('_')
      )
    return name


class FhirPathInterpreterVariables:
  """Holds variables used for the FhirPath runner only, given a view."""

  def __init__(self, view: views.View):
    fhir_context = view.get_fhir_path_context()
    url = view.get_structdef_url()
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

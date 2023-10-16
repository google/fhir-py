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
from typing import Collection, Mapping, MutableSequence, Optional, Tuple, Union

import numpy
import pandas as pd

from google.fhir.core.fhir_path import _bigquery_interpreter
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
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
    builders = self._view.get_select_expressions()
    from_expressions = [f'`{self._dataset}`.{self._table_name}']
    where_expressions = self._build_where_expressions(
        self._view.get_constraint_expressions()
    )

    if not builders:
      # If there're no select builders, it means selecting all fields from root.
      return self._build_sql_statement(
          ['*'], from_expressions, where_expressions
      )

    sql_statement = ''
    next_from_expressions = []
    child_builders = []
    columns_selected = []
    while builders or next_from_expressions:
      select_expressions, next_from_expressions = (
          self._build_select_and_next_from_expressions(
              builders,
              child_builders,
              columns_selected,
          )
      )
      sql_statement = self._build_sql_statement(
          select_expressions, from_expressions, where_expressions
      )
      from_expressions = [f'({sql_statement})']
      from_expressions.extend(next_from_expressions)
      where_expressions = []
      builders = tuple(child_builders)
      child_builders = []
    return sql_statement

  def _build_select_and_next_from_expressions(
      self,
      builders: Tuple[column_expression_builder.ColumnExpressionBuilder, ...],
      child_builders: MutableSequence[
          column_expression_builder.ColumnExpressionBuilder
      ],
      columns_selected: MutableSequence[str],
  ) -> Tuple[MutableSequence[str], MutableSequence[str]]:
    """Build select expressions and next from expressions from the builders.

    Args:
      builders: the immutable current builders to compute select expressions.
      child_builders: collects the current given builders' children for the next
        round.
      columns_selected: accumulatively collects columns which has already been
        handled completely.

    Returns:
      The select expressions and next from expressions computed form the given
      builders.
    """
    select_expressions = []
    next_from_expressions = []

    for column_name in columns_selected:
      select_expressions.append(f'(SELECT {column_name}) AS {column_name}')

    for builder in builders:
      child_builders.extend(builder.children)

      if builder.column_name:
        column_alias = builder.column_name
        columns_selected.append(builder.column_name)
      else:
        # Find the last invoke node's identifier as the intermediate name.
        invoke_node = builder.node
        while (
            invoke_node
            and not hasattr(invoke_node, 'identifier')
            or not invoke_node.identifier
        ):
          invoke_node = invoke_node.parent_node
        column_alias = invoke_node.identifier

      needs_unnest = builder.needs_unnest or builder.children
      select_expression = self._encode(
          builder=builder,
          select_scalars_as_array=needs_unnest,
      )
      if needs_unnest:
        select_expression = (
            f'{select_expression} AS {column_alias}_needs_unnest_'
        )
        next_from_expressions.append(
            f'UNNEST({column_alias}_needs_unnest_) AS {column_alias}'
        )
      else:
        select_expression = f'{select_expression} AS {column_alias}'
      select_expressions.append(select_expression)
    return (select_expressions, next_from_expressions)

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
      from_expressions: MutableSequence[str],
      where_expressions: MutableSequence[str],
  ) -> str:
    """Build SQL statement from list of select and where statements."""
    select_clause = (
        f'SELECT {",".join(select_expressions)} '
        f'FROM {",".join(from_expressions)}'
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
    column_to_return_type_mapping: Mapping[
        str,
        _fhir_path_data_types.FhirPathDataType,
    ],
) -> pd.DataFrame:
  """Cleans dataframe retrieved from backend.

  Args:
    df: Dataframe to clean
    column_to_return_type_mapping: If the view has columns, we can narrow the
      non-scalar column list by checking only for list or struct columns.

  Returns:
    Cleaned dataframe
  """
  non_scalar_cols = []
  if not column_to_return_type_mapping:
    # No fields were specified, so we must check any 'object' field
    # in the dataframe.
    non_scalar_cols = df.select_dtypes(include=['object']).columns.tolist()

  for column_name, return_type in column_to_return_type_mapping.items():
    if return_type.returns_collection() or return_type.fields():
      non_scalar_cols.append(column_name)

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

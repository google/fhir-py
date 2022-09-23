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
"""Functionality to output BigQuery SQL expressions from FHIRPath expressions."""

from typing import Any

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.fhir_path import expressions


def _escape_identifier(identifier_value: str) -> str:
  """Returns the value surrounded by backticks if it is a keyword."""
  # Keywords are case-insensitive
  if identifier_value.upper() in _sql_data_types.STANDARD_SQL_KEYWORDS:
    return f'`{identifier_value}`'
  return identifier_value  # No-op


_FHIR_PATH_URL_TO_STANDARD_SQL_TYPE = {
    _fhir_path_data_types.Boolean.url: _sql_data_types.Boolean,
    _fhir_path_data_types.Integer.url: _sql_data_types.Int64,
    _fhir_path_data_types.Decimal.url: _sql_data_types.Numeric,
    _fhir_path_data_types.String.url: _sql_data_types.String,
    _fhir_path_data_types.Quantity.url: _sql_data_types.String,
    _fhir_path_data_types.DateTime.url: _sql_data_types.Datetime,
    _fhir_path_data_types.Date.url: _sql_data_types.Date,
    _fhir_path_data_types.Time.url: _sql_data_types.Time,
}


class BigQuerySqlInterpreter(_evaluation.ExpressionNodeBaseVisitor):
  """Traverses the ExpressionNode tree and generates BigQuery SQL recursively."""

  def _get_standard_sql_data_type(
      self, fhir_type: _fhir_path_data_types.FhirPathDataType
  ) -> _sql_data_types.StandardSqlDataType:
    sql_type = _FHIR_PATH_URL_TO_STANDARD_SQL_TYPE.get(fhir_type.url)
    return sql_type if sql_type else _sql_data_types.Undefined

  def encode(self,
             builder: expressions.Builder,
             select_scalars_as_array: bool = True) -> str:
    """Returns a Standard SQL encoding of a FHIRPath expression.

    If select_scalars_as_array is True, the resulting Standard SQL encoding
    always returns a top-level `ARRAY`, whose elements are non-`NULL`. Otherwise
    the resulting SQL will attempt to return a scalar when possible and only
    return an `ARRAY` for actual collections.

    Args:
      builder: The FHIR Path builder to encode as a SQL string.
      select_scalars_as_array: When True, always builds SQL selecting results in
        an array. When False, attempts to build SQL returning scalars where
        possible.

    Returns:
      A Standard SQL representation of the provided FHIRPath expression.
    """

    result = self.visit(builder.get_node())
    if select_scalars_as_array or (
        builder.get_node().return_type() and
        builder.get_node().return_type().is_collection()):
      return (f'ARRAY(SELECT {result.sql_alias}\n'
              f'FROM {result.to_subquery()}\n'
              f'WHERE {result.sql_alias} IS NOT NULL)')
    else:
      # Parenthesize raw SELECT so it can plug in anywhere an expression can.
      return f'({result})'

  def visit_literal(
      self, literal: _evaluation.LiteralNode) -> _sql_data_types.RawExpression:
    """Translates a FHIRPath literal to Standard SQL."""

    if literal.return_type() is None:
      sql_value = 'NULL'
      sql_data_type = _sql_data_types.Undefined
    elif isinstance(literal.return_type(), _fhir_path_data_types._Boolean):  # pylint: disable=protected-access
      sql_value = str(literal.get_value()).upper()
      sql_data_type = _sql_data_types.Boolean
    elif isinstance(literal.return_type(), _fhir_path_data_types._Quantity):  # pylint: disable=protected-access
      sql_value = f"'{literal.get_value()}'"  # Quote string literals for SQL
      sql_data_type = _sql_data_types.String
    elif isinstance(literal.return_type(), _fhir_path_data_types._Integer):  # pylint: disable=protected-access
      sql_value = str(literal.get_value())
      sql_data_type = _sql_data_types.Int64
    elif isinstance(literal.return_type(), _fhir_path_data_types._Decimal):  # pylint: disable=protected-access
      sql_value = str(literal.get_value())
      sql_data_type = _sql_data_types.Numeric
    else:
      # LiteralNode constructor ensures that literal has to be one of the above
      # cases. But we error out here in case we enter an illegal state.
      raise ValueError(f'Unsupported literal value: {literal}.')

    return _sql_data_types.RawExpression(
        sql_value,
        _sql_data_type=sql_data_type,
        _sql_alias='literal_',
    )

  def visit_root(self, root: _evaluation.RootMessageNode) -> None:
    # TODO: Consider returning an empty sql statement with the
    # from_part filled in.
    return None

  def visit_invoke_expression(
      self, identifier: _evaluation.InvokeExpressionNode
  ) -> _sql_data_types.IdentifierSelect:
    """Translates a FHIRPath member identifier to Standard SQL."""
    parent_result = self.visit(identifier.operand_node)

    # TODO: Handle "special" identifiers
    if identifier.identifier == '$this':
      raise NotImplementedError('TODO: add support for $this.')
    else:
      raw_identifier_str = identifier.identifier

    # Map to Standard SQL type. Note that we never map to a type of `ARRAY`,
    # as the member encoding flattens any `ARRAY` members.
    sql_data_type = self._get_standard_sql_data_type(identifier.return_type())

    identifier_str = _escape_identifier(raw_identifier_str)
    if (identifier.return_type() and
        identifier.return_type().is_collection()):  # Array
      # If the identifier is `$this`, we assume that the repeated field has been
      # unnested upstream so we only need to reference it with its alias:
      # `{}_element_`.
      if identifier.identifier == '$this':
        sql_alias = f'{raw_identifier_str}_element_'
        return _sql_data_types.IdentifierSelect(
            select_part=_sql_data_types.Identifier(sql_alias, sql_data_type),
            from_part=parent_result,
        )
      else:
        sql_alias = f'{raw_identifier_str}_element_'
        if parent_result:
          parent_identifier_str = parent_result.sql_alias
          identifier_str = f'{parent_identifier_str}.{identifier_str}'
        else:
          identifier_str = f'{identifier_str}'

        from_part = (f'UNNEST({identifier_str}) AS {sql_alias} '
                     f'WITH OFFSET AS element_offset')
        if parent_result:
          from_part = f'({parent_result}),\n{from_part}'
        # When UNNEST-ing a repeated field, we always generate an offset column
        # as well. If unused by the overall query, the expectation is that the
        # BigQuery query optimizer will be able to detect the unused column and
        # ignore it.
        return _sql_data_types.IdentifierSelect(
            select_part=_sql_data_types.Identifier(sql_alias, sql_data_type),
            from_part=from_part)
    else:  # Scalar
      return _sql_data_types.IdentifierSelect(
          select_part=_sql_data_types.Identifier(identifier_str, sql_data_type),
          from_part=parent_result,
      )

  def visit_indexer(self, indexer: _evaluation.IndexerNode) -> Any:
    raise NotImplementedError('TODO: Implement `Indexer` visitor')

  def visit_arithmetic(self, arithmetic: _evaluation.ArithmeticNode) -> Any:
    raise NotImplementedError('TODO: Implement `Arithmetic` visitor')

  def visit_equality(self, equality: _evaluation.EqualityNode) -> Any:
    raise NotImplementedError('TODO: Implement `Equality` visitor.')

  def visit_comparison(self, comparison: _evaluation.ComparisonNode) -> Any:
    raise NotImplementedError('TODO: Implement `Comparison` visitor.')

  def visit_boolean_op(self,
                       boolean_logic: _evaluation.BooleanOperatorNode) -> Any:
    raise NotImplementedError('TODO: Implement `BooleanOp` visitor')

  def visit_member_of(self, membership: _evaluation.MemberOfFunction) -> Any:
    raise NotImplementedError('TODO: Implement `MemberOf` visitor.')

  def visit_polarity(self, polarity: _evaluation.NumericPolarityNode) -> Any:
    raise NotImplementedError('TODO: Implement `Polarity` visitor.')

  def visit_function(self, function: _evaluation.FunctionNode) -> Any:
    raise NotImplementedError('TODO: Implement `Function` visitor.')

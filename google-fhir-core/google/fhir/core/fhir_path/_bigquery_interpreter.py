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

import dataclasses
from typing import Any

from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _bigquery_sql_functions
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
    _fhir_path_data_types.Quantity.url: _sql_data_types.OpaqueStruct,
    _fhir_path_data_types.DateTime.url: _sql_data_types.Timestamp,
    _fhir_path_data_types.Date.url: _sql_data_types.Date,
    _fhir_path_data_types.Time.url: _sql_data_types.Time,
}


class BigQuerySqlInterpreter(_evaluation.ExpressionNodeBaseVisitor):
  """Traverses the ExpressionNode tree and generates BigQuery SQL recursively."""

  def _get_standard_sql_data_type(
      self, fhir_type: _fhir_path_data_types.FhirPathDataType
  ) -> _sql_data_types.StandardSqlDataType:
    if not fhir_type:
      return _sql_data_types.Undefined
    if isinstance(fhir_type, _fhir_path_data_types.StructureDataType):
      return _sql_data_types.OpaqueStruct
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
    if select_scalars_as_array or _fhir_path_data_types.is_collection(
        builder.get_node().return_type()):
      return (f'ARRAY(SELECT {result.sql_alias}\n'
              f'FROM {result.to_subquery()}\n'
              f'WHERE {result.sql_alias} IS NOT NULL)')
    else:
      # Parenthesize raw SELECT so it can plug in anywhere an expression can.
      return f'({result})'

  def visit_literal(
      self, literal: _evaluation.LiteralNode) -> _sql_data_types.RawExpression:
    """Translates a FHIRPath literal to Standard SQL."""

    if (literal.return_type() is None or
        isinstance(literal.return_type(), _fhir_path_data_types._Empty)):  # pylint: disable=protected-access
      sql_value = 'NULL'
      sql_data_type = _sql_data_types.Undefined
    # TODO: Make _fhir_path_data_types.FhirPathDataType classes public.
    elif isinstance(literal.return_type(), _fhir_path_data_types._Boolean):  # pylint: disable=protected-access
      sql_value = str(literal).upper()
      sql_data_type = _sql_data_types.Boolean
    elif isinstance(literal.return_type(), _fhir_path_data_types._Quantity):  # pylint: disable=protected-access
      sql_value = f"'{literal}'"  # Quote string literals for SQL
      sql_data_type = _sql_data_types.String
    elif isinstance(literal.return_type(), _fhir_path_data_types._Integer):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Int64
    elif isinstance(literal.return_type(), _fhir_path_data_types._Decimal):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Numeric
    elif isinstance(literal.return_type(), _fhir_path_data_types._DateTime):  # pylint: disable=protected-access
      # Date and datetime literals start with an @ and need to be quoted.
      sql_value = f"'{str(literal)[1:]}'"
      sql_data_type = _sql_data_types.Timestamp
    elif isinstance(literal.return_type(), _fhir_path_data_types._Date):  # pylint: disable=protected-access
      sql_value = f"'{str(literal)[1:]}'"
      sql_data_type = _sql_data_types.Date
    elif isinstance(literal.return_type(), _fhir_path_data_types._String):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.String
    else:
      # LiteralNode constructor ensures that literal has to be one of the above
      # cases. But we error out here in case we enter an illegal state.
      raise ValueError(
          f'Unsupported literal value: {literal} {literal.return_type()}.')

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
    sql_alias = f'{raw_identifier_str}'
    identifier_str = f'{raw_identifier_str}'
    if _fhir_path_data_types.is_collection(identifier.return_type()):  # Array
      # If the identifier is `$this`, we assume that the repeated field has been
      # unnested upstream so we only need to reference it with its alias:
      # `{}_element_`.
      if identifier.identifier == '$this':
        sql_alias = f'{sql_alias}_element_'
        return _sql_data_types.IdentifierSelect(
            select_part=_sql_data_types.Identifier(raw_identifier_str,
                                                   sql_data_type),
            from_part=parent_result,
        )
      else:
        sql_alias = f'{sql_alias}_element_'
        if parent_result:
          parent_identifier_str = parent_result.sql_alias
          identifier_str = f'{parent_identifier_str}.{raw_identifier_str}'
        else:
          # Identifiers need to be escaped if they are referenced directly.
          identifier_str = f'{_escape_identifier(raw_identifier_str)}'

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
      # Append the current identifier to the path chain being selected if there
      # is a parent.Includes the from & where clauses of the parent.
      if parent_result:
        return dataclasses.replace(
            parent_result,
            select_part=parent_result.select_part.dot(
                raw_identifier_str,
                sql_data_type,
                sql_alias=_escape_identifier(sql_alias),
            ))
      else:
        return _sql_data_types.IdentifierSelect(
            select_part=_sql_data_types.Identifier(
                _escape_identifier(identifier_str), sql_data_type),
            from_part=parent_result,
        )

  def visit_indexer(self,
                    indexer: _evaluation.IndexerNode) -> _sql_data_types.Select:
    """Translates a FHIRPath indexer expression to Standard SQL.

    Args:
      indexer: The `_Indexer` Expression node.

    Returns:
      A compiled Standard SQL expression.
    """
    collection_result = self.visit(indexer.collection)
    index_result = self.visit(indexer.index)

    # Intermediate indexed table subquery.
    indexed_collection = ('SELECT ROW_NUMBER() OVER() AS row_,\n'
                          f'{collection_result.sql_alias}\n'
                          f'FROM {collection_result.to_subquery()}')

    # Construct SQL expression; index must be a single integer per the FHIRPath
    # grammar, so we can leverage a scalar subquery.
    sql_alias = f'indexed_{collection_result.sql_alias}'
    return _sql_data_types.Select(
        select_part=_sql_data_types.Identifier(
            collection_result.sql_alias,
            collection_result.sql_data_type,
            _sql_alias=sql_alias,
        ),
        from_part=f'({indexed_collection}) AS inner_tbl',
        where_part=f'(inner_tbl.row_ - 1) = {index_result.as_operand()}',
    )

  def visit_arithmetic(
      self, arithmetic: _evaluation.ArithmeticNode) -> _sql_data_types.Select:
    """Translates a FHIRPath arithmetic expression to Standard SQL.

    Each operand is expected to be a collection of a single element. Both
    operands must be of the same type, or of compatible types according to the
    rules of implicit conversion.

    Args:
      arithmetic: The `_Arithmetic` Expression node.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(arithmetic.left)
    rhs_result = self.visit(arithmetic.right)
    sql_data_type = _sql_data_types.coerce(lhs_result.sql_data_type,
                                           rhs_result.sql_data_type)

    # Extract the values of LHS and RHS to be used as scalar subqueries.
    lhs_subquery = lhs_result.as_operand()
    rhs_subquery = rhs_result.as_operand()

    # TODO: Handle <string> + <string> when either operand is
    # empty.
    if sql_data_type == _sql_data_types.String:
      sql_value = f'CONCAT({lhs_subquery}, {rhs_subquery})'
    elif arithmetic.op == _ast.Arithmetic.Op.MODULO:
      sql_value = f'MOD({lhs_subquery}, {rhs_subquery})'
    elif arithmetic.op == _ast.Arithmetic.Op.TRUNCATED_DIVISION:
      sql_value = f'DIV({lhs_subquery}, {rhs_subquery})'
    else:  # +, -, *, /
      sql_value = f'({lhs_subquery} {arithmetic.op} {rhs_subquery})'

    sql_alias = 'arith_'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_value, _sql_data_type=sql_data_type, _sql_alias=sql_alias),
        from_part=None)

  def visit_equality(
      self, equality: _evaluation.EqualityNode) -> _sql_data_types.Select:
    """Returns `TRUE` if the left collection is equal/equivalent to the right.

    See more at: http://hl7.org/fhirpath/#equality.

    Args:
      equality: The `Equality` Expression node.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(equality.left)
    rhs_result = self.visit(equality.right)

    if (equality.op == _ast.EqualityRelation.Op.EQUAL or
        equality.op == _ast.EqualityRelation.Op.EQUIVALENT):
      collection_check_func_name = 'NOT EXISTS'
      scalar_check_op = '='
    else:  # NOT_*
      collection_check_func_name = 'EXISTS'
      scalar_check_op = '!='

    sql_alias = 'eq_'
    sql_data_type = _sql_data_types.Boolean

    # Both sides are scalars.
    if (_fhir_path_data_types.is_scalar(equality.left.return_type()) and
        _fhir_path_data_types.is_scalar(equality.right.return_type())):
      # Use the simpler query.
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              f'({lhs_result.as_operand()} '
              f'{scalar_check_op} '
              f'{rhs_result.as_operand()})',
              _sql_data_type=sql_data_type,
              _sql_alias=sql_alias),
          from_part=None)

    else:
      sql_expr = ('SELECT lhs_.*\n'
                  'FROM (SELECT ROW_NUMBER() OVER() AS row_, '
                  f'{lhs_result.sql_alias}\n'
                  f'FROM {lhs_result.to_subquery()}) AS lhs_\n'
                  'EXCEPT DISTINCT\n'
                  'SELECT rhs_.*\n'
                  'FROM (SELECT ROW_NUMBER() OVER() AS row_, '
                  f'{rhs_result.sql_alias}\n'
                  f'FROM {rhs_result.to_subquery()}) AS rhs_')

      return _sql_data_types.Select(
          select_part=_sql_data_types.FunctionCall(
              collection_check_func_name, (_sql_data_types.RawExpression(
                  sql_expr, _sql_data_type=_sql_data_types.Int64),),
              _sql_data_type=sql_data_type,
              _sql_alias=sql_alias),
          from_part=None)

  def visit_comparison(
      self, comparison: _evaluation.ComparisonNode) -> _sql_data_types.Select:
    """Translates a FHIRPath comparison to Standard SQL.

    Each operand is expected to be a collection of a single element. Operands
    can be strings, integers, decimals, dates, datetimes, and times. Comparison
    will perform implicit conversion between applicable types.

    Args:
      comparison: The `Comparison` Expression node.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(comparison.left)
    rhs_result = self.visit(comparison.right)

    # Extract the values of LHS and RHS to be used as scalar subqueries.
    lhs_subquery = lhs_result.as_operand()
    rhs_subquery = rhs_result.as_operand()

    sql_value = f'({lhs_subquery} {comparison.op} {rhs_subquery})'
    sql_alias = 'comparison_'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_value,
            _sql_data_type=_sql_data_types.Boolean,
            _sql_alias=sql_alias),
        from_part=None)

  def visit_boolean_op(
      self,
      boolean_logic: _evaluation.BooleanOperatorNode) -> _sql_data_types.Select:
    """Translates a FHIRPath Boolean logic operation to Standard SQL.

    Note that evaluation for Boolean logic is only supported for Boolean
    operands of scalar cardinality.

    Args:
      boolean_logic: The FHIRPath AST `BooleanLogic` node.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(boolean_logic.left)
    rhs_result = self.visit(boolean_logic.right)

    # Extract boolean values from both sides if needed.
    if lhs_result.sql_data_type != _sql_data_types.Boolean:
      lhs_result = lhs_result.is_not_null()
    if rhs_result.sql_data_type != _sql_data_types.Boolean:
      rhs_result = rhs_result.is_not_null()

    # Extract the values of LHS and RHS to be used as scalar subqueries.
    lhs_subquery = lhs_result.as_operand()
    rhs_subquery = rhs_result.as_operand()

    if boolean_logic.op == _ast.BooleanLogic.Op.IMPLIES:
      sql_value = f'(NOT {lhs_subquery} OR {rhs_subquery})'
    elif boolean_logic.op == _ast.BooleanLogic.Op.XOR:
      sql_value = f'({lhs_subquery} <> {rhs_subquery})'
    else:  # AND, OR
      sql_value = f'({lhs_subquery} {boolean_logic.op.upper()} {rhs_subquery})'

    sql_alias = 'logic_'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_value,
            _sql_data_type=_sql_data_types.Boolean,
            _sql_alias=sql_alias),
        from_part=None)

  def visit_member_of(self, membership: _evaluation.MemberOfFunction) -> Any:
    raise NotImplementedError(
        'TODO: Implement `in` or `contains` member of visitor.')

  def visit_polarity(
      self,
      polarity: _evaluation.NumericPolarityNode) -> _sql_data_types.Select:
    """Translates FHIRPath unary polarity (+/-) to Standard SQL."""
    operand_result = self.visit(polarity.operand)
    sql_expr = f'{polarity.op}{operand_result.as_operand()}'
    sql_alias = 'pol_'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_expr,
            _sql_data_type=operand_result.sql_data_type,
            _sql_alias=sql_alias,
        ),
        from_part=None)

  def visit_function(self, function: _evaluation.FunctionNode) -> Any:
    """Translates a FHIRPath function to Standard SQL."""
    parent_result = self.visit(function.parent_node())

    params_result = [self.visit(p) for p in function.params()]
    func = _bigquery_sql_functions.FUNCTION_MAP.get(function.NAME)
    return func(function, parent_result, params_result)

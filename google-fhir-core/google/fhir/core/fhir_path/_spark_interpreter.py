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
"""Functionality to output Spark SQL expressions from FHIRPath expressions."""

from typing import Any, Optional

from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.fhir_path import expressions
from google.fhir.core.internal import _primitive_time_utils


class SparkSqlInterpreter(_evaluation.ExpressionNodeBaseVisitor):
  """Traverses the ExpressionNode tree and generates BigQuery SQL recursively.
  """

  def encode(self,
             builder: expressions.Builder,
             select_scalars_as_array: bool = True) -> str:
    """Returns a Standard SQL encoding of a FHIRPath expression.

    If select_scalars_as_array is True, the resulting Standard SQL encoding
    always returns a top-level `COLLECT_LIST`, whose elements are non-`NULL`.
    Otherwise the resulting SQL will attempt to return a scalar when possible
    and only return a `COLLECT_LIST` for actual collections.

    Args:
      builder: The FHIR Path builder to encode as a SQL string.
      select_scalars_as_array: When True, always builds SQL selecting results in
        an array. When False, attempts to build SQL returning scalars where
        possible.

    Returns:
      A Standard SQL representation of the provided FHIRPath expression.
    """
    result = self.visit(builder.get_node())
    if select_scalars_as_array or _fhir_path_data_types.returns_collection(
        builder.get_node().return_type()):
      return (f'(SELECT COLLECT_LIST({result.sql_alias})\n'
              f'FROM {result.to_subquery()}\n'
              f'WHERE {result.sql_alias} IS NOT NULL)')
    else:
      # Parenthesize raw SELECT so it can plug in anywhere an expression can.
      return f'{result.to_subquery()}'

  def visit_root(
      self, root: _evaluation.RootMessageNode
  ) -> Optional[_sql_data_types.IdentifierSelect]:
    """Translates a FHIRPath root to Standard SQL."""

  def visit_reference(self, reference: _evaluation.ExpressionNode) -> Any:
    """Translates a FHIRPath reference to Standard SQL."""

  def visit_literal(
      self, literal: _evaluation.LiteralNode) -> _sql_data_types.RawExpression:
    """Translates a FHIRPath literal to Standard SQL."""

    if (literal.return_type() is None or
        isinstance(literal.return_type(), _fhir_path_data_types._Empty)):  # pylint: disable=protected-access
      sql_value = 'NULL'
      sql_data_type = _sql_data_types.Undefined
    # TODO(b/244184211): Make _fhir_path_data_types.FhirPathDataType classes
    # public.
    elif isinstance(literal.return_type(), _fhir_path_data_types._Boolean):  # pylint: disable=protected-access
      sql_value = str(literal).upper()
      sql_data_type = _sql_data_types.Boolean
    elif isinstance(literal.return_type(), _fhir_path_data_types._Quantity):  # pylint: disable=protected-access
      # Since quantity string literals contain quotes, they are escaped.
      # E.g. '10 \'mg\''.
      quantity_quotes_escaped = str(literal).translate(
          str.maketrans({'"': r'\"'}))
      sql_value = f"'{quantity_quotes_escaped}'"
      sql_data_type = _sql_data_types.String
    elif isinstance(literal.return_type(), _fhir_path_data_types._Integer):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Int64
    elif isinstance(literal.return_type(), _fhir_path_data_types._Decimal):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Numeric
    elif isinstance(literal.return_type(), _fhir_path_data_types._DateTime):  # pylint: disable=protected-access
      # Date and datetime literals start with an @ and need to be quoted.
      dt = _primitive_time_utils.get_date_time_value(literal.get_value())
      sql_value = f"'{dt.isoformat()}'"
      sql_data_type = _sql_data_types.Timestamp
    elif isinstance(literal.return_type(), _fhir_path_data_types._Date):  # pylint: disable=protected-access
      dt = _primitive_time_utils.get_date_time_value(literal.get_value()).date()
      sql_value = f"'{str(dt)}'"
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
        _sql_data_types.wrap_time_types_spark(sql_value, sql_data_type),
        _sql_data_type=sql_data_type,
        _sql_alias='literal_',
    )

  def visit_invoke_expression(
      self, identifier: _evaluation.InvokeExpressionNode) -> Any:
    """Translates a FHIRPath member identifier to Standard SQL."""

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

    # Construct SQL expression; index must be a single integer per the FHIRPath
    # grammar, so we can leverage a scalar subquery.
    sql_alias = f'indexed_{collection_result.sql_alias}'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            f'element_at(COLLECT_LIST({collection_result.sql_alias}),'
            f'{index_result.as_operand()} + 1)',
            collection_result.sql_data_type,
            _sql_alias=sql_alias,
        ),
        from_part=f'{collection_result.to_subquery()}',
    )

  def visit_arithmetic(
      self,
      arithmetic: _evaluation.ArithmeticNode) -> _sql_data_types.Select:
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

  def visit_equality(self, equality: _evaluation.EqualityNode):
    """Returns `TRUE` if the left collection is equal/equivalent to the right.

    See more at: http://hl7.org/fhirpath/#equality.

    Args:
      equality: The `Equality` Expression node.

    Returns:
      A compiled Standard SQL expression.
    """

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

    sql_value = f'{lhs_subquery} {comparison.op} {rhs_subquery}'
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
      sql_value = f'NOT {lhs_subquery} OR {rhs_subquery}'
    elif boolean_logic.op == _ast.BooleanLogic.Op.XOR:
      sql_value = f'{lhs_subquery} <> {rhs_subquery}'
    else:  # AND, OR
      sql_value = f'{lhs_subquery} {boolean_logic.op.upper()} {rhs_subquery}'

    sql_alias = 'logic_'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_value,
            _sql_data_type=_sql_data_types.Boolean,
            _sql_alias=sql_alias),
        from_part=None)

  def visit_membership(self,
                       relation: _evaluation.MembershipRelationNode) -> Any:
    """Translates a FHIRPath membership relation to Standard SQL.

    For the `IN` relation, the LHS operand is assumed to be a collection of a
    single value. For 'CONTAINS', the RHS operand is assumed to be a collection
    of a single value.

    Args:
      relation: The FHIRPath AST `MembershipRelation` node.

    Returns:
      A compiled Standard SQL expression.
    """

  def visit_union(self, union: _evaluation.UnionNode):
    """Translates a FHIRPath union to Standard SQL."""

  def visit_polarity(self, polarity: _evaluation.NumericPolarityNode):
    """Translates FHIRPath unary polarity (+/-) to Standard SQL."""

  def visit_function(self, function: _evaluation.FunctionNode) -> Any:
    """Translates a FHIRPath function to Standard SQL."""

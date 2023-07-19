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
import functools
from typing import Any, Optional

from google.cloud import bigquery

from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _bigquery_sql_functions
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.fhir_path import expressions
from google.fhir.core.internal import _primitive_time_utils
from google.fhir.core.utils import fhir_package
from google.fhir.r4.terminology import local_value_set_resolver


def _escape_identifier(identifier_value: str) -> str:
  """Returns the value in a valid column name format.

  Args:
    identifier_value: Raw identifier string.

  Returns:
    If the identifier is a keyword, then it will be surrounded by backticks. If
    the identifier contains a hyphen, the hyphen will be replaced by an
    underscore.
  """
  identifier_value = identifier_value.replace('-', '_')
  # Keywords are case-insensitive
  if identifier_value.upper() in _sql_data_types.STANDARD_SQL_KEYWORDS:
    return f'`{identifier_value}`'
  return identifier_value  # No-op


def _function_implementation_supports_polymorphic_type(
    node: _evaluation.FunctionNode,
):
  return isinstance(
      node,
      (
          _evaluation.ExistsFunction,
          _evaluation.OfTypeFunction,
          _evaluation.CountFunction,
      ),
  )


class BigQuerySqlInterpreter(_evaluation.ExpressionNodeBaseVisitor):
  """Traverses the ExpressionNode tree and generates BigQuery SQL recursively."""

  def __init__(
      self,
      value_set_codes_table: Optional[bigquery.TableReference] = None,
      value_set_codes_definitions: Optional[
          fhir_package.FhirPackageManager
      ] = None,
  ) -> None:
    """Creates a BigQuerySqlInterpreter.

    Args:
      value_set_codes_table: The name of the database table containing value set
        code definitions. Used when building SQL for memberOf expressions. If
        given, value set definitions needed for memberOf expressions will be
        retrieved from this table if they can not be found in
        `value_set_codes_definitions`. If neither this nor
        `value_set_codes_definitions` is given, no memberOf SQL will be
        generated.
      value_set_codes_definitions: A package manager containing value set
        definitions which can be used to build SQL for memberOf expressions.
        These value set definitions can be consulted in favor of using an
        external `value_set_codes_table`. If neither this nor
        `value_set_codes_definitions` is given, no memberOf SQL will be
        generated.
    """
    self._value_set_codes_table = value_set_codes_table
    self._value_set_codes_definitions = value_set_codes_definitions
    self._use_resource_alias = None

  def encode(
      self, builder: expressions.Builder, select_scalars_as_array: bool = True,
            use_resource_alias: bool = False) -> str:
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
      use_resource_alias: Determines whether it is necessary to call the
        resource table directly through an alias.

    Returns:
      A Standard SQL representation of the provided FHIRPath expression.
    """
    self._use_resource_alias = use_resource_alias
    result = self.visit(builder.node)
    if select_scalars_as_array or _fhir_path_data_types.returns_collection(
        builder.node.return_type
    ):
      return (
          f'ARRAY(SELECT {result.sql_alias}\n'
          f'FROM {result.to_subquery()}\n'
          f'WHERE {result.sql_alias} IS NOT NULL)'
      )
    else:
      # Parenthesize raw SELECT so it can plug in anywhere an expression can.
      return f'{result.to_subquery()}'

  def visit(
      self,
      node: _evaluation.ExpressionNode,
      use_resource_alias: Optional[bool] = None,
  ) -> Any:
    if use_resource_alias is not None:
      self._use_resource_alias = use_resource_alias
    return super().visit(node)

  def visit_root(
      self, root: _evaluation.RootMessageNode
  ) -> Optional[_sql_data_types.IdentifierSelect]:
    if self._use_resource_alias:
      return _sql_data_types.IdentifierSelect(
          _sql_data_types.Identifier(
              _escape_identifier(root.to_fhir_path()),
              _sql_data_types.OpaqueStruct,
          ),
          from_part=None,
      )
    return None

  def visit_reference(
      self, reference: _evaluation.ExpressionNode
  ) -> _sql_data_types.IdentifierSelect:
    # When $this is used, we need the last identifier from the operand.
    sql_alias = reference.to_fhir_path().split('.')[-1]
    # If the identifier is `$this`, we assume that the repeated field has been
    # unnested upstream so we only need to reference it with its alias:
    # `{}_element_`.
    if _fhir_path_data_types.returns_collection(reference.return_type):
      sql_alias = f'{sql_alias}_element_'

    sql_data_type = _sql_data_types.get_standard_sql_data_type(
        reference.return_type
    )
    return _sql_data_types.IdentifierSelect(
        select_part=_sql_data_types.Identifier(
            _escape_identifier(sql_alias), sql_data_type
        ),
        from_part=None,
    )

  def visit_literal(
      self, literal: _evaluation.LiteralNode
  ) -> _sql_data_types.RawExpression:
    """Translates a FHIRPath literal to Standard SQL."""

    if literal.return_type is None or isinstance(
        literal.return_type, _fhir_path_data_types.Empty.__class__
    ):  # pylint: disable=protected-access
      sql_value = 'NULL'
      sql_data_type = _sql_data_types.Undefined
    # TODO(b/244184211): Make _fhir_path_data_types.FhirPathDataType classes
    # public.
    elif isinstance(literal.return_type, _fhir_path_data_types._Boolean):  # pylint: disable=protected-access
      sql_value = str(literal).upper()
      sql_data_type = _sql_data_types.Boolean
    elif isinstance(literal.return_type, _fhir_path_data_types._Quantity):  # pylint: disable=protected-access
      # Since quantity string literals contain quotes, they are escaped.
      # E.g. '10 \'mg\''.
      quantity_quotes_escaped = str(literal).translate(
          str.maketrans({"'": r'\'', '"': r'\"'})
      )
      sql_value = f"'{quantity_quotes_escaped}'"
      sql_data_type = _sql_data_types.String
    elif isinstance(literal.return_type, _fhir_path_data_types._Integer):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Int64
    elif isinstance(literal.return_type, _fhir_path_data_types._Decimal):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Numeric
    elif isinstance(literal.return_type, _fhir_path_data_types._DateTime):  # pylint: disable=protected-access
      # Date and datetime literals start with an @ and need to be quoted.
      dt = _primitive_time_utils.get_date_time_value(literal.get_value())
      sql_value = f"'{dt.isoformat()}'"
      sql_data_type = _sql_data_types.Timestamp
    elif isinstance(literal.return_type, _fhir_path_data_types._Date):  # pylint: disable=protected-access
      dt = _primitive_time_utils.get_date_time_value(literal.get_value()).date()
      sql_value = f"'{str(dt)}'"
      sql_data_type = _sql_data_types.Date
    elif isinstance(literal.return_type, _fhir_path_data_types._String):  # pylint: disable=protected-access
      sql_value = str(literal)
      sql_data_type = _sql_data_types.String
    else:
      # LiteralNode constructor ensures that literal has to be one of the above
      # cases. But we error out here in case we enter an illegal state.
      raise ValueError(
          f'Unsupported literal value: {literal} {literal.return_type}.'
      )

    return _sql_data_types.RawExpression(
        _sql_data_types.wrap_time_types(sql_value, sql_data_type),
        _sql_data_type=sql_data_type,
        _sql_alias='literal_',
    )

  def visit_invoke_expression(
      self, identifier: _evaluation.InvokeExpressionNode
  ) -> _sql_data_types.IdentifierSelect:
    """Translates a FHIRPath member identifier to Standard SQL."""

    if identifier.identifier == '$this':
      return self.visit_reference(identifier.parent_node)

    raw_identifier_str = identifier.identifier
    parent_result = self.visit(identifier.parent_node)

    # Map to Standard SQL type. Note that we never map to a type of `ARRAY`,
    # as the member encoding flattens any `ARRAY` members.
    sql_data_type = _sql_data_types.get_standard_sql_data_type(
        identifier.return_type
    )
    sql_alias = raw_identifier_str
    identifier_str = raw_identifier_str
    if _fhir_path_data_types.is_collection(identifier.return_type):  # Array
      # If the identifier is `$this`, we assume that the repeated field has been
      # unnested upstream so we only need to reference it with its alias:
      # `{}_element_`.
      sql_alias = f'{sql_alias}_element_'
      if identifier.identifier == '$this':
        return _sql_data_types.IdentifierSelect(
            select_part=_sql_data_types.Identifier(sql_alias, sql_data_type),
            from_part=parent_result,
        )

      if parent_result:
        parent_identifier_str = parent_result.sql_alias
        identifier_str = f'{parent_identifier_str}.{raw_identifier_str}'
      else:
        # Identifiers need to be escaped if they are referenced directly.
        identifier_str = _escape_identifier(raw_identifier_str)

      from_part = (
          f'UNNEST({identifier_str}) AS {sql_alias} '
          'WITH OFFSET AS element_offset'
      )
      if parent_result:
        from_part = f'({parent_result}),\n{from_part}'
      # When UNNEST-ing a repeated field, we always generate an offset column
      # as well. If unused by the overall query, the expectation is that the
      # BigQuery query optimizer will be able to detect the unused column and
      # ignore it.
      return _sql_data_types.IdentifierSelect(
          select_part=_sql_data_types.Identifier(sql_alias, sql_data_type),
          from_part=from_part,
      )

    # Scalar
    select_part = _sql_data_types.Identifier(
        _escape_identifier(identifier_str), sql_data_type
    )
    if parent_result:
      # Append the current identifier to the path chain being selected if the
      # parent is not a ReferenceNode. If it is a ReferenceNode, we assume that
      # the parent has already been previously unnested.
      if not isinstance(identifier.parent_node, _evaluation.ReferenceNode):
        select_part = parent_result.select_part.dot(
            raw_identifier_str,
            sql_data_type,
            sql_alias=_escape_identifier(sql_alias),
        )
      return dataclasses.replace(parent_result, select_part=select_part)

    return _sql_data_types.IdentifierSelect(
        select_part=select_part,
        from_part=None,
    )

  def visit_invoke_reference(
      self, identifier: _evaluation.InvokeReferenceNode
  ) -> _sql_data_types.Select:
    reference_node = identifier.parent_node

    if not isinstance(
        reference_node.return_type,
        _fhir_path_data_types.ReferenceStructureDataType,
    ):
      raise ValueError(
          'visit_reference called on node with return type'
          f' {reference_node.return_type}.'
      )

    # Build a SELECT for the reference struct.
    parent_query = self.visit(reference_node)

    # Select the first non-null ID field on the struct. Validation
    # should ensure at most one of the fields is not null.
    type_names = (
        # Get the base resource type for structure definition URIs in
        # `target_profiles`.
        identifier.context.get_fhir_type_from_string(
            profile=reference, type_code=None, element_definition=None
        ).base_type
        for reference in reference_node.return_type.target_profiles
    )

    # If we have a parent query, append our record access against it.
    if parent_query is not None:
      prefix = f'{parent_query.select_part}.'
    else:
      prefix = ''

    # Build column names for each resource type, e.g. patientId, deviceId.
    column_names = (
        f'{prefix}{type_name[:1].lower()}{type_name[1:]}Id'
        for type_name in type_names
    )
    # Select the first non-null ID column by chaining IFNULL calls, e.g.
    # IFNULL("a", IFNULL("b", "c"))
    sql = functools.reduce(
        lambda acc, column: f'IFNULL({column}, {acc})', sorted(column_names)
    )
    select_part = _sql_data_types.RawExpression(
        sql, _sql_data_types.String, 'reference'
    )

    if parent_query is not None:
      return dataclasses.replace(parent_query, select_part=select_part)

    return _sql_data_types.Select(select_part=select_part, from_part=None)

  def visit_indexer(
      self, indexer: _evaluation.IndexerNode
  ) -> _sql_data_types.Select:
    """Translates a FHIRPath indexer expression to Standard SQL.

    Args:
      indexer: The `_Indexer` Expression node.

    Returns:
      A compiled Standard SQL expression.
    """
    collection_result = self.visit(indexer.collection)
    index_result = self.visit(indexer.index)

    # Intermediate indexed table subquery.
    indexed_collection = (
        'SELECT ROW_NUMBER() OVER() AS row_,\n'
        f'{collection_result.sql_alias}\n'
        f'FROM {collection_result.to_subquery()}'
    )

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
      self, arithmetic: _evaluation.ArithmeticNode
  ) -> _sql_data_types.Select:
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
    sql_data_type = _sql_data_types.coerce(
        lhs_result.sql_data_type, rhs_result.sql_data_type
    )

    # Extract the values of LHS and RHS to be used as scalar subqueries.
    lhs_subquery = lhs_result.as_operand()
    rhs_subquery = rhs_result.as_operand()

    # TODO(b/196238279): Handle <string> + <string> when either operand is
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
            sql_value, _sql_data_type=sql_data_type, _sql_alias=sql_alias
        ),
        from_part=None,
    )

  def visit_equality(
      self, equality: _evaluation.EqualityNode
  ) -> _sql_data_types.Select:
    """Returns `TRUE` if the left collection is equal/equivalent to the right.

    See more at: http://hl7.org/fhirpath/#equality.

    Args:
      equality: The `Equality` Expression node.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(equality.left)
    rhs_result = self.visit(equality.right)

    if (
        equality.op == _ast.EqualityRelation.Op.EQUAL
        or equality.op == _ast.EqualityRelation.Op.EQUIVALENT
    ):
      collection_check_func_name = 'NOT EXISTS'
      scalar_check_op = '='
    else:  # NOT_*
      collection_check_func_name = 'EXISTS'
      scalar_check_op = '!='

    sql_alias = 'eq_'
    sql_data_type = _sql_data_types.Boolean

    # Both sides are scalars.
    if _fhir_path_data_types.is_scalar(
        equality.left.return_type
    ) and _fhir_path_data_types.is_scalar(equality.right.return_type):
      # Use the simpler query.
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              (
                  f'({lhs_result.as_operand()} '
                  f'{scalar_check_op} '
                  f'{rhs_result.as_operand()})'
              ),
              _sql_data_type=sql_data_type,
              _sql_alias=sql_alias,
          ),
          from_part=None,
      )

    else:
      sql_expr = (
          'SELECT lhs_.*\n'
          'FROM (SELECT ROW_NUMBER() OVER() AS row_, '
          f'{lhs_result.sql_alias}\n'
          f'FROM {lhs_result.to_subquery()}) AS lhs_\n'
          'EXCEPT DISTINCT\n'
          'SELECT rhs_.*\n'
          'FROM (SELECT ROW_NUMBER() OVER() AS row_, '
          f'{rhs_result.sql_alias}\n'
          f'FROM {rhs_result.to_subquery()}) AS rhs_'
      )

      return _sql_data_types.Select(
          select_part=_sql_data_types.FunctionCall(
              collection_check_func_name,
              (
                  _sql_data_types.RawExpression(
                      sql_expr, _sql_data_type=_sql_data_types.Int64
                  ),
              ),
              _sql_data_type=sql_data_type,
              _sql_alias=sql_alias,
          ),
          from_part=None,
      )

  def visit_comparison(
      self, comparison: _evaluation.ComparisonNode
  ) -> _sql_data_types.Select:
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
            _sql_alias=sql_alias,
        ),
        from_part=None,
    )

  def visit_boolean_op(
      self, boolean_logic: _evaluation.BooleanOperatorNode
  ) -> _sql_data_types.Select:
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
            _sql_alias=sql_alias,
        ),
        from_part=None,
    )

  def visit_membership(
      self, relation: _evaluation.MembershipRelationNode
  ) -> Any:
    """Translates a FHIRPath membership relation to Standard SQL.

    For the `IN` relation, the LHS operand is assumed to be a collection of a
    single value. For 'CONTAINS', the RHS operand is assumed to be a collection
    of a single value.

    Args:
      relation: The FHIRPath AST `MembershipRelation` node.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(relation.left)
    rhs_result = self.visit(relation.right)

    # SELECT (<lhs>) IN(<rhs>) AS mem_
    # Where relation.op \in {IN, CONTAINS}; `CONTAINS` is the converse of `IN`
    in_lhs = (
        lhs_result if isinstance(relation, _evaluation.InNode) else rhs_result
    )
    in_rhs = (
        rhs_result if isinstance(relation, _evaluation.InNode) else lhs_result
    )

    sql_expr = f'({in_lhs.as_operand()})\nIN ({in_rhs.as_operand()})'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_expr,
            _sql_data_type=_sql_data_types.Boolean,
            _sql_alias='mem_',
        ),
        from_part=None,
    )

  def visit_union(
      self, union: _evaluation.UnionNode
  ) -> _sql_data_types.UnionExpression:
    lhs_result = self.visit(union.left)
    rhs_result = self.visit(union.right)

    # Supported in FHIRPath, but currently generates invalid Standard SQL.
    if isinstance(
        lhs_result.sql_data_type, _sql_data_types.Struct
    ) or isinstance(rhs_result.sql_data_type, _sql_data_types.Struct):
      raise ValueError(
          f'Unsupported `STRUCT` union between {lhs_result}, {rhs_result}.'
      )

    sql_alias = 'union_'
    lhs = _sql_data_types.Select(
        select_part=_sql_data_types.Identifier(
            ('lhs_', lhs_result.sql_alias),
            _sql_alias=sql_alias,
            _sql_data_type=lhs_result.sql_data_type,
        ),
        from_part=f'{lhs_result.to_subquery()} AS lhs_',
    )
    rhs = _sql_data_types.Select(
        select_part=_sql_data_types.Identifier(
            ('rhs_', rhs_result.sql_alias),
            _sql_alias=sql_alias,
            _sql_data_type=rhs_result.sql_data_type,
        ),
        from_part=f'{rhs_result.to_subquery()} AS rhs_',
    )
    return lhs.union(rhs, distinct=True)

  def visit_polarity(
      self, polarity: _evaluation.NumericPolarityNode
  ) -> _sql_data_types.Select:
    """Translates FHIRPath unary polarity (+/-) to Standard SQL."""
    operand_result = self.visit(polarity.parent_node)
    sql_expr = f'{polarity.op}{operand_result.as_operand()}'
    sql_alias = 'pol_'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_expr,
            _sql_data_type=operand_result.sql_data_type,
            _sql_alias=sql_alias,
        ),
        from_part=None,
    )

  def visit_function(self, function: _evaluation.FunctionNode) -> Any:
    """Translates a FHIRPath function to Standard SQL."""
    parent_result = self.visit(function.parent_node)
    params_result = [self.visit(p) for p in function.params()]
    if isinstance(
        function.parent_node.return_type,
        _fhir_path_data_types.PolymorphicDataType,
    ) and not _function_implementation_supports_polymorphic_type(function):
      return NotImplementedError(
          'TODO(b/271314993): Support polymorphic operand for'
          f' {function.__class__.__name__}.'
      )

    if isinstance(function, _evaluation.MemberOfFunction):
      kwargs = {}
      if self._value_set_codes_table is not None:
        kwargs['value_set_codes_table'] = str(self._value_set_codes_table)
      if self._value_set_codes_definitions is not None:
        kwargs['value_set_resolver'] = local_value_set_resolver.LocalResolver(
            self._value_set_codes_definitions
        )

      return _bigquery_sql_functions.FUNCTION_MAP[function.NAME](
          function, parent_result, params_result, **kwargs
      )
    func = _bigquery_sql_functions.FUNCTION_MAP.get(function.NAME)
    return func(function, parent_result, params_result)

  # TODO(b/208900793): Remove LOGICAL_AND(UNNEST) when the SQL generator
  # can return single values and it's safe to do so for non-repeated
  # fields.
  def wrap_where_expression(self, where_expression: str) -> str:
    return (
        '(SELECT LOGICAL_AND(logic_)\n'
        f'FROM UNNEST({where_expression}) AS logic_)'
    )

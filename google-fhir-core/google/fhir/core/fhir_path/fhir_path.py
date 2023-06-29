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
"""Functionality for manipulating FHIRPath expressions."""

import copy
import dataclasses
import decimal
from typing import Any, Iterable, Optional, Set, cast

from google.cloud import bigquery

from google.protobuf import message
from google.fhir.core.proto import fhirpath_replacement_list_pb2
from google.fhir.core import fhir_errors
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _fhir_path_to_sql_functions
from google.fhir.core.fhir_path import _navigation
from google.fhir.core.fhir_path import _semant
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import expressions
from google.fhir.core.fhir_path import fhir_path_options
from google.fhir.core.utils import fhir_package
from google.fhir.core.utils import proto_utils
from google.fhir.r4.terminology import local_value_set_resolver

# TODO(b/201107372): Update FHIR-agnostic types to a protocol.
StructureDefinition = message.Message
ElementDefinition = message.Message
Constraint = message.Message

# See more at: https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md
_PRIMITIVE_TO_STANDARD_SQL_MAP = {
    'base64Binary': _sql_data_types.String,
    'boolean': _sql_data_types.Boolean,
    'code': _sql_data_types.String,
    'date': _sql_data_types.String,
    'dateTime': _sql_data_types.String,
    'decimal': _sql_data_types.Numeric,
    'id': _sql_data_types.String,
    'instant': _sql_data_types.String,
    'integer': _sql_data_types.Int64,
    'markdown': _sql_data_types.String,
    'oid': _sql_data_types.String,
    'positiveInt': _sql_data_types.Int64,
    'string': _sql_data_types.String,
    'time': _sql_data_types.String,
    'unsignedInt': _sql_data_types.Int64,
    'uri': _sql_data_types.String,
    'xhtml': _sql_data_types.String,
}

# See more at:
# * https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md
# * https://www.hl7.org/fhir/fhirpath.html#types
_SYSTEM_PRIMITIVE_TO_STANDARD_SQL_MAP = {
    'http://hl7.org/fhirpath/System.Boolean': _sql_data_types.Boolean,
    'http://hl7.org/fhirpath/System.Date': _sql_data_types.String,
    'http://hl7.org/fhirpath/System.DateTime': _sql_data_types.String,
    'http://hl7.org/fhirpath/System.Decimal': _sql_data_types.Numeric,
    'http://hl7.org/fhirpath/System.Integer': _sql_data_types.Int64,
    'http://hl7.org/fhirpath/System.Quantity': _sql_data_types.OpaqueStruct,
    'http://hl7.org/fhirpath/System.String': _sql_data_types.String,
    'http://hl7.org/fhirpath/System.Time': _sql_data_types.String,
}

# Timestamp format to convert ISO strings into BigQuery Timestamp types.
_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%E*S%Ez'

# ISO format of dates used by FHIR.
_DATE_FORMAT = '%Y-%m-%d'


def _escape_identifier(identifier_value: str) -> str:
  """Returns the value surrounded by backticks if it is a keyword."""
  # Keywords are case-insensitive
  if identifier_value.upper() in _sql_data_types.STANDARD_SQL_KEYWORDS:
    return f'`{identifier_value}`'
  return identifier_value  # No-op


def _get_analytic_path(element_definition: ElementDefinition) -> str:
  """Returns the identifying dot-separated (`.`) analytic path of the element.

  The `analytic path` is:
  - If the given element is a slice on an extension, it returns the element id
    with the `extension` part discarded.
    (e.g: if slice element id is `Foo.extension:slice`, it returns `Foo.slice`)
  - Else, the element.path attribute.

  Args:
    element_definition: The element definition that we are operating on.
  """
  if _utils.is_slice_on_extension(element_definition):
    initial_path: str = cast(Any, element_definition).id.value
    return initial_path.replace('extension:', '')

  if not proto_utils.field_is_set(element_definition, 'path'):
    raise ValueError(
        f'Required field "path" is not set for {element_definition}.'
    )
  return cast(Any, element_definition).path.value


def _last_path_token(element_definition: ElementDefinition) -> str:
  """Returns `element_definition`'s last path token less the resource type.

  For example:
    * "Foo" returns "" (empty string)
    * "Foo.bar" returns "bar"
    * "Foo.bar.bats" returns "bats"

  Args:
    element_definition: The `ElementDefinition` whose relative path to return.
  """
  path = _get_analytic_path(element_definition)
  components_less_resource = path.split('.')[1:]
  return components_less_resource[-1] if components_less_resource else ''


def _is_type(element_definition: ElementDefinition, type_code: str) -> bool:
  """Returns `True` if `element_definition` is of type, `type_code`."""
  type_codes = _utils.element_type_codes(element_definition)
  if len(type_codes) != 1:
    return False
  return type_codes[0] == type_code


def _is_primitive_typecode(type_code: str) -> bool:
  """Returns True if the given typecode is primitive. False otherwise."""
  return (
      type_code in _PRIMITIVE_TO_STANDARD_SQL_MAP
      or
      # Ids are a special case of primitive that have their type code equal to
      # 'http://hl7.org/fhirpath/System.String'.
      type_code == 'http://hl7.org/fhirpath/System.String'
  )


@dataclasses.dataclass
class SqlGenerationOptions:
  """Used by FhirProfileStandardSqlEncoder to define optional settings.

  Attributes:
    skip_keys: A set of constraint keys that should be skipped during encoding.
    add_primitive_regexes: Whether or not to add constraints requiring primitive
      fields to match their corresponding regex.
    add_value_set_bindings: Whether or not to add constraints enforcing
      membership of codes in the value sets defined by the implementation guide
    expr_replace_list: A list that specifies fhir path expressions to be
      replaced. It also specifies what they should be replaced with.
    value_set_codes_table: The name of the database table containing value set
      code definitions. Used when building SQL for memberOf expressions.
    value_set_codes_definitions: A package manager containing value set
      definitions which can be used to build SQL for memberOf expressions. These
      value set definitions can be consulted in favor of using an external
      `value_set_codes_table`.
    verbose_error_reporting: If False, the error report will contain the
      exception message associated with the error. If True, it will contain the
      full stack trace for the exception.
  """

  skip_keys: Set[str] = dataclasses.field(default_factory=set)
  add_primitive_regexes: bool = False
  expr_replace_list: fhirpath_replacement_list_pb2.FHIRPathReplacementList = (
      dataclasses.field(
          default_factory=fhirpath_replacement_list_pb2.FHIRPathReplacementList
      )
  )
  add_value_set_bindings: bool = False
  value_set_codes_table: Optional[bigquery.TableReference] = None
  # TODO(b/269329295): collapse these definitions with the
  # structure_definitions passed to
  # FhirPathStandardSqlEncoder.__init__ in a single package manager.
  value_set_codes_definitions: Optional[fhir_package.FhirPackageManager] = None
  verbose_error_reporting: bool = False


class FhirPathStandardSqlEncoder(_ast.FhirPathAstBaseVisitor):
  """Encodes a FHIRPath Constraint into a Standard SQL expression."""

  def __init__(
      self,
      structure_definitions: Iterable[StructureDefinition],
      options: Optional[SqlGenerationOptions] = None,
      validation_options: Optional[
          fhir_path_options.SqlValidationOptions
      ] = None,
  ) -> None:
    """Creates a new instance of `FhirPathStandardSqlEncoder`.

    Args:
      structure_definitions: The list of `StructureDefinition`s comprising the
        FHIR resource "graph" for traversal and encoding of constraints.
      options: Optional settings for influencing SQL Generation.
      validation_options: Optional settings for influencing validation behavior.
    """
    self._env = _navigation._Environment(structure_definitions)
    self._options = options or SqlGenerationOptions()
    self._semantic_analyzer = _semant.FhirPathSemanticAnalyzer(
        self._env, validation_options=validation_options
    )

  # TODO(b/194290588): Perform recursive type inference on `STRUCT`s.
  def _get_standard_sql_data_type(
      self, element_definition: ElementDefinition
  ) -> _sql_data_types.StandardSqlDataType:
    """Return the Standard SQL data type describing the `ElementDefinition`.

    Complex resources are returned as `OpaqueStruct` instances (no visibility
    into field type).

    Args:
      element_definition: The `ElementDefinition` whose type to return.

    Returns:
      A Standard SQL data type describing the `ElementDefinition`.
    """
    type_codes = _utils.element_type_codes(element_definition)
    if len(type_codes) == 1:
      uri_value: str = type_codes[0]
      if uri_value in _PRIMITIVE_TO_STANDARD_SQL_MAP:
        return _PRIMITIVE_TO_STANDARD_SQL_MAP[uri_value]

      if uri_value in _SYSTEM_PRIMITIVE_TO_STANDARD_SQL_MAP:
        return _SYSTEM_PRIMITIVE_TO_STANDARD_SQL_MAP[uri_value]

    return _sql_data_types.OpaqueStruct  # Empty `STRUCT`

  def encode(
      self,
      *,
      structure_definition: StructureDefinition,
      fhir_path_expression: str,
      element_definition: Optional[ElementDefinition] = None,
      select_scalars_as_array: bool = True,
  ) -> str:
    """Returns a Standard SQL encoding of a FHIRPath expression.

    If select_scalars_as_array is True, the resulting Standard SQL encoding
    always returns a top-level `ARRAY`, whose elements are non-`NULL`. Otherwise
    the resulting SQL will attempt to return a scalar when possible and only
    return an `ARRAY` for actual collections.

    Args:
      structure_definition: The containing type of `element_definition`.
      fhir_path_expression: A fluent-style FHIRPath expression, e.g.:
        `foo.bar.exists()`.
      element_definition: The `ElementDefinition` that the
        `fhir_path_expression` is relative to. If this is None, the root element
        definition is used.
      select_scalars_as_array: When True, always builds SQL selecting results in
        an array. When False, attempts to build SQL returning scalars where
        possible.

    Returns:
      A Standard SQL representation of the provided FHIRPath expression.

    Raises:
      ValueError: In the event that the provided `input_str` was syntactically
        invalid FHIRPath that failed during lexing/parsing.
      TypeError: In the event that errors occur during semantic analysis.
        Meaning that the `input_str` was semantically invalid FHIRPath.
    """
    ast = _ast.build_fhir_path_ast(fhir_path_expression)

    if element_definition is None:
      element_definition = _utils.get_root_element_definition(
          structure_definition
      )

    semant_error_reporter = fhir_errors.ListErrorReporter()
    self._semantic_analyzer.add_semantic_annotations(
        ast,
        semant_error_reporter,
        structure_definition,
        element_definition,
    )
    if semant_error_reporter.errors:
      semantic_errors = '.\n'.join(semant_error_reporter.errors)
      raise TypeError(
          'Unexpected errors during semantic analysis:\n%s' % semantic_errors
      )

    if _ast.contains_reference_without_id_for(ast):
      raise TypeError(
          'The ast contains a resource type without a corresponding idFor call'
          ' to disambiguate the underlying database column. The FHIRPath for'
          ' resource types must contain an idFor call at present:'
          f' {ast.debug_string()}'
      )

    walker = _navigation.FhirStructureDefinitionWalker(
        self._env,
        structure_definition,
        element_definition,
    )
    result = self.visit(ast, walker=walker)

    if select_scalars_as_array or isinstance(
        ast.data_type, _fhir_path_data_types.Collection
    ):
      return (
          f'ARRAY(SELECT {result.sql_alias}\n'
          f'FROM {result.to_subquery()}\n'
          f'WHERE {result.sql_alias} IS NOT NULL)'
      )
    else:
      # Parenthesize raw SELECT so it can plug in anywhere an expression can.
      return f'{result.to_subquery()}'

  def validate(
      self,
      structure_definition: StructureDefinition,
      element_definition: ElementDefinition,
      fhir_path_expression: str,
  ) -> fhir_errors.ListErrorReporter:
    """Validates the given FHIR path expression.

    Validates a given FHIR path expression in the context of a structure
    definition and element definition.

    Args:
      structure_definition: The containing type of `element_definition`.
      element_definition: The `ElementDefinition` that the
        `fhir_path_expression` is relative to.
      fhir_path_expression: A fluent-style FHIRPath expression, e.g.:
        `foo.bar.exists()`.

    Returns:
      An error reporter that will be populated with any errors / warnings
      encountered.
    """
    error_reporter = fhir_errors.ListErrorReporter()

    try:
      ast = _ast.build_fhir_path_ast(fhir_path_expression)
      self._semantic_analyzer.add_semantic_annotations(
          ast,
          error_reporter,
          structure_definition,
          element_definition,
      )

    except ValueError as e:
      error_reporter.report_conversion_error(
          cast(Any, element_definition).path.value, str(e)
      )

    return error_reporter

  def visit_literal(
      self, literal: _ast.Literal, **unused_kwargs: Any
  ) -> _sql_data_types.RawExpression:
    """Translates a FHIRPath literal to Standard SQL."""

    if literal.value is None:
      sql_value = 'NULL'
      sql_data_type = _sql_data_types.Undefined
    elif isinstance(literal.value, bool):
      sql_value = str(literal).upper()
      sql_data_type = _sql_data_types.Boolean
    elif literal.is_date_type:
      # Unfortunately, _ast.Literal does not differentiate how the Timestamp was
      # given so it's nontrivial to parse the string correctly.
      sql_value = f"'{literal.value}'"
      sql_data_type = _sql_data_types.String
    elif isinstance(literal.value, str):
      sql_value = f"'{literal.value}'"  # Quote string literals for SQL
      sql_data_type = _sql_data_types.String
    elif isinstance(literal.value, _ast.Quantity):
      # Since quantity string literals contain quotes, they are escaped.
      # E.g. '10 \'mg\''.
      quantity_quotes_escaped = str(literal.value).translate(
          str.maketrans({"'": r'\'', '"': r'\"'})
      )
      sql_value = f"'{quantity_quotes_escaped}'"
      sql_data_type = _sql_data_types.String
    elif isinstance(literal.value, int):
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Int64
    elif isinstance(literal.value, decimal.Decimal):
      sql_value = str(literal)
      sql_data_type = _sql_data_types.Numeric
    else:
      # Semantic analysis ensures that literal has to be one of the above cases.
      # But we error out here in case we enter an illegal state.
      raise ValueError(f'Unsupported literal value: {literal}.')

    return _sql_data_types.RawExpression(
        sql_value,
        _sql_data_type=sql_data_type,
        _sql_alias='literal_',
    )

  def visit_identifier(
      self,
      identifier: _ast.Identifier,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.IdentifierSelect:
    """Translates a FHIRPath member identifier to Standard SQL."""
    # TODO(b/244184211): Handle "special" identifiers

    # Advance the message context.
    if identifier.value == '$this':
      # If the identifier string is `$this`, then we don't have to advance the
      # message context because `$this` is just a reference to the current
      # identifier.
      raw_identifier_str = _last_path_token(walker.element)
    else:
      walker.step(identifier.value)
      raw_identifier_str = identifier.value

    # Map to Standard SQL type. Note that we never map to a type of `ARRAY`,
    # as the member encoding flattens any `ARRAY` members.
    sql_data_type = self._get_standard_sql_data_type(walker.element)

    identifier_str = _escape_identifier(raw_identifier_str)
    if _utils.is_repeated_element(walker.element):  # Array
      # If the identifier is `$this`, we assume that the repeated field has been
      # unnested upstream so we only need to reference it with its alias:
      # `{}_element_`.
      if identifier.value == '$this':
        sql_alias = f'{raw_identifier_str}_element_'
        return _sql_data_types.IdentifierSelect(
            select_part=_sql_data_types.Identifier(sql_alias, sql_data_type),
            from_part=None,
        )
      else:
        sql_alias = f'{raw_identifier_str}_element_'
        # When UNNEST-ing a repeated field, we always generate an offset column
        # as well. If unused by the overall query, the expectation is that the
        # BigQuery query optimizer will be able to detect the unused column and
        # ignore it.
        return _sql_data_types.IdentifierSelect(
            select_part=_sql_data_types.Identifier(sql_alias, sql_data_type),
            from_part=f'UNNEST({identifier_str}) AS {sql_alias} '
            + 'WITH OFFSET AS element_offset',
        )
    else:  # Scalar
      return _sql_data_types.IdentifierSelect(
          select_part=_sql_data_types.Identifier(identifier_str, sql_data_type),
          from_part=None,
      )

  def visit_indexer(
      self,
      indexer: _ast.Indexer,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.Select:
    """Translates a FHIRPath indexer expression to Standard SQL.

    Args:
      indexer: The AST `_Indexer` node.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      A compiled Standard SQL expression.

    Raises:
      TypeError in the event that the `indexer.index` attribute is not  of type
      `Int64`.
    """
    collection_result = self.visit(indexer.collection, walker=copy.copy(walker))
    # Semantic analysis verifies that this is always an Integer.
    index_result = self.visit(indexer.index, walker=copy.copy(walker))

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
      self,
      arithmetic: _ast.Arithmetic,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.Select:
    """Translates a FHIRPath arithmetic expression to Standard SQL.

    Each operand is expected to be a collection of a single element. Both
    operands must be of the same type, or of compatible types according to the
    rules of implicit conversion.

    Args:
      arithmetic: The AST `_Arithmetic` node.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      A compiled Standard SQL expression.

    Raises:
      ValueError in the event that the generated Standard SQL represents an
      incompatible arithmetic expression.
    """
    lhs_result = self.visit(arithmetic.lhs, walker=copy.copy(walker))
    rhs_result = self.visit(arithmetic.rhs, walker=copy.copy(walker))
    sql_data_type = _sql_data_types.coerce(
        lhs_result.sql_data_type, rhs_result.sql_data_type
    )

    # Extract the values of LHS and RHS to be used as scalar subqueries.
    lhs_subquery = lhs_result.as_operand()
    rhs_subquery = rhs_result.as_operand()

    # TODO(b/196238279): Handle <string> + <string> when either operand is empty
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

  def visit_type_expression(
      self,
      type_expression: _ast.TypeExpression,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.StandardSqlExpression:
    raise NotImplementedError('`visit_type_expression` is not yet implemented.')

  # TODO(b/191895864): Equality relation against an empty collection will be
  # truth-y, which is problematic for equals, but not equivalent-to.
  # TODO(b/191896705): DateTimes are treated as `STRING`s in SQL; ensure
  # timezone of 'Z' is respected/treated as +00:00.
  # TODO(b/191895721): Verify equivalence order-dependence (documentation says
  # it is *not* order-dependent, but HL7 JS implementation *is*).
  def visit_equality(
      self,
      relation: _ast.EqualityRelation,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.Select:
    """Returns `TRUE` if the left collection is equal/equivalent to the right.

    See more at: http://hl7.org/fhirpath/#equality.

    Args:
      relation: The AST `EqualityRelation` node.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      A compiled Standard SQL expression.

    Raises:
      ValueError in the event that the generated Standard SQL represents an
      unsupported equality relation.
    """
    lhs_result = self.visit(relation.lhs, walker=copy.copy(walker))
    rhs_result = self.visit(relation.rhs, walker=copy.copy(walker))

    # Semantic analysis ensures that the lhs and rhs are either directly
    # comparable or implicitly comparable to each other.
    if (
        relation.op == _ast.EqualityRelation.Op.EQUAL
        or relation.op == _ast.EqualityRelation.Op.EQUIVALENT
    ):
      collection_check_func_name = 'NOT EXISTS'
      scalar_check_op = '='
    else:  # NOT_*
      collection_check_func_name = 'EXISTS'
      scalar_check_op = '!='

    sql_alias = 'eq_'
    sql_data_type = _sql_data_types.Boolean

    # Both sides are scalars.
    if not isinstance(
        relation.lhs.data_type, _fhir_path_data_types.Collection
    ) and not isinstance(
        relation.rhs.data_type, _fhir_path_data_types.Collection
    ):
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
      self,
      comparison: _ast.Comparison,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.Select:
    """Translates a FHIRPath comparison to Standard SQL.

    Each operand is expected to be a collection of a single element. Operands
    can be strings, integers, decimals, dates, datetimes, and times. Comparison
    will perform implicit conversion between applicable types.

    Args:
      comparison: The FHIRPath AST `Comparison` node.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      A compiled Standard SQL expression.

    Raises:
      TypeError: In the event that coercion fails between the operands, or that
      the resulting type is a `STRUCT`.
    """
    lhs_result = self.visit(comparison.lhs, walker=copy.copy(walker))
    rhs_result = self.visit(comparison.rhs, walker=copy.copy(walker))

    # TODO(b/196239030): Leverage semantic analysis type information to make
    # more nuanced decision (e.g. if Quantity, certain operations can be
    # supported).
    type_ = _sql_data_types.coerce(
        lhs_result.sql_data_type, rhs_result.sql_data_type
    )
    if isinstance(type_, _sql_data_types.Struct):
      raise TypeError(
          'Unsupported `STRUCT` logical comparison between '
          f'{lhs_result} {comparison.op} {rhs_result}.'
      )

    # Extract the values of LHS and RHS to be used as scalar subqueries.
    lhs_subquery = lhs_result.as_operand()
    rhs_subquery = rhs_result.as_operand()

    # A check in semantic analysis prevents us from reaching this code with
    # incompatable types.
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

  def visit_boolean_logic(
      self,
      boolean_logic: _ast.BooleanLogic,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.Select:
    """Translates a FHIRPath Boolean logic operation to Standard SQL.

    Note that evaluation for Boolean logic is only supported for Boolean
    operands of scalar cardinality.

    Args:
      boolean_logic: The FHIRPath AST `BooleanLogic` node.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      A compiled Standard SQL expression.

    Raises:
      TypeError: In the event that either operand does not evaluate to a `BOOL`.
    """
    lhs_result = self.visit(boolean_logic.lhs, walker=copy.copy(walker))
    rhs_result = self.visit(boolean_logic.rhs, walker=copy.copy(walker))

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
      self,
      relation: _ast.MembershipRelation,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.Select:
    """Translates a FHIRPath membership relation to Standard SQL.

    For the `IN` relation, the LHS operand is assumed to be a collection of a
    single value. For 'CONTAINS', the RHS operand is assumed to be a collection
    of a single value.

    Args:
      relation: The FHIRPath AST `MembershipRelation` node.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(relation.lhs, walker=copy.copy(walker))
    rhs_result = self.visit(relation.rhs, walker=copy.copy(walker))

    # SELECT (<lhs>) IN(<rhs>) AS mem_
    # Where relation.op \in {IN, CONTAINS}; `CONTAINS` is the converse of `IN`
    in_lhs = (
        lhs_result
        if relation.op == _ast.MembershipRelation.Op.IN
        else rhs_result
    )
    in_rhs = (
        rhs_result
        if relation.op == _ast.MembershipRelation.Op.IN
        else lhs_result
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
      self,
      union: _ast.UnionOp,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.UnionExpression:
    """Merge two collections into a single *distinct* collection.

    Args:
      union: The FHIRPath AST `UnionOp` node.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      A compiled Standard SQL expression.
    """
    lhs_result = self.visit(union.lhs, walker=copy.copy(walker))
    rhs_result = self.visit(union.rhs, walker=copy.copy(walker))

    # Supported in FHIRPath, but currently generates invalid Standard SQL.
    if isinstance(
        lhs_result.sql_data_type, _sql_data_types.Struct
    ) or isinstance(rhs_result.sql_data_type, _sql_data_types.Struct):
      raise TypeError(
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
      self,
      polarity: _ast.Polarity,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.Select:
    """Translates FHIRPath unary polarity (+/-) to Standard SQL."""
    operand_result = self.visit(polarity.operand, walker=walker)
    sql_expr = f'{polarity.op}{operand_result.as_operand()}'
    sql_alias = 'pol_'
    # For consistency with visit_polarity in FhirPathCompilerVisitor.
    if isinstance(polarity.operand, _ast.Literal):
      sql_alias = 'literal_'
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_expr,
            _sql_data_type=operand_result.sql_data_type,
            _sql_alias=sql_alias,
        ),
        from_part=None,
    )

  def visit_invocation(
      self,
      invocation: _ast.Invocation,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _sql_data_types.StandardSqlExpression:
    """Translates a FHIRPath invocation to Standard SQL."""
    # Function invocation
    if isinstance(invocation.rhs, _ast.Function):
      return self.visit_function(
          invocation.rhs, operand=invocation.lhs, walker=walker
      )

    # Member invocation
    # TODO(b/244184211): Most of the RHS encoding is redudant, since we need to
    # "stitch" it together with the LHS. Rework this.
    # As is, we need to call visit on both lhs and rhs to increment the walker.
    lhs_result = self.visit(invocation.lhs, walker=walker)
    rhs_result = self.visit(invocation.rhs, walker=walker)

    # RHS must always be an identifier. If repeated, then this is an ARRAY value
    # which needs to be "unpacked" to a table. Semantic analysis should error
    # out before this point if this is not the case.
    rhs_identifier = str(invocation.rhs)

    if _utils.is_repeated_element(walker.element):
      # When UNNEST-ing a repeated field, we always generate an offset column as
      # well. If unused by the overall query, the expectation is that the
      # BigQuery query optimizer will be able to detect the unused column and
      # ignore it.
      return _sql_data_types.IdentifierSelect(
          select_part=_sql_data_types.Identifier(
              rhs_result.sql_alias, rhs_result.sql_data_type
          ),
          from_part=(
              f'{lhs_result.to_subquery()},\n'
              f'UNNEST({lhs_result.sql_alias}.{rhs_identifier}) '
              f'AS {rhs_result.sql_alias} '
              # As mentioned
              'WITH OFFSET AS element_offset'
          ),
      )
    else:
      # Append the rhs to the path chain being selected.
      # Including the from & where clauses of the lhs.
      return dataclasses.replace(
          lhs_result,
          select_part=lhs_result.select_part.dot(
              rhs_identifier,
              rhs_result.sql_data_type,
              sql_alias=rhs_result.sql_alias,
          ),
      )

  def visit_function(
      self,
      function: _ast.Function,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
      operand: Optional[_ast.Expression] = None,
  ) -> _sql_data_types.Select:
    """Translates a FHIRPath function to Standard SQL."""
    # Encode the operand, if present, and potentially mutate the `ctx`
    operand_result = (
        self.visit(operand, walker=walker) if operand is not None else None
    )

    # Encode each parameter with a shallow-copy of `ctx`
    params_result = [
        self.visit(p, walker=copy.copy(walker)) for p in function.params
    ]

    # Semantic analysis should error out before here if an invalid function is
    # used.
    func = _fhir_path_to_sql_functions.FUNCTION_MAP.get(
        function.identifier.value
    )

    # If the function is ofType, propagate its chosen type to the walker.
    if function.identifier.value == _ast.Function.Name.OF_TYPE:
      walker.selected_choice_type = str(function.params[0])

    if function.identifier.value == _ast.Function.Name.MEMBER_OF:
      kwargs = {}
      if self._options.value_set_codes_table is not None:
        kwargs['value_set_codes_table'] = str(
            self._options.value_set_codes_table
        )
      if self._options.value_set_codes_definitions is not None:
        kwargs['value_set_resolver'] = local_value_set_resolver.LocalResolver(
            self._options.value_set_codes_definitions
        )

      return func(function, operand_result, params_result, **kwargs)
    else:
      return func(function, operand_result, params_result)

  # TODO(b/208900793): Remove LOGICAL_AND(UNNEST) when the SQL generator
  # can return single values and it's safe to do so for non-repeated
  # fields.
  def wrap_where_expression(self, where_expression: str) -> str:
    """Wraps where expression to take care of repeated fields."""
    return (
        '(SELECT LOGICAL_AND(logic_)\n'
        f'FROM UNNEST({where_expression}) AS logic_)'
    )


def wrap_datetime_sql(expr: expressions.Builder, raw_sql: str) -> str:
  """Wraps raw sql if the result is datetime."""
  # Dates and datetime types are stored as strings to preseve completeness
  # of the underlying data, but views converts to date and datetime types
  # for ease of use.

  node_type = expr.node.return_type

  # Use date format constants drawn from the FHIR Store export conventions
  # for simplicity. If users encounter different formats in practice, we
  # could allow these formats to be overridden when constructing the runner
  # or check the string format explicitily on each row.
  if node_type == _fhir_path_data_types.DateTime:
    raw_sql = f'PARSE_TIMESTAMP("{_TIMESTAMP_FORMAT}", {raw_sql})'
  elif node_type == _fhir_path_data_types.Date:
    raw_sql = f'PARSE_DATE("{_DATE_FORMAT}", {raw_sql})'

  return raw_sql

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
from typing import Any, Collection, Dict, List, Optional, Set, cast

from google.cloud import bigquery

from google.protobuf import message
from google.fhir.core.proto import fhirpath_replacement_list_pb2
from google.fhir.core.proto import validation_pb2
from google.fhir.core import codes
from google.fhir.core import fhir_errors
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _fhir_path_to_sql_functions
from google.fhir.core.fhir_path import _navigation
from google.fhir.core.fhir_path import _semant
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import fhir_path_options
from google.fhir.core.utils import proto_utils

# TODO: Update FHIR-agnostic types to a protocol.
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
    'instant': _sql_data_types.Timestamp,
    'integer': _sql_data_types.Int64,
    'markdown': _sql_data_types.String,
    'oid': _sql_data_types.String,
    'positiveInt': _sql_data_types.Int64,
    'string': _sql_data_types.String,
    'time': _sql_data_types.Time,
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
    'http://hl7.org/fhirpath/System.Time': _sql_data_types.Time,
}

# These primitives are excluded from regex encoding because at the point when
# our validation is called, they are already saved as their correct types.
_PRIMITIVES_EXCLUDED_FROM_REGEX_ENCODING = frozenset([
    'base64Binary',
    'boolean',
    'decimal',
    'integer',
    'xhtml',
])

# The `ElementDefinition.type.code` is a URL of the datatype or resource used
# for an element. References are URLs that are relative to:
# http://hl7.org/fhir/StructureDefinition.
#
# `ElementDefinition`s whose type codes overlap with this set will be silently
# skipped during profile traversal.
_SKIP_TYPE_CODES = frozenset([
    # TODO: Add support for traversing `targetProfile`s of a
    # `Reference` type.
    'Reference',

    # Ignore the Resource type. Because it can stand for any resource, it is
    # typed as a string in our protos. Thus we do not need to encode constraints
    # for it.
    'Resource',
])

# A list of fhir path constraint keys to skip.
_SKIP_KEYS = frozenset([
    # TODO: This constraint produces a regex that escapes
    # our string quotes.
    'eld-19',
    # TODO: Remove this key after we start taking profiles into
    # account when encoding constraints for fields.
    'comparator-matches-code-regex',
    # Ignore this constraint because it is only directed towards primitive
    # fields.
    'ele-1',
    # Ignore these constraints because they require verifying html.
    'txt-1',
    'txt-2',
    # Ignore these constraints because they are directed towards
    # `DomainResource.contained` which is not supported by the SQL-on-FHIR
    # standard. More on why at `semant._UNSUPPORTED_BASE_PATHS`.
    'dom-2',
    'dom-3',
    'dom-4',
    'dom-5',
    # Ignore this constraint because it is directed towards `Extension` fields
    # which are not propagated to our protos or tables.
    'ext-1',
])


@dataclasses.dataclass
class _RegexInfo:
  """A named tuple with information needed to make a regex constraint."""
  regex: str
  type_code: str


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
        f'Required field "path" is not set for {element_definition}.')
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


def _path_to_sql_column_name(path: str) -> str:
  """Given a path to an `ElementDefinition`, returns a SQL column name."""
  return path.lower().replace('.', '_')


def _key_to_sql_column_name(key: str) -> str:
  """Given a constraint key, returns a SQL column name."""
  return key.lower().replace('-', '_')


def _is_required(element_definition: ElementDefinition) -> bool:
  """Returns true if the given element_definition is required."""
  return cast(Any, element_definition).min.value > 0


def _is_disabled(element_definition: ElementDefinition) -> bool:
  """Returns true if the given element_definition is a disabled by a profile."""
  return cast(Any, element_definition).max.value == '0'


def _escape_fhir_path_identifier(identifier: str) -> str:
  if identifier in _fhir_path_data_types.RESERVED_FHIR_PATH_KEYWORDS:
    return f'`{identifier}`'
  return identifier


def _escape_fhir_path_invocation(invocation: str) -> str:
  """Returns the given fhir path invocation with reserved words escaped."""
  identifiers = invocation.split('.')
  return '.'.join([_escape_fhir_path_identifier(id_) for id_ in identifiers])


def _get_regex_from_element_type(type_: message.Message):
  """Returns regex from ElementDefinition.type if available."""
  for sub_type in cast(Any, type_):
    for extension in sub_type.extension:
      if (extension.url.value == 'http://hl7.org/fhir/StructureDefinition/regex'
         ):
        # Escape backslashes from regex.
        primitive_regex = extension.value.string_value.value.replace(
            '\\', '\\\\')
        # Make regex a full match in sql.
        primitive_regex = f'^({primitive_regex})$'
        # If we found the regex we can stop here.
        return primitive_regex

  return None


def _get_regex_from_structure(structure_definition: StructureDefinition,
                              type_code: str) -> Optional[str]:
  """Returns the regex in the given StructureDefinition if it exists."""
  for element in cast(Any, structure_definition).snapshot.element:
    if element.id.value == f'{type_code}.value':
      primitive_regex = _get_regex_from_element_type(element.type)

      if primitive_regex is not None:
        return primitive_regex

  return None


def _is_primitive_typecode(type_code: str) -> bool:
  """Returns True if the given typecode is primitive. False otherwise."""
  return (
      type_code in _PRIMITIVE_TO_STANDARD_SQL_MAP or
      # Ids are a special case of primitive that have their type code equal to
      # 'http://hl7.org/fhirpath/System.String'.
      type_code == 'http://hl7.org/fhirpath/System.String')


@dataclasses.dataclass
class State:
  """A named tuple for capturing position within a FHIR resource graph.

  For the root element in the resource graph, `containing_type` will contain the
  structure definition of that root element and `element` will contain the
  element definition (it is usually the first element definition in the
  structure definition) of that element.
  """
  element: ElementDefinition
  containing_type: StructureDefinition


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
  """
  skip_keys: Set[str] = dataclasses.field(default_factory=set)
  add_primitive_regexes: bool = False
  expr_replace_list: fhirpath_replacement_list_pb2.FHIRPathReplacementList = (
      fhirpath_replacement_list_pb2.FHIRPathReplacementList())
  add_value_set_bindings: bool = False
  value_set_codes_table: bigquery.TableReference = None


class FhirPathStandardSqlEncoder(_ast.FhirPathAstBaseVisitor):
  """Encodes a FHIRPath Constraint into a Standard SQL expression."""

  def __init__(
      self,
      structure_definitions: List[StructureDefinition],
      options: Optional[SqlGenerationOptions] = None,
      validation_options: Optional[
          fhir_path_options.SqlValidationOptions] = None,
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
        self._env, validation_options=validation_options)

  # TODO: Perform recursive type inference on `STRUCT`s.
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

  def encode(self,
             *,
             structure_definition: StructureDefinition,
             fhir_path_expression: str,
             element_definition: Optional[ElementDefinition] = None,
             select_scalars_as_array: bool = True) -> str:
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
          structure_definition)

    semant_error_reporter = fhir_errors.ListErrorReporter()
    self._semantic_analyzer.add_semantic_annotations(
        ast,
        semant_error_reporter,
        structure_definition,
        element_definition,
    )
    if semant_error_reporter.errors:
      semantic_errors = '.\n'.join(semant_error_reporter.errors)
      raise TypeError('Unexpected errors during semantic analysis:\n%s' %
                      semantic_errors)

    walker = _navigation.FhirStructureDefinitionWalker(
        self._env,
        structure_definition,
        element_definition,
    )
    result = self.visit(ast, walker=walker)

    if select_scalars_as_array or isinstance(ast.data_type,
                                             _fhir_path_data_types.Collection):
      return (f'ARRAY(SELECT {result.sql_alias}\n'
              f'FROM {result.to_subquery()}\n'
              f'WHERE {result.sql_alias} IS NOT NULL)')
    else:
      # Parenthesize raw SELECT so it can plug in anywhere an expression can.
      return f'({result})'

  def validate(self, structure_definition: StructureDefinition,
               element_definition: ElementDefinition,
               fhir_path_expression: str) -> fhir_errors.ListErrorReporter:
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
          cast(Any, element_definition).path.value, str(e))

    return error_reporter

  def visit_literal(self, literal: _ast.Literal,
                    **unused_kwargs: Any) -> _sql_data_types.RawExpression:
    """Translates a FHIRPath literal to Standard SQL."""

    if literal.value is None:
      sql_value = 'NULL'
      sql_data_type = _sql_data_types.Undefined
    elif isinstance(literal.value, bool):
      sql_value = str(literal).upper()
      sql_data_type = _sql_data_types.Boolean
    elif isinstance(literal.value, (str, _ast.Quantity)):
      sql_value = f"'{literal.value}'"  # Quote string literals for SQL
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
      self, identifier: _ast.Identifier, *,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _sql_data_types.IdentifierSelect:
    """Translates a FHIRPath member identifier to Standard SQL."""
    # TODO: Handle "special" identifiers

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
            from_part=f'UNNEST({identifier_str}) AS {sql_alias} ' +
            'WITH OFFSET AS element_offset',
        )
    else:  # Scalar
      return _sql_data_types.IdentifierSelect(
          select_part=_sql_data_types.Identifier(identifier_str, sql_data_type),
          from_part=None,
      )

  def visit_indexer(
      self, indexer: _ast.Indexer, *,
      walker: _navigation.FhirStructureDefinitionWalker
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
      self, arithmetic: _ast.Arithmetic, *,
      walker: _navigation.FhirStructureDefinitionWalker
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
    sql_data_type = _sql_data_types.coerce(lhs_result.sql_data_type,
                                           rhs_result.sql_data_type)

    # Extract the values of LHS and RHS to be used as scalar subqueries.
    lhs_subquery = lhs_result.as_operand()
    rhs_subquery = rhs_result.as_operand()

    # TODO: Handle <string> + <string> when either operand is empty
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

  def visit_type_expression(
      self, type_expression: _ast.TypeExpression, *,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _sql_data_types.StandardSqlExpression:
    raise NotImplementedError('`visit_type_expression` is not yet implemented.')

  # TODO: Equality relation against an empty collection will be
  # truth-y, which is problematic for equals, but not equivalent-to.
  # TODO: DateTimes are treated as `STRING`s in SQL; ensure
  # timezone of 'Z' is respected/treated as +00:00.
  # TODO: Verify equivalence order-dependence (documentation says
  # it is *not* order-dependent, but HL7 JS implementation *is*).
  def visit_equality(
      self, relation: _ast.EqualityRelation, *,
      walker: _navigation.FhirStructureDefinitionWalker
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
    if (relation.op == _ast.EqualityRelation.Op.EQUAL or
        relation.op == _ast.EqualityRelation.Op.EQUIVALENT):
      collection_check_func_name = 'NOT EXISTS'
      scalar_check_op = '='
    else:  # NOT_*
      collection_check_func_name = 'EXISTS'
      scalar_check_op = '!='

    sql_alias = 'eq_'
    sql_data_type = _sql_data_types.Boolean

    # Both sides are scalars.
    if (not isinstance(relation.lhs.data_type, _fhir_path_data_types.Collection)
        and not isinstance(relation.rhs.data_type,
                           _fhir_path_data_types.Collection)):
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
      self, comparison: _ast.Comparison, *,
      walker: _navigation.FhirStructureDefinitionWalker
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

    # TODO: Leverage semantic analysis type information to make
    # more nuanced decision (e.g. if Quantity, certain operations can be
    # supported).
    type_ = _sql_data_types.coerce(lhs_result.sql_data_type,
                                   rhs_result.sql_data_type)
    if isinstance(type_, _sql_data_types.Struct):
      raise TypeError('Unsupported `STRUCT` logical comparison between '
                      f'{lhs_result} {comparison.op} {rhs_result}.')

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
            _sql_alias=sql_alias),
        from_part=None)

  def visit_boolean_logic(
      self, boolean_logic: _ast.BooleanLogic, *,
      walker: _navigation.FhirStructureDefinitionWalker
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
      lhs_result = _sql_data_types.RawExpression(
          sql_expr=f'(SELECT {lhs_result.sql_alias} IS NOT NULL FROM {lhs_result.to_subquery()})',
          _sql_data_type=_sql_data_types.Boolean,
          _sql_alias=lhs_result.sql_alias)
    if rhs_result.sql_data_type != _sql_data_types.Boolean:
      rhs_result = _sql_data_types.RawExpression(
          sql_expr=f'(SELECT {rhs_result.sql_alias} IS NOT NULL FROM {rhs_result.to_subquery()})',
          _sql_data_type=_sql_data_types.Boolean,
          _sql_alias=rhs_result.sql_alias)

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

  def visit_membership(
      self, relation: _ast.MembershipRelation, *,
      walker: _navigation.FhirStructureDefinitionWalker
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
        if relation.op == _ast.MembershipRelation.Op.IN else rhs_result)
    in_rhs = (
        rhs_result
        if relation.op == _ast.MembershipRelation.Op.IN else lhs_result)

    sql_expr = (f'({in_lhs.as_operand()})\n' f'IN ({in_rhs.as_operand()})')
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            sql_expr,
            _sql_data_type=_sql_data_types.Boolean,
            _sql_alias='mem_',
        ),
        from_part=None)

  def visit_union(
      self, union: _ast.UnionOp, *,
      walker: _navigation.FhirStructureDefinitionWalker
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
    if (isinstance(lhs_result.sql_data_type, _sql_data_types.Struct) or
        isinstance(rhs_result.sql_data_type, _sql_data_types.Struct)):
      raise TypeError(
          f'Unsupported `STRUCT` union between {lhs_result}, {rhs_result}.')

    sql_alias = 'union_'
    lhs = _sql_data_types.Select(
        select_part=_sql_data_types.Identifier(
            ('lhs_', lhs_result.sql_alias),
            _sql_alias=sql_alias,
            _sql_data_type=lhs_result.sql_data_type),
        from_part=f'{lhs_result.to_subquery()} AS lhs_')
    rhs = _sql_data_types.Select(
        select_part=_sql_data_types.Identifier(
            ('rhs_', rhs_result.sql_alias),
            _sql_alias=sql_alias,
            _sql_data_type=rhs_result.sql_data_type),
        from_part=f'{rhs_result.to_subquery()} AS rhs_',
    )
    return lhs.union(rhs, distinct=True)

  def visit_polarity(
      self, polarity: _ast.Polarity, *,
      walker: _navigation.FhirStructureDefinitionWalker
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
        from_part=None)

  def visit_invocation(
      self, invocation: _ast.Invocation, *,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _sql_data_types.StandardSqlExpression:
    """Translates a FHIRPath invocation to Standard SQL."""
    # Function invocation
    if isinstance(invocation.rhs, _ast.Function):
      return self.visit_function(
          invocation.rhs, operand=invocation.lhs, walker=walker)

    # Member invocation
    # TODO: Most of the RHS encoding is redudant, since we need to
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
          select_part=_sql_data_types.Identifier(rhs_result.sql_alias,
                                                 rhs_result.sql_data_type),
          from_part=(
              f'{lhs_result.to_subquery()},\n'
              f'UNNEST({lhs_result.sql_alias}.{rhs_identifier}) '
              f'AS {rhs_result.sql_alias} '
              # As mentioned
              'WITH OFFSET AS element_offset'),
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
          ))

  def visit_function(
      self,
      function: _ast.Function,
      *,
      walker: _navigation.FhirStructureDefinitionWalker,
      operand: Optional[_ast.Expression] = None) -> _sql_data_types.Select:
    """Translates a FHIRPath function to Standard SQL."""
    # Encode the operand, if present, and potentially mutate the `ctx`
    operand_result = (
        self.visit(operand, walker=walker) if operand is not None else None)

    # Encode each parameter with a shallow-copy of `ctx`
    params_result = [
        self.visit(p, walker=copy.copy(walker)) for p in function.params
    ]

    # Semantic analysis should error out before here if an invalid function is
    # used.
    func = _fhir_path_to_sql_functions.FUNCTION_MAP.get(
        function.identifier.value)

    # If the function is ofType, propagate its chosen type to the walker.
    if function.identifier.value == _ast.Function.Name.OF_TYPE:
      walker.selected_choice_type = str(function.params[0])

    if function.identifier.value == _ast.Function.Name.MEMBER_OF:
      kwargs = {}
      if self._options.value_set_codes_table is not None:
        kwargs['value_set_codes_table'] = str(
            self._options.value_set_codes_table)
      return func(function, operand_result, params_result, **kwargs)
    else:
      return func(function, operand_result, params_result)


class FhirProfileStandardSqlEncoder:
  """Standard SQL encoding of a `StructureDefinition`'s FHIRPath constraints.

  The encoder performs a pre-order recursive walk of a
  [FHIRProfile](https://www.hl7.org/fhir/profiling.html) represented as a
  [StructureDefinition](http://www.hl7.org/fhir/structuredefinition.html)
  protobuf message and translates its [FHIRPath](http://hl7.org/fhirpath/)
  constraints to a list of equivalent BigQuery Standard SQL expressions.

  Constraints encoded directly on the FHIRProfile as well as "transitory"
  constraints (e.g. constraints defined on types present as fields in the
  FHIRProfile under consideration) are encoded. If a field is un-set in a
  profile, the corresponding transitory constraints are considered vacuously-
  satsified, and the Standard SQL expression translations will produce `NULL` at
  runtime.

  All direct and transitory FHIRPath constraint Standard SQL expression
  encodings are returned as a list by the outer recursive walk over each profle.
  The caller can then join them into a `SELECT` clause, or perform further
  manipulation.
  """

  def __init__(
      self,
      structure_definitions: List[StructureDefinition],
      error_reporter: fhir_errors.ErrorReporter,
      *,
      options: Optional[SqlGenerationOptions] = None,
      validation_options: Optional[
          fhir_path_options.SqlValidationOptions] = None,
  ) -> None:
    """Creates a new instance of `FhirProfileStandardSqlEncoder`.

    Args:
      structure_definitions: The list of `StructureDefinition`s comprising the
        FHIR resource "graph" for traversal and encoding of constraints.
      error_reporter: A `fhir_errors.ErrorReporter` delegate for error-handling.
      options: Defines a list of optional settings that can be used to customize
        the behaviour of FhirProfileStandardSqlEncoder.
      validation_options: Optional settings for influencing validation behavior.
    """
    # Persistent state provided during initialization that the profile encoder
    # uses for navigation, error reporting, configuration, etc.
    self._env = _navigation._Environment(structure_definitions)
    self._error_reporter = error_reporter
    self._options = options or SqlGenerationOptions()
    self._fhir_path_encoder = FhirPathStandardSqlEncoder(
        structure_definitions,
        options=self._options,
        validation_options=validation_options)
    # Add keys that currently cause issues internally.
    self._options.skip_keys.update(_SKIP_KEYS)

    # Ephemeral state that is guaranteed to be cleaned-up between invocations
    # of `encode`.
    self._ctx: List[State] = []
    self._in_progress: Set[str] = set()
    self._requirement_column_names: Set[str] = set()
    self._element_id_to_regex_map: Dict[str, _RegexInfo] = {}
    self._regex_columns_generated = set()

  def _abs_path_invocation(self) -> str:
    """Returns the absolute path invocation given the traversal context."""
    if not self._ctx:
      return ''

    bottom = self._ctx[0]
    root_path = _get_analytic_path(bottom.element)
    path_components = [_last_path_token(s.element) for s in self._ctx[1:]]
    return '.'.join([root_path] + [c for c in path_components if c])

  def _encode_fhir_path_expression(self,
                                   structure_definition: StructureDefinition,
                                   element_definition: ElementDefinition,
                                   fhir_path_expression: str) -> Optional[str]:
    """Returns a Standard SQL translation of `fhir_path_expression`.

    If an error is encountered during encoding, the associated error reporter
    will be notified, and this method will return `None`.

    Args:
      structure_definition: The `StructureDefinition` containing the provided
        `element_definition` that the expression is defined with respect to.
      element_definition: The `ElementDefinition` that `fhir_path_expression` is
        defined with respect to.
      fhir_path_expression: The fluent-style dot-delimited ('.') FHIRPath
        expression to encode to Standard SQL.

    Returns:
      A Standard SQL encoding of `fhir_path_expression` upon successful
      completion.
    """
    try:
      sql_expression = self._fhir_path_encoder.encode(
          structure_definition=structure_definition,
          element_definition=element_definition,
          fhir_path_expression=fhir_path_expression)
    # Delegate all FHIRPath encoding errors to the associated `ErrorReporter`
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(),
          fhir_path_expression,
          str(e),
      )
      return None

    # Check to see if `fhir_path_expression` is a top-level constraint or a
    # transitive constraint. If top-level, simply return `sql_expression`.
    # If transitive, we need to add a supporting context query. This is
    # accomplished by a separate call to the `_FhirPathStandardSqlEncoder`,
    # passing the relative path invocation as a synthetic FHIRPath query that
    # should be executed from the `bottom` root element.
    # We determine if this is a top-level constraint by checking if
    # fhir_path_expression` is defined relative to the bottom root element.
    bottom = self._ctx[0]
    bottom_root_element = self._env.get_root_element_for(bottom.containing_type)
    if bottom_root_element is None:
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(), fhir_path_expression,
          'No root element definition for: '
          f'{cast(Any, bottom.containing_type).url.value}.')
      return None

    if bottom_root_element == element_definition:
      return sql_expression

    path_invocation = _escape_fhir_path_invocation(self._abs_path_invocation())
    path_invocation_less_resource = '.'.join(path_invocation.split('.')[1:])
    try:
      root_sql_expression = self._fhir_path_encoder.encode(
          structure_definition=bottom.containing_type,
          element_definition=bottom_root_element,
          fhir_path_expression=path_invocation_less_resource)
    # Delegate all FHIRPath encoding errors to the associated `ErrorReporter`
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(),
          fhir_path_expression,
          str(e),
      )
      return None

    # Bind the two expressions together via a correlated `ARRAY` subquery
    sql_expression = ('ARRAY(SELECT result_\n'
                      f'FROM (SELECT {sql_expression} AS subquery_\n'
                      'FROM (SELECT AS VALUE ctx_element_\n'
                      f'FROM UNNEST({root_sql_expression}) AS ctx_element_)),\n'
                      'UNNEST(subquery_) AS result_)')
    return sql_expression

  def _encode_constraints(
      self, structure_definition: StructureDefinition,
      element_definition: ElementDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of `SqlRequirement`s for FHIRPath constraints.

    Args:
      structure_definition: The enclosing `StructureDefinition`.
      element_definition: The `ElementDefinition` whose constraints should be
        encoded.

    Returns:
      A list of `SqlRequirement`s expressing FHIRPath constraints defined on the
      `element_definition`.
    """
    result: List[validation_pb2.SqlRequirement] = []
    constraints: List[Constraint] = (cast(Any, element_definition).constraint)
    for constraint in constraints:
      constraint_key: str = cast(Any, constraint).key.value
      if constraint_key in self._options.skip_keys:
        continue

      # Metadata for the requirement
      fhir_path_expression: str = cast(Any, constraint).expression.value
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(constraint_key)
      column_name_base: str = _path_to_sql_column_name(element_definition_path)
      column_name = f'{column_name_base}_{constraint_key_column_name}'

      if column_name in self._requirement_column_names:
        self._error_reporter.report_fhir_path_error(
            element_definition_path, fhir_path_expression,
            f'Duplicate FHIRPath requirement: {column_name}.')
        continue

      if cast(Any, constraint).severity.value == 0:
        self._error_reporter.report_fhir_path_error(
            element_definition_path, fhir_path_expression,
            'Constraint severity must be set.')
        continue  # Malformed constraint

      # TODO: Remove this implementation when a better
      # implementation at the FhirPackage level has been added.
      # Replace fhir_path_expression if needed. This functionality is mainly for
      # temporary replacements of invalid expressions defined in the spec while
      # we wait for the spec to be updated.
      if self._options.expr_replace_list:
        for replacement in self._options.expr_replace_list.replacement:
          if ((not replacement.element_path or
               replacement.element_path == element_definition_path) and
              replacement.expression_to_replace == fhir_path_expression):
            fhir_path_expression = replacement.replacement_expression

      # Create Standard SQL expression
      sql_expression = self._encode_fhir_path_expression(
          structure_definition,
          element_definition,
          fhir_path_expression,
      )
      if sql_expression is None:
        continue  # Failure to generate Standard SQL expression

      # Constraint type and severity metadata; default to WARNING
      # TODO: Cleanup validation severity mapping
      type_ = validation_pb2.ValidationType.VALIDATION_TYPE_FHIR_PATH_CONSTRAINT
      severity = cast(Any, constraint).severity
      severity_value_field = severity.DESCRIPTOR.fields_by_name.get('value')
      severity_str = codes.enum_value_descriptor_to_code_string(
          severity_value_field.enum_type.values_by_number[severity.value])
      try:
        validation_severity = validation_pb2.ValidationSeverity.Value(
            f'SEVERITY_{severity_str.upper()}')
      except ValueError:
        self._error_reporter.report_fhir_path_warning(
            element_definition_path, fhir_path_expression,
            f'Unknown validation severity conversion: {severity_str}.')
        validation_severity = validation_pb2.ValidationSeverity.SEVERITY_WARNING

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=sql_expression,
          severity=validation_severity,
          type=type_,
          element_path=element_definition_path,
          description=cast(Any, constraint).human.value,
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_expression,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              fhir_path_expression))

      self._requirement_column_names.add(column_name)
      result.append(requirement)

    return result

  # TODO: Handle general cardinality requirements.
  def _encode_required_fields(
      self,
      structure_definition: message.Message,
      element_definition: message.Message,
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns `SqlRequirement`s for all required fields in `ElementDefinition`.

    Args:
      structure_definition: The enclosing `StructureDefinition`.
      element_definition: The element to encode required fields for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      required fields on the element.
    """

    # If this is an extension, we don't want to access its children/fields.
    # TODO: Add support for complex extensions and the fields
    # inside them.
    if cast(Any, structure_definition).type.value == 'Extension':
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []
    children = self._env.get_children(structure_definition, element_definition)
    for child in children:

      # This allows us to encode required fields on slices of extensions while
      # filtering out slices on non-extensions.
      # TODO: Properly handle slices that are not slices on
      # extensions.
      if (_utils.is_slice_element(child) and
          not _utils.is_slice_on_extension(child)):
        continue

      min_size = cast(Any, child).min.value
      max_size = cast(Any, child).max.value
      relative_path = _last_path_token(child)
      element_count = f'{_escape_fhir_path_invocation(relative_path)}.count()'

      query_list = []

      if _utils.is_repeated_element(child) and max_size.isdigit():
        query_list.append(f'{element_count} <= {max_size}')

      if min_size == 1:
        query_list.append(
            f'{_escape_fhir_path_invocation(relative_path)}.exists()')
      elif min_size > 0:
        query_list.append(f'{min_size} <= {element_count}')

      if not query_list:
        continue

      constraint_key = f'{relative_path}-cardinality-is-valid'
      description = (f'The length of {relative_path} must be maximum '
                     f'{max_size} and minimum {min_size}.')

      fhir_path_expression = ' and '.join(query_list)

      if constraint_key in self._options.skip_keys:
        continue  # Allows users to skip required field constraints.

      # Early-exit if any types overlap with `_SKIP_TYPE_CODES`.
      type_codes = _utils.element_type_codes(child)
      if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
        continue

      required_sql_expression = self._encode_fhir_path_expression(
          structure_definition, element_definition, fhir_path_expression)
      if required_sql_expression is None:
        continue  # Failure to generate Standard SQL expression.

      # Create the `SqlRequirement`.
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(
          _path_to_sql_column_name(constraint_key))
      column_name_base: str = _path_to_sql_column_name(
          self._abs_path_invocation())
      column_name = f'{column_name_base}_{constraint_key_column_name}'

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=required_sql_expression,
          severity=(validation_pb2.ValidationSeverity.SEVERITY_ERROR),
          type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
          element_path=element_definition_path,
          description=description,
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_expression,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              fhir_path_expression))
      encoded_requirements.append(requirement)
    return encoded_requirements

  def get_extension_value_element(
      self, structure_definition: StructureDefinition,
      element_definition: ElementDefinition) -> Optional[ElementDefinition]:
    """Returns the value element of the given extension structure/ element pair.

    Args:
      structure_definition: The structure_definition of that extension.
      element_definition: The root element_definition of that extension.

    Returns:
      The value element of the given structure definition and root element
      pair. If a value element cannot be found, returns None.
    """
    children = self._env.get_children(structure_definition, element_definition)

    for child in children:
      base_path = cast(Any, child).base.path.value
      # Extract value element.
      if base_path == 'Extension.value[x]':
        return child

    return None

  def get_type_codes_from_slice_element(
      self, element_definition: ElementDefinition) -> List[str]:
    """Returns the type codes of slice elements."""

    element_definition_path = _get_analytic_path(element_definition)

    # This function currently only supports getting type codes from slices on
    # extensions.
    if not _utils.is_slice_on_extension(element_definition):
      self._error_reporter.report_conversion_error(
          element_definition_path,
          'Attempted to get type code from slice of non-extension.'
          ' Which is not supported.')

    urls = _utils.slice_element_urls(element_definition)
    # TODO: Handle choice types.
    if not urls:
      raise ValueError('Unable to get url for slice on extension with id: '
                       f'{_get_analytic_path(element_definition)}')

    if len(urls) > 1:
      raise ValueError('Expected element with only one url but got: '
                       f'{urls}, is this a choice type?')

    url = urls[0]
    containing_type = self._env.get_structure_definition_for(url)
    if containing_type is None:
      self._error_reporter.report_conversion_error(
          element_definition_path,
          f'Unable to find `StructureDefinition` for: {url}.')

    root_element = self._env.get_root_element_for(containing_type)
    if root_element is None:
      self._error_reporter.report_conversion_error(
          element_definition_path,
          f'Unable to find root `ElementDefinition` for: {url}.')

    value_element = self.get_extension_value_element(containing_type,
                                                     root_element)

    if value_element is None or _is_disabled(value_element):
      # At this point, the current element is a slice on an extension that has
      # no valid `Extension.value[x]` element, so we assume it is a complex
      # extension.
      # TODO: Handle complex extensions.
      return []
    else:
      return _utils.element_type_codes(value_element)

  # TODO: Move important ElementDefinition (and other) functions
  # to their respective utility modules and unit test their public facing apis .
  def _get_regex_from_element(
      self, element_definition: ElementDefinition) -> Optional[_RegexInfo]:
    """Returns the regex of this element_definition if available."""

    type_codes = _utils.element_type_codes(element_definition)

    if _utils.is_slice_on_extension(element_definition):
      type_codes = self.get_type_codes_from_slice_element(element_definition)

    if not type_codes:
      return None
    if len(type_codes) > 1:
      raise ValueError('Expected element with only one type code but got: '
                       f'{type_codes}, is this a choice type?')
    current_type_code = type_codes[0]

    element_id: str = cast(Any, element_definition).id.value
    # TODO: Look more into how this section handles multithreading.
    # If we have memoised the regex of this element, then just return it.
    if element_id in self._element_id_to_regex_map:
      return self._element_id_to_regex_map[element_id]

    # Ignore regexes on primitive types that are not represented as strings.
    if (current_type_code == 'positiveInt' or
        current_type_code == 'unsignedInt'):
      return _RegexInfo(regex='', type_code=current_type_code)

    # TODO Remove this after we figure out a better way to encode
    # primitive regex constraints for id fields.
    # If the current element_definition ends with `.id` and it's type_code is
    # `http://hl7.org/fhirpath/System.String`, then assume it is an `id` type.
    # We only care about ids that are direct children of a resource
    # E.g. `Foo.id` and not `Foo.bar.id`. These ids will have a base path of
    # `Resource.id`.
    base_path: str = cast(Any, element_definition).base.path.value
    if (base_path == 'Resource.id' and
        current_type_code == 'http://hl7.org/fhirpath/System.String'):
      current_type_code = 'id'

    # If the current_type_code is non primitive we filter it out here.
    if (current_type_code in _PRIMITIVE_TO_STANDARD_SQL_MAP and
        current_type_code not in _PRIMITIVES_EXCLUDED_FROM_REGEX_ENCODING):
      primitive_url = _utils.get_absolute_uri_for_structure(current_type_code)

      # If we have not memoised it, then extract it from its
      # `StructureDefinition`.
      type_definition = self._env.get_structure_definition_for(primitive_url)
      regex_value = _get_regex_from_structure(type_definition,
                                              current_type_code)
      if regex_value is None:
        self._error_reporter.report_validation_error(
            self._abs_path_invocation(), 'Unable to find regex pattern for; '
            f'type_code:`{current_type_code}` '
            f'and url:`{primitive_url}` in environment.')
      else:
        # Memoise the regex of this element for quick retrieval
        # later.
        regex_info = _RegexInfo(regex_value, current_type_code)
        self._element_id_to_regex_map[element_id] = regex_info
        return regex_info

    return None

  def _encode_primitive_regexes(
      self, structure_definition: message.Message,
      element_definition: ElementDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns regex `SqlRequirement`s for primitives in `ElementDefinition`.

    This function generates regex `SqlRequirement`s specifically for the direct
    child elements of the given `element_definition`.

    Args:
      structure_definition: The enclosing `StructureDefinition`.
      element_definition: The `ElementDefinition` to encode primitive regexes
        for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      primitive fields on the element that have regexes .
    """

    element_definition_path = self._abs_path_invocation()
    # TODO: Remove this key after we start taking profiles into
    # account when encoding constraints for fields.
    if 'comparator' in element_definition_path.split('.'):
      return []

    # If this is an extension, we don't want to access its children/fields.
    # TODO: Add support for complex extensions and the fields
    # inside them.
    if cast(Any, structure_definition).type.value == 'Extension':
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []
    children = self._env.get_children(structure_definition, element_definition)
    for child in children:
      # TODO: Handle choice types, which may have more than one
      # `type.code` value present.
      # If this element is a choice type, a slice (that is not on an extension)
      # or is disabled, then don't encode requirements for it.
      # TODO: Properly handle slices on non-simple extensions.
      if (('[x]' in _get_analytic_path(child) or _is_disabled(child)) or
          (_utils.is_slice_element(child) and
           not _utils.is_slice_on_extension(child))):
        continue

      primitive_regex_info = self._get_regex_from_element(child)
      if primitive_regex_info is None:
        continue  # Unable to find primitive regexes for this child element.

      primitive_regex = primitive_regex_info.regex
      regex_type_code = primitive_regex_info.type_code

      relative_path = _last_path_token(child)
      constraint_key = f'{relative_path}-matches-{regex_type_code}-regex'

      if constraint_key in self._options.skip_keys:
        continue  # Allows users to skip specific regex checks.

      # Early-exit if any types overlap with `_SKIP_TYPE_CODES`.
      type_codes = _utils.element_type_codes(child)
      if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
        continue

      escaped_relative_path = _escape_fhir_path_invocation(relative_path)

      # Generate the FHIR path expression that checks regexes, while also
      # accounting for repeated fields, as FHIR doesn't allow function calls to
      # `matches` where the input collection is repeated.
      # More info here:
      # http://hl7.org/fhirpath/index.html#matchesregex-string-boolean.
      element_is_repeated = _utils.is_repeated_element(child)
      fhir_path_expression = (
          f"{escaped_relative_path}.all( $this.matches('{primitive_regex}') )"
          if element_is_repeated else
          f"{escaped_relative_path}.matches('{primitive_regex}')")

      # Handle special typecode cases, while also accounting for repeated fields
      # , as FHIR doesn't allow direct comparisons involving repeated fields.
      # More info here:
      # http://hl7.org/fhirpath/index.html#comparison.
      if regex_type_code == 'positiveInt':
        fhir_path_expression = (f'{escaped_relative_path}.all( $this > 0 )'
                                if element_is_repeated else
                                f'{escaped_relative_path} > 0')
      if regex_type_code == 'unsignedInt':
        fhir_path_expression = (f'{escaped_relative_path}.all( $this >= 0 )'
                                if element_is_repeated else
                                f'{escaped_relative_path} >= 0')

      required_sql_expression = self._encode_fhir_path_expression(
          structure_definition, element_definition, fhir_path_expression)
      if required_sql_expression is None:
        continue  # Failure to generate Standard SQL expression.

      # Create the `SqlRequirement`.
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(
          _path_to_sql_column_name(constraint_key))
      column_name_base: str = _path_to_sql_column_name(
          self._abs_path_invocation())
      column_name = f'{column_name_base}_{constraint_key_column_name}'
      if column_name in self._regex_columns_generated:
        continue
      self._regex_columns_generated.add(column_name)

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=required_sql_expression,
          severity=(validation_pb2.ValidationSeverity.SEVERITY_ERROR),
          type=validation_pb2.ValidationType.VALIDATION_TYPE_PRIMITIVE_REGEX,
          element_path=element_definition_path,
          description=(f'{relative_path} needs to match regex of '
                       f'{regex_type_code}.'),
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_expression,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              fhir_path_expression))
      encoded_requirements.append(requirement)

    return encoded_requirements

  def _encode_element_definition(
      self, structure_definition: StructureDefinition,
      element_definition: ElementDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of Standard SQL expressions for an `ElementDefinition`."""
    result: List[validation_pb2.SqlRequirement] = []

    # This filters out choice types as they are currently not supported.
    # TODO: Handle choice types, which may have more than one
    # `type.code` value present.
    element_definition_path = (
        f'{self._abs_path_invocation()}.{_last_path_token(element_definition)}')
    if '[x]' in _get_analytic_path(element_definition):
      self._error_reporter.report_conversion_error(
          element_definition_path,
          'The given element is a choice type, which is not yet supported.')
      return result

    # This filters out slices that are not on extensions as they are currently
    # not supported.
    # TODO: Properly handle slices that are not on extensions.
    if (_utils.is_slice_element(element_definition) and
        not _utils.is_slice_on_extension(element_definition)):
      self._error_reporter.report_conversion_error(
          element_definition_path,
          'The given element is a slice that is not on an extension. This is '
          'not yet supported.')
      return result

    type_codes = _utils.element_type_codes(element_definition)
    if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
      return result  # Early-exit if any types overlap with `_SKIP_TYPE_CODES`

    # `ElementDefinition.base.path` is guaranteed to be present for snapshots
    base_path: str = cast(Any, element_definition).base.path.value
    if base_path in _semant.UNSUPPORTED_BASE_PATHS:
      return result  # Early-exit if unsupported `ElementDefinition.base.path`

    # Recurse over the `element_definition`s type
    type_codes = _utils.element_type_codes(element_definition)

    # Mark `(element_definition, structure_definition)` as being visited
    self._ctx.append(State(element_definition, structure_definition))

    # At this point there are no choice types so every element_definition should
    # have at most one type code.
    # Avoid encoding any constraints for the raw `Extension` type, because it's
    # fields are not propagated to the our tables.
    if (type_codes and not _is_primitive_typecode(type_codes[0]) and
        type_codes[0] != 'Extension'):
      type_code = type_codes[0]
      url = _utils.get_absolute_uri_for_structure(type_code)
      parent_structure_definition = self._env.get_structure_definition_for(url)
      if parent_structure_definition is None:
        self._error_reporter.report_conversion_error(
            self._abs_path_invocation(),
            f'Unable to find `StructureDefinition`: `{url}` in environment.')
      else:
        result += self._encode(parent_structure_definition)

    # Encode all relevant FHIRPath expression constraints, prior to recursing on
    # chidren.
    result += self._encode_constraints(structure_definition, element_definition)
    result += self._encode_required_fields(structure_definition,
                                           element_definition)
    if self._options.add_primitive_regexes:
      result += self._encode_primitive_regexes(structure_definition,
                                               element_definition)

    if self._options.add_value_set_bindings:
      result += self._encode_value_set_bindings(element_definition)

    # Ignores the fields inside complex extensions.
    # TODO: Add support for complex extensions and the fields
    # inside them.
    if cast(Any, structure_definition).type.value != 'Extension':
      children = self._env.get_children(structure_definition,
                                        element_definition)
      for child in children:
        result += self._encode_element_definition(structure_definition, child)

    # Finish visiting `(element_definition, structure_definition)`
    _ = self._ctx.pop()

    return result

  def _encode_value_set_bindings(
      self, element_definition: ElementDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Encode .memberOf calls implied by elements bound to value sets."""
    # Ensure the element defines a value set binding.
    binding = cast(Any, element_definition).binding
    value_set_uri: str = binding.value_set.value
    if not value_set_uri:
      return []

    # Ensure the binding is required, see
    # https://build.fhir.org/valueset-binding-strength.html#expansion
    required_enum_val: int = binding.strength.DESCRIPTOR.fields_by_name[
        'value'].enum_type.values_by_name['REQUIRED'].number
    if binding.strength.value != required_enum_val:
      return []

    # Ensure we aren't configured to skip this validation.
    relative_path = _last_path_token(element_definition)
    constraint_key = '%s-memberOf' % relative_path
    if constraint_key in self._options.skip_keys:
      return []

    # Attempt to build SQL for the binding.
    # We always want to build top-level, non-transitive constraints. Breaking
    # the generated SQL expressions into two parts, with one providing the
    # context, and running them together as correlated queries can introduce
    # errors from BigQuery like:
    # "Correlated subqueries that reference other tables are not supported
    # unless they can be de-correlated, such as by transforming them into an
    # efficient JOIN."
    # The SQL generated for memberOf queries handles being called on NULLs by
    # itself. It does not rely on the context returning an empty result set for
    # NULLs.
    path_invocation_less_resource = '.'.join(
        self._abs_path_invocation().split('.')[1:])
    top_level_fhir_path_expression = "%s.memberOf('%s')" % (
        _escape_fhir_path_invocation(path_invocation_less_resource),
        value_set_uri)

    relative_fhir_path_expression = "%s.memberOf('%s')" % (
        _escape_fhir_path_invocation(relative_path), value_set_uri)

    # Build the expression against the top-level resource.
    bottom = self._ctx[0]
    bottom_root_element = self._env.get_root_element_for(bottom.containing_type)
    sql_expression = self._encode_fhir_path_expression(
        bottom.containing_type,
        bottom_root_element,
        top_level_fhir_path_expression,
    )
    if sql_expression is None:
      return []

    element_definition_path = self._abs_path_invocation()
    column_name = _key_to_sql_column_name(
        _path_to_sql_column_name('%s-memberOf' % element_definition_path))
    description = '%s must be a member of %s' % (relative_path, value_set_uri)
    return [
        validation_pb2.SqlRequirement(
            column_name=column_name,
            sql_expression=sql_expression,
            severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
            type=(validation_pb2.ValidationType
                  .VALIDATION_TYPE_VALUE_SET_BINDING),
            element_path=element_definition_path,
            description=description,
            fhir_path_key=constraint_key,
            fhir_path_expression=relative_fhir_path_expression,
            fields_referenced_by_expression=_fields_referenced_by_expression(
                relative_fhir_path_expression))
    ]

  def _encode(
      self, structure_definition: StructureDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Recursively encodes the provided resource into Standard SQL."""
    url_value: str = cast(Any, structure_definition).url.value
    if url_value in self._in_progress:
      self._error_reporter.report_conversion_error(
          self._abs_path_invocation(),
          f'Cycle detected when encoding: {url_value}.')
      return []

    root_element = self._env.get_root_element_for(structure_definition)
    if root_element is None:
      self._error_reporter.report_conversion_error(
          self._abs_path_invocation(),
          f'No root element definition found for: {url_value}.')
      return []

    self._in_progress.add(url_value)
    result = self._encode_element_definition(structure_definition, root_element)
    # Removes duplicates (Same SQL Expression) from our list of requirements.
    result = list({
        requirement.sql_expression: requirement for requirement in result
    }.values())
    self._in_progress.remove(url_value)
    return result

  def encode(
      self, structure_definition: StructureDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Encodes the provided resource into a list of Standard SQL expressions."""
    result: List[validation_pb2.SqlRequirement] = []

    try:
      # Call into our protected recursive-helper method to encode the provided
      # `StructureDefinition`. Propagate any exceptions that occur, and always
      # cleanup state prior to returning.
      result = self._encode(structure_definition)
    finally:
      self._ctx.clear()
      self._in_progress.clear()
      self._requirement_column_names.clear()
      self._element_id_to_regex_map.clear()
      self._regex_columns_generated.clear()

    return result


def _fields_referenced_by_expression(
    fhir_path_expression: str) -> Collection[str]:
  """Finds paths for fields referenced by the given expression.

  For example, an expression like 'a.b.where(c > d.e)' references fields
  ['a.b', 'c, 'd.e']

  Args:
    fhir_path_expression: The expression to search for field paths.

  Returns:
    A collection of paths for fields referenced in the given expression.
  """
  # Sort the results so they are consistently ordered for the golden tests.
  return sorted(
      _ast.paths_referenced_by(_ast.build_fhir_path_ast(fhir_path_expression)))

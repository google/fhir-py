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
"""Contains clases used for getting SQL equivalents of a FHIRPath functions."""

import abc
import copy
import dataclasses
import functools
import operator
from typing import Any, Dict, List, Optional

from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.utils import url_utils
from google.fhir.r4.terminology import local_value_set_resolver


class _FhirPathFunctionStandardSqlEncoder(abc.ABC):
  """Returns a Standard SQL equivalent of a FHIRPath function."""

  def __call__(
      self,
      function: Any,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
      # Individual classes may specify additional kwargs they accept.
      **kwargs,
  ) -> _sql_data_types.Select:
    raise NotImplementedError('Subclasses *must* implement `__call__`.')


class _CountFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns an integer representing the number of elements in a collection.

  By default, `_CountFunction` will return 0.
  """

  def __call__(
      self,
      function: _evaluation.CountFunction,
      operand_result: Optional[_sql_data_types.Select],
      unused_params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('count() cannot be called without an operand.')

    if operand_result.from_part is None:
      # COUNT is an aggregation and requires a FROM. If there is not one
      # already, build a subquery for the FROM.
      return _sql_data_types.Select(
          select_part=_sql_data_types.CountCall((
              _sql_data_types.RawExpression(
                  operand_result.sql_alias,
                  _sql_data_type=operand_result.sql_data_type,
              ),
          )),
          from_part=str(operand_result.to_subquery()),
          where_part=operand_result.where_part,
      )
    else:
      # We don't need a sub-query because we already have a FROM.
      return dataclasses.replace(
          operand_result,
          select_part=_sql_data_types.CountCall((operand_result.select_part,)),
      )


class _EmptyFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns `TRUE` if the input collection is empty and `FALSE` otherwise.

  This is the opposite of `_ExistsFunction`. If the operand is empty, then the
  result is `TRUE`.

  The returned SQL expression is a table of cardinality 1, whose value is of
  `BOOL` type. By default, `_EmptyFunction` will return `FALSE` if given no
  operand.
  """

  def __call__(
      self,
      function: _evaluation.EmptyFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('empty() cannot be called without an operand.')

    sql_alias = 'empty_'
    sql_data_type = _sql_data_types.Boolean

    if not _fhir_path_data_types.returns_collection(
        function.parent_node.return_type
    ):
      # We can use a less expensive scalar check.
      return dataclasses.replace(
          operand_result,
          select_part=operand_result.select_part.is_null(_sql_alias=sql_alias),
      )
    else:
      # We have to use a more expensive EXISTS check.
      return _sql_data_types.Select(
          select_part=_sql_data_types.FunctionCall(
              'NOT EXISTS',
              (
                  _sql_data_types.RawExpression(
                      (
                          f'SELECT {operand_result.sql_alias}\n'
                          f'FROM {operand_result.to_subquery()}\n'
                          f'WHERE {operand_result.sql_alias} IS NOT NULL'
                      ),
                      _sql_data_type=operand_result.sql_data_type,
                  ),
              ),
              _sql_data_type=sql_data_type,
              _sql_alias=sql_alias,
          ),
          from_part=None,
      )


# TODO(b/244184211): Add support for params
class _ExistsFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns `TRUE` if the operand has any elements, and `FALSE` otherwise.

  This is the opposite of `_EmptyFunction`. If the operand is empty, then the
  result is `FALSE`.

  The returned SQL expression is a table of cardinality 1, whose value is of
  `BOOL` type. By default, `_ExistsFunction` will return `TRUE` if given no
  operand.
  """

  def __call__(
      self,
      function: _evaluation.ExistsFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('exists() cannot be called without an operand.')

    if params_result:
      raise ValueError(
          'Unsupported FHIRPath expression: `criteria` parameter for exists() '
          'is not currently supported.'
      )

    sql_alias = 'exists_'
    sql_data_type = _sql_data_types.Boolean
    # Check that the operand is not a collection and has no where part.
    # In situations where the `where` function filters out all results,
    # it causes the query to return 'no rows' which we later interpret
    # as 'passing validation' in our `sql_expressions_to_view.py`.
    if (
        not _fhir_path_data_types.returns_collection(
            function.parent_node.return_type
        )
        and not operand_result.where_part
    ):
      # We can use a less expensive scalar check.
      return dataclasses.replace(
          operand_result,
          select_part=operand_result.select_part.is_not_null(
              _sql_alias=sql_alias
          ),
      )
    else:
      # We have to use a more expensive EXISTS check.
      return _sql_data_types.Select(
          select_part=_sql_data_types.FunctionCall(
              'EXISTS',
              (
                  _sql_data_types.RawExpression(
                      (
                          f'SELECT {operand_result.sql_alias}\n'
                          f'FROM {operand_result.to_subquery()}\n'
                          f'WHERE {operand_result.sql_alias} IS NOT NULL'
                      ),
                      _sql_data_type=operand_result.sql_data_type,
                  ),
              ),
              _sql_data_type=sql_data_type,
              _sql_alias=sql_alias,
          ),
          from_part=None,
      )


class _FirstFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns a collection with the first value of the operand collection.

  The returned SQL expression is a table with cardinality 0 or 1.
  """

  def __call__(
      self,
      function: _evaluation.FirstFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('first() cannot be called without an operand.')

    # We append a limit 1 to get the first row in row order.
    # Note that if an ARRAY was unnested, row order may not match array order,
    # but for most FHIR this should not matter.
    result = copy.copy(operand_result)
    if not result.limit_part:
      result.limit_part = 1
    return result


class _AnyTrueFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns true if any value in the operand collection is TRUE."""

  def __call__(
      self,
      function: _evaluation.AnyTrueFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('anyTrue() cannot be called without an operand.')

    sql_alias = '_anyTrue'
    return _sql_data_types.Select(
        select_part=_sql_data_types.FunctionCall(
            'LOGICAL_OR',
            (
                _sql_data_types.RawExpression(
                    operand_result.sql_alias,
                    _sql_data_type=operand_result.sql_data_type,
                ),
            ),
            _sql_data_type=_sql_data_types.Boolean,
            _sql_alias=sql_alias,
        ),
        from_part=str(operand_result.to_subquery()),
    )


class _HasValueFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns `TRUE` if the operand is a primitive with a `value` field.

  The input operand must be a collection of FHIR primitives of cardinality 1,
  and the value must be non-`NULL`.
  """

  def __call__(
      self,
      function: _evaluation.HasValueFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('hasValue() cannot be called without an operand.')

    # TODO(b/234476234):
    # The spec says: "Returns true if the input collection contains a single
    # value which is a FHIR primitive, and it has a primitive value (e.g. as
    # opposed to not having a value and just having extensions)."
    # Currently we're just checking if the value is null.
    # We don't check if it's a primitive or not.
    # For collections, we're returning one row per collection element rather
    # than ensuring the collection contains a single value.
    sql_alias = 'has_value_'
    return dataclasses.replace(
        operand_result,
        select_part=operand_result.select_part.is_not_null(
            _sql_alias=sql_alias
        ),
    )


class _NotFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns `TRUE` if the input collection evaluates to `FALSE`.

  The operand is expected to be a table subquery of cardinality 1, whose value
  is a `BOOL` type. By default, `_NotFunction` will return `FALSE` if given no
  operator.
  """

  def __call__(
      self,
      function: _evaluation.NotFunction,
      operand_result: Optional[_sql_data_types.Select],
      unused_params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('not() cannot be called without an operand.')

    # TODO(b/234478081):
    # The spec says: "Returns true if the input collection evaluates to false,
    # and false if it evaluates to true. Otherwise, the result is empty ({ })"
    # https://hl7.org/fhirpath/#not-boolean
    #
    # For collections, we are returning one value per element in the
    # collection rather than aggregating the collection elements into a single
    # boolean value.
    #
    # The spec describes how to evaluate collections as single boolean values:
    # "For all boolean operators, the collections passed as operands are first
    # evaluated as Booleans (as described in Singleton Evaluation of
    # Collections). The operators then use three-valued logic to propagate
    # empty operands.
    # https://hl7.org/fhirpath/#boolean-logic
    #
    # More information here:
    # https://hl7.org/fhirpath/#singleton-evaluation-of-collections
    sql_alias = 'not_'
    sql_data_type = _sql_data_types.Boolean

    return dataclasses.replace(
        operand_result,
        select_part=_sql_data_types.FunctionCall(
            'NOT',
            (operand_result.select_part,),
            _sql_alias=sql_alias,
            _sql_data_type=sql_data_type,
        ),
    )


class _MatchesFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns `TRUE` if the operand matches the regex in the given param.

  This function takes one param (`pattern`) in addition to the operand. If
  `pattern` is not provided the matches function returns the empty set which in
  this function translates to NULL.

  The returned SQL expression is a table of cardinality 1, whose value is of
  `BOOL` type. By default, `_MatchesFunction` will return `FALSE` if given no
  operand.

  Returns an error In the event that the input collection contains multiple
  items.
  """

  def __call__(
      self,
      function: _evaluation.MatchesFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('matches() cannot be called without an operand.')

    # If the input collection contains multiple items, the evaluation of the
    # expression will end and signal an error to the calling environment.
    # https://build.fhir.org/ig/HL7/FHIRPath/#matchesregex-string-boolean
    if _fhir_path_data_types.is_collection(function.parent_node.return_type):
      raise ValueError(
          'matches() cannot be called on a collection type. '
          'Must either be scalar or unnested with a function '
          'like all()'
      )
    sql_alias = 'matches_'
    sql_data_type = _sql_data_types.Boolean

    # If `pattern` is not given, return empty set.
    if not params_result:
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              'NULL',
              _sql_alias=sql_alias,
              _sql_data_type=sql_data_type,
          ),
          from_part=None,
      )
    else:
      return dataclasses.replace(
          operand_result,
          select_part=_sql_data_types.RegexpContainsCall(
              (operand_result.select_part, params_result[0])
          ),
      )


class _MemberOfFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns `TRUE` if the operand is a value in the given valueset.

  See the memberOf function in https://build.fhir.org/fhirpath.html#functions
  for the full specification.

  The generated SQL assumes the existence of a value set codes table as defined
  here:
  https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support

  By default, the generated SQL will refer to a value set codes table named
  VALUESET_VIEW. A value_set_codes_table argument may be supplied to this
  class's __call__ method to use a different table name. The process executing
  the generated SQL must have permission to read this table.

  This function takes one param (`valueset`) in addition to the operand. The
  valueset must be a URI referring to a valueset known in a VALUESET_VIEW table
  or view so the generator can expand it appropriately.

  The operand may be a String, Code, Coding or CodeableConcept.

  The returned SQL expression is a table of cardinality 1 for non-repeated
  fields and of cardinality matching the number of elements in repeated fields
  or fields with repeated parents. The table's value is of `BOOL` type.
  """

  def __call__(  # pytype: disable=signature-mismatch  # overriding-parameter-type-checks
      self,
      function: _evaluation.MemberOfFunction,
      operand_result: _sql_data_types.IdentifierSelect,
      params_result: List[_sql_data_types.StandardSqlExpression],
      value_set_codes_table: Optional[str] = None,
      value_set_resolver: Optional[
          local_value_set_resolver.LocalResolver
      ] = None,
  ) -> _sql_data_types.Select:
    operand_node = function.parent_node
    operand_type = operand_node.return_type
    sql_alias = 'memberof_'

    # See if the value set has a simple definition we can expand
    # ourselves. If so, expand the value set and generate an IN
    # expression validating if the codes in the column are members of
    # the value set.
    if value_set_resolver is not None:
      expanded_value_set = value_set_resolver.expand_value_set_url(
          function.value_set_url
      )
      if expanded_value_set is not None:
        return self._member_of_sql_against_inline_value_sets(
            sql_alias,
            operand_result,
            operand_type,
            expanded_value_set,
        )

    # If we can't expand the value set ourselves, we fall back to
    # JOIN-ing against an external value sets table.
    if value_set_codes_table is not None:
      return self._member_of_sql_against_remote_value_set_table(
          sql_alias,
          operand_result,
          operand_node,
          operand_type,
          function.value_set_url,
          value_set_codes_table,
      )

    raise ValueError(
        'Unable to expand value set %s locally and no value set'
        ' definitions table provided. Unable to generate memberOf SQL.'
        % function.value_set_url,
    )

  def _member_of_sql_against_inline_value_sets(
      self,
      sql_alias: str,
      operand_result: _sql_data_types.IdentifierSelect,
      operand_type: _fhir_path_data_types.FhirPathDataType,
      expanded_value_set: value_set_pb2.ValueSet,
  ) -> _sql_data_types.Select:
    """Generates memberOf SQL using an IN statement."""
    if isinstance(operand_type, _fhir_path_data_types.String.__class__):
      # For strings and codes, we can just generate SQL like
      # SELECT code IN (code1, code2...)
      params = [
          _sql_data_types.RawExpression(
              '"%s"' % concept.code.value, _sql_data_types.String
          )
          for concept in expanded_value_set.expansion.contains
      ]

      return dataclasses.replace(
          operand_result,
          select_part=operand_result.select_part.is_null().or_(
              operand_result.select_part.in_(params), _sql_alias=sql_alias
          ),
      )
    if _fhir_path_data_types.is_coding(operand_type):
      # Codings include a code system in addition to the code value,
      # so we have to generate more complex SQL like:
      # SELECT (system = system1 AND code IN (code1, code2)) OR
      #        (system = system2 AND code IN (code3, code4))
      predicate = self._build_predicate_for_coding_in_value_set(
          expanded_value_set, operand_result.select_part
      )
      return dataclasses.replace(
          operand_result,
          select_part=operand_result.select_part.is_null().or_(
              predicate, _sql_alias=sql_alias
          ),
      )

    if _fhir_path_data_types.is_codeable_concept(operand_type):
      # Codeable concepts are an array of codings. We need to check if
      # any of the codings in the array match one of the value set's
      # bound codings. We generate SQL like:
      # SELECT EXISTS(
      #   SELECT 1
      #   FROM UNNEST(codeable_concept)
      #   WHERE (system = system1 AND code IN (code1, code2)) OR
      #         (system = system2 AND code IN (code3, code4))
      #
      coding_column = operand_result.select_part.dot(
          'coding', _sql_data_types.String
      )
      predicate = self._build_predicate_for_coding_in_value_set(
          expanded_value_set
      )
      return dataclasses.replace(
          operand_result,
          select_part=coding_column.is_null().or_(
              _sql_data_types.RawExpression(
                  (
                      'EXISTS(\n'
                      'SELECT 1\n'
                      f'FROM UNNEST({coding_column})\n'
                      f'WHERE {predicate})'
                  ),
                  _sql_data_type=_sql_data_types.Boolean,
              ),
              _sql_alias=sql_alias,
          ),
      )

    raise ValueError(
        'Unexpected type %s and structure definition %s encountered'
        % (operand_type, operand_type.url)
    )

  def _build_predicate_for_coding_in_value_set(
      self,
      expanded_value_set: value_set_pb2.ValueSet,
      coding_column: Optional[_sql_data_types.Identifier] = None,
  ) -> _sql_data_types.StandardSqlExpression:
    """Builds a predicate asserting the coding column is bound to the value_set.

    Ensures that the codings contained in `coding_column` are codings found in
    `expanded_value_set`.
    Produces SQL like:
    (`coding_column`.system = system1 AND `coding_column`.code IN (
      code1, code2)) OR
    (`coding_column`.system = system2 AND `coding_column`.code IN (
      code3, code4))

    Args:
      expanded_value_set: The expanded value set containing the coding values to
        assert membership against.
      coding_column: The column containing the coding values. If given, columns
        `coding_column`.system and `coding_column`.code will be referenced in
        the predicate. If not given, columns 'system' and 'code' will be
        referenced.

    Returns:
      The SQL for the value set binding predicate.
    """
    # Group codes per code system.
    codes_per_system = {}
    for concept in expanded_value_set.expansion.contains:
      codes_per_system.setdefault(concept.system.value, []).append(
          concept.code.value
      )

    # Sort the code system entries to ensure we build stable SQL.
    codes_per_system = list(codes_per_system.items())
    codes_per_system.sort(key=operator.itemgetter(0))
    for _, codes in codes_per_system:
      codes.sort()

    # Build a 'system = system AND code IN (codes)' expression per code system
    if coding_column is None:
      code_col = _sql_data_types.Identifier('code', _sql_data_types.String)
      system_col = _sql_data_types.Identifier('system', _sql_data_types.String)
    else:
      code_col = coding_column.dot('code', _sql_data_types.String)
      system_col = coding_column.dot('system', _sql_data_types.String)

    code_system_predicates = []
    for system, codes in codes_per_system:
      system = _sql_data_types.RawExpression(
          '"%s"' % system, _sql_data_types.String
      )
      codes = [
          _sql_data_types.RawExpression('"%s"' % code, _sql_data_types.String)
          for code in codes
      ]
      code_system_predicates.append(
          system_col.eq_(system).and_(code_col.in_(codes))
      )

    # OR each of the above predicates.
    return functools.reduce(
        lambda acc, pred: acc.or_(pred), code_system_predicates
    )

  def _member_of_sql_against_remote_value_set_table(
      self,
      sql_alias: str,
      operand_result: _sql_data_types.IdentifierSelect,
      operand_node: _evaluation.ExpressionNode,
      operand_type: _fhir_path_data_types.FhirPathDataType,
      value_set_param: str,
      value_set_codes_table: str,
  ) -> _sql_data_types.Select:
    """Generates memberOf SQL using a JOIN against a terminology table.."""
    is_collection = _fhir_path_data_types.returns_collection(
        operand_node.return_type
    )
    is_string_or_code = isinstance(
        operand_type, _fhir_path_data_types.String.__class__
    )
    is_coding = _fhir_path_data_types.is_coding(operand_type)
    is_codeable_concept = _fhir_path_data_types.is_codeable_concept(
        operand_type
    )

    value_set_uri, value_set_version = url_utils.parse_url_version(
        value_set_param
    )

    value_set_uri_expr = f"'{value_set_uri}'"
    if value_set_version:
      value_set_version_predicate = (
          f"AND vs.valuesetversion='{value_set_version}'\n"
      )
    else:
      value_set_version_predicate = ''

    # TODO(b/228467141): As the future allows, we'd like to simplify this SQL.
    # We have to take pains to avoid the following error from BigQuery as we
    # join to the external value set codes table:
    # "Correlated subqueries that reference other tables are not supported
    # unless they can be de-correlated, such as by transforming them into an
    # efficient JOIN."
    #
    # When selecting scalar codes, we add a semantically meaningless
    # UNNEST SELECT IF(code IS NULL, [], [EXISTS(...)])
    # to trick the query optimizer. We'd prefer to write
    # SELECT IF(CODE IS NULL, NULL, EXISTS(...))
    # However, the predicate from the enclosing
    # ARRAY(... WHERE sql_alias IS NOT NULL)
    # expression added by FhirPathStandardSqlEncoder.encode triggers the
    # correlated subqueries error. The UNNEST([...]) avoids the error.
    #
    # When working with collections of codes, we likewise need to add a spurious
    # UNNEST(ARRAY(...)) to avoid the error. Worse, the same error prevents us
    # from writing the simpler
    # SELECT EXISTS(
    #   SELECT 1 FROM VALUESET_VIEW vs
    #   WHERE vs.valueseturi='....' AND vs.code = repeated_elem.code)
    # FROM UNNEST(repeated_elem) AS repeated_elem
    # Inestead, we are forced to write a much more complex query.
    # The current strategy is to find the offsets for all elements in the
    # array which join to the value set view and then compare it to the full
    # set of offsets.
    if is_string_or_code and not is_collection:
      return _sql_data_types.Select(
          select_part=_sql_data_types.Identifier(
              sql_alias, _sql_data_type=_sql_data_types.Boolean
          ),
          from_part=(
              'UNNEST((SELECT IF('
              f'{operand_result.select_part} IS NULL, '
              '[], [\n'
              'EXISTS(\n'
              'SELECT 1\n'
              f'FROM `{value_set_codes_table}` vs\n'
              'WHERE\n'
              f'vs.valueseturi={value_set_uri_expr}\n'
              f'{value_set_version_predicate}'
              f'AND vs.code={operand_result.select_part}\n'
              f')]))) AS {sql_alias}'
          ),
          where_part=operand_result.where_part,
      )
    elif is_string_or_code and is_collection:
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              # Report if offsets from the collection's array were not able to
              # be joined to the value set view.
              'matches.element_offset IS NOT NULL',
              _sql_alias=sql_alias,
              _sql_data_type=_sql_data_types.Boolean,
          ),
          from_part=(
              # Find all offsets in the collection's array.
              f'(SELECT element_offset\n'
              f'FROM {operand_result.from_part}) AS all_\n'
              # Compare them against offsets for elements in the value set.
              f'LEFT JOIN (SELECT element_offset\n'
              # The UNNEST(ARRAY()) is semantically meaningless but allows the
              # query to run. Otherwise, BigQuery returns the error: "Correlated
              # subqueries that reference other tables are not supported unless
              # they can be de-correlated, such as by transforming them into an
              # efficient JOIN."
              f'FROM UNNEST(ARRAY(SELECT element_offset FROM (\n'
              # Find offsets for all array elements present in the value set.
              f'SELECT element_offset\n'
              f'FROM {operand_result.from_part}\n'
              f'INNER JOIN `{value_set_codes_table}` vs ON\n'
              f'vs.valueseturi={value_set_uri_expr}\n'
              f'{value_set_version_predicate}'
              f'AND vs.code={operand_result.select_part}\n'
              f'))) AS element_offset\n'
              ') AS matches\n'
              f'ON all_.element_offset=matches.element_offset\n'
              f'ORDER BY all_.element_offset'
          ),
          where_part=operand_result.where_part,
      )
    elif is_coding and not is_collection:
      return _sql_data_types.Select(
          select_part=_sql_data_types.Identifier(
              sql_alias, _sql_data_type=_sql_data_types.Boolean
          ),
          from_part=(
              'UNNEST((SELECT IF('
              f'{operand_result.select_part} IS NULL, '
              '[], [\n'
              'EXISTS(\n'
              'SELECT 1\n'
              f'FROM `{value_set_codes_table}` vs\n'
              'WHERE\n'
              f'vs.valueseturi={value_set_uri_expr}\n'
              f'{value_set_version_predicate}'
              f'AND vs.system={operand_result.select_part}.system\n'
              f'AND vs.code={operand_result.select_part}.code\n'
              f')]))) AS {sql_alias}'
          ),
          where_part=operand_result.where_part,
      )
    elif is_coding and is_collection:
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              # Report if offsets from the collection's array were not able to
              # be joined to the value set view.
              'matches.element_offset IS NOT NULL',
              _sql_alias=sql_alias,
              _sql_data_type=_sql_data_types.Boolean,
          ),
          from_part=(
              # Find all offsets in the collection's array.
              f'(SELECT element_offset\n'
              # The from_part will be an UNNEST because this is a collection.
              f'FROM {operand_result.from_part}) AS all_\n'
              # Compare them against offsets for elements in the value set.
              'LEFT JOIN (SELECT element_offset\n'
              # The UNNEST(ARRAY()) is semantically meaningless but allows the
              # query to run. Otherwise, BigQuery returns the error: "Correlated
              # subqueries that reference other tables are not supported unless
              # they can be de-correlated, such as by transforming them into an
              # efficient JOIN."
              f'FROM UNNEST(ARRAY(SELECT element_offset FROM (\n'
              # Find offsets for all array elements present in the value set.
              f'SELECT element_offset\n'
              f'FROM {operand_result.from_part}\n'
              f'INNER JOIN `{value_set_codes_table}` vs ON\n'
              f'vs.valueseturi={value_set_uri_expr}\n'
              f'{value_set_version_predicate}'
              f'AND vs.system={operand_result.select_part}.system\n'
              f'AND vs.code={operand_result.select_part}.code\n'
              f'))) AS element_offset\n'
              ') AS matches\n'
              'ON all_.element_offset=matches.element_offset\n'
              'ORDER BY all_.element_offset'
          ),
          where_part=operand_result.where_part,
      )
    elif is_codeable_concept and not is_collection:
      return _sql_data_types.Select(
          select_part=_sql_data_types.Identifier(
              sql_alias, _sql_data_type=_sql_data_types.Boolean
          ),
          from_part=(
              'UNNEST((SELECT IF('
              f'{operand_result.select_part} IS NULL, '
              '[], [\n'
              'EXISTS(\n'
              'SELECT 1\n'
              f'FROM UNNEST({operand_result.select_part}.coding) AS codings\n'
              f'INNER JOIN `{value_set_codes_table}` vs ON\n'
              f'vs.valueseturi={value_set_uri_expr}\n'
              f'{value_set_version_predicate}'
              'AND vs.system=codings.system\n'
              'AND vs.code=codings.code\n'
              f')]))) AS {sql_alias}'
          ),
          where_part=operand_result.where_part,
      )
    elif is_codeable_concept and is_collection:
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              # Report if offsets from the collection's array were not able to
              # be joined to the value set view.
              'matches.element_offset IS NOT NULL',
              _sql_alias=sql_alias,
              _sql_data_type=_sql_data_types.Boolean,
          ),
          from_part=(
              # Find all offsets in the collection's array.
              f'(SELECT element_offset\n'
              # The from_part will be an UNNEST because this is a collection.
              f'FROM {operand_result.from_part}) AS all_\n'
              # Compare them against offsets for elements in the value set.
              f'LEFT JOIN (SELECT element_offset\n'
              # The UNNEST(ARRAY()) is semantically meaningless but allows the
              # query to run. Otherwise, BigQuery returns the error: "Correlated
              # subqueries that reference other tables are not supported unless
              # they can be de-correlated, such as by transforming them into an
              # efficient JOIN."
              'FROM UNNEST(ARRAY(SELECT element_offset FROM (\n'
              # Find offsets for all array elements present in the value set.
              # We need the distinct in case multiple codes in the coding array
              # join to the value set table.
              f'SELECT DISTINCT element_offset\n'
              f'FROM {operand_result.from_part},\n'
              f'UNNEST({operand_result.select_part}.coding) AS codings\n'
              f'INNER JOIN `{value_set_codes_table}` vs ON\n'
              f'vs.valueseturi={value_set_uri_expr}\n'
              f'{value_set_version_predicate}'
              'AND vs.system=codings.system\n'
              'AND vs.code=codings.code\n'
              '))) AS element_offset\n'
              ') AS matches\n'
              'ON all_.element_offset=matches.element_offset\n'
              f'ORDER BY all_.element_offset'
          ),
          where_part=operand_result.where_part,
      )
    else:
      raise ValueError(
          'Unexpected type %s and structure definition %s encountered'
          % (operand_type, operand_type.url)
      )


# TODO(b/221322122): Separate custom functions from core FHIRPath functions,
# and add dedicated tests for the different function classes.
class _IdForFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns the raw ID for a given resource type."""

  def __call__(  # pytype: disable=signature-mismatch  # overriding-parameter-type-checks
      self,
      function: _evaluation.IdForFunction,
      operand_result: Optional[_sql_data_types.IdentifierSelect],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('idFor() cannot be called without an operand.')

    if len(params_result) != 1:
      raise ValueError('IdForFunction must have a resource type parameter.')

    # As described in
    # https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md,
    # this is a special case where the name of the field is based on the desired
    # reference target, e.g. patientId or organizationId.
    sql_alias = 'idFor_'
    resource_type = function.base_type_str
    # Make the first character lowercase to match the column names.
    resource_type = resource_type[:1].lower() + resource_type[1:]

    return dataclasses.replace(
        operand_result,
        select_part=operand_result.select_part.dot(
            f'{resource_type}Id', _sql_data_types.String, sql_alias=sql_alias
        ),
    )


class _OfTypeFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns the resource of the given type, typically used in choice types."""

  def __call__(  # pytype: disable=signature-mismatch  # overriding-parameter-type-checks
      self,
      function: _evaluation.OfTypeFunction,
      operand_result: Optional[_sql_data_types.IdentifierSelect],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('ofType() cannot be called without an operand.')

    if len(params_result) != 1:
      raise ValueError('ofType must have a data type parameter.')

    sql_alias = 'ofType_'
    attribute = function.base_type_str
    return_type = _sql_data_types.get_standard_sql_data_type(
        function.return_type
    )

    return dataclasses.replace(
        operand_result,
        select_part=operand_result.select_part.dot(
            attribute, return_type, sql_alias=sql_alias
        ),
    )


# TODO(b/197153378): Add support for $this.
class _WhereFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns a collection of all the items that match the criteria expression.

  This function takes one param (`criteria`) in addition to the operand.

  If the operand is not provided the matches function returns the empty set
  which in this function translates to NULL.

  Returns an error in the event that the `criteria` param is not provided or its
  data type is not bool.
  """

  def __call__(
      self,
      function: _evaluation.WhereFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    sql_alias = 'where_clause_'

    if not operand_result:
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              'NULL',
              _sql_alias=sql_alias,
              _sql_data_type=_sql_data_types.Undefined,
          ),
          from_part=None,
      )

    criteria = params_result[0]
    where_part = (
        f'{operand_result.where_part} AND {criteria.as_operand()}'
        if operand_result.where_part
        else criteria.as_operand()
    )

    # Queries without a FROM clause cannot have a WHERE clause. So we create a
    # dummy FROM clause here if needed.
    if operand_result.from_part:
      from_part = operand_result.from_part
    # Include the asterix in the dummy from clause if the element is a
    # STRUCT, because it's fields may be accessed in the where clause.
    elif isinstance(operand_result.sql_data_type, _sql_data_types.Struct):
      from_part = f'(SELECT {operand_result.select_part}.*)'
    else:
      from_part = f'(SELECT {operand_result.select_part})'

    return _sql_data_types.Select(
        select_part=operand_result.select_part,
        from_part=from_part,
        where_part=where_part,
    )


class _AllFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns true if criteria evaluates to true for every item in its operand.

  This function takes one param (`criteria`) in addition to its operand. If
  operand is not provided, it returns True.

  Returns an error in the event that the `criteria` param is not provided or its
  data type is not bool.
  """

  def __call__(
      self,
      function: _evaluation.AllFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    sql_alias = 'all_'
    sql_data_type = _sql_data_types.Boolean

    # If operand or params is not given, return TRUE.
    if not operand_result or not params_result:
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              'TRUE',
              _sql_alias=sql_alias,
              _sql_data_type=_sql_data_types.Boolean,
          ),
          from_part=None,
      )
    else:
      criteria = params_result[0]

      # There is an edge case where if the operand(context elemenet) is a
      # repeated field then using the whole subquery causes a value error.
      # Because the whole subquery includes `SELECT repeated_field_alias...`
      # which causes any future reference to a field in repeated_field_alias to
      # fail, thus we extract and use just the from_clause.
      context_sql = None
      where_part = None
      if _fhir_path_data_types.is_collection(function.parent_node.return_type):
        context_sql = operand_result.from_part
        where_part = operand_result.where_part
      else:
        context_sql = str(operand_result.to_subquery())

      criteria_sql = _sql_data_types.RawExpression(
          criteria.as_operand(),
          _sql_alias=sql_alias,
          _sql_data_type=sql_data_type,
      ).to_subquery()

      internal_if_null_call = _sql_data_types.FunctionCall(
          'IFNULL',
          [criteria_sql, 'FALSE'],
          _sql_alias=sql_alias,
          _sql_data_type=sql_data_type,
      )

      logical_and_call = _sql_data_types.FunctionCall(
          'LOGICAL_AND',
          (internal_if_null_call,),
          _sql_alias=sql_alias,
          _sql_data_type=sql_data_type,
      )

      # Constructs and returns the following sql:
      # `IF_NULL(LOGICAL_AND(IF_NULL(criteria, False)), True)`.
      # We need the internal IF_NULL because the all function returns True if
      # the input param / criteria is NULL.
      return _sql_data_types.Select(
          select_part=_sql_data_types.FunctionCall(
              'IFNULL',
              [logical_and_call, 'TRUE'],
              _sql_alias=sql_alias,
              _sql_data_type=sql_data_type,
          ),
          from_part=context_sql,
          where_part=where_part,
      )


class _ToIntegerFunction(_FhirPathFunctionStandardSqlEncoder):
  """Casts the given operand to an integer.

  The spec for this function is found here:
  https://build.fhir.org/ig/HL7/FHIRPath/#integer-conversion-functions
  """

  def __call__(
      self,
      function: _evaluation.ToIntegerFunction,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('toInteger() cannot be called without an operand.')

    if params_result:
      raise ValueError('toInteger() does not accept any parameters.')

    sql_alias = 'to_integer_'
    sql_data_type = _sql_data_types.Int64

    # Use the AST to figure out the type of the operand.
    operand_type = function.parent_node.return_type

    # If the input collection contains a single item, this function
    # will return a single integer if:
    #
    # the item is an Integer
    # the item is a String and is convertible to an integer
    # the item is a Boolean, where true results in a 1 and false results in a 0.
    #
    # If the item is not one the above types, the result is empty.
    if not isinstance(
        operand_type,
        (
            _fhir_path_data_types.Integer.__class__,
            _fhir_path_data_types.String.__class__,
            _fhir_path_data_types.Boolean.__class__,
        ),
    ):
      return _sql_data_types.Select(
          select_part=_sql_data_types.RawExpression(
              'NULL',
              _sql_alias=sql_alias,
              _sql_data_type=sql_data_type,
          ),
          from_part=None,
      )

    # The spec says:
    # "If the input collection contains multiple items, the evaluation
    # of the expression will end and signal an error to the calling
    # environment."
    # This is harder to do in SQL where we can't raise exceptions, so
    # instead we just apply a limit of 1 on any collections.
    if _fhir_path_data_types.returns_collection(operand_type):
      first = FUNCTION_MAP[_evaluation.FirstFunction.NAME]
      operand_result = first(function, operand_result, [])

    return dataclasses.replace(
        operand_result,
        select_part=operand_result.select_part.cast(
            sql_data_type, _sql_alias=sql_alias
        ),
    )


FUNCTION_MAP: Dict[str, _FhirPathFunctionStandardSqlEncoder] = {
    _evaluation.CountFunction.NAME: _CountFunction(),
    _evaluation.EmptyFunction.NAME: _EmptyFunction(),
    _evaluation.ExistsFunction.NAME: _ExistsFunction(),
    _evaluation.FirstFunction.NAME: _FirstFunction(),
    _evaluation.AnyTrueFunction.NAME: _AnyTrueFunction(),
    _evaluation.HasValueFunction.NAME: _HasValueFunction(),
    _evaluation.IdForFunction.NAME: _IdForFunction(),
    _evaluation.NotFunction.NAME: _NotFunction(),
    _evaluation.MatchesFunction.NAME: _MatchesFunction(),
    _evaluation.MemberOfFunction.NAME: _MemberOfFunction(),
    _evaluation.OfTypeFunction.NAME: _OfTypeFunction(),
    _evaluation.WhereFunction.NAME: _WhereFunction(),
    _evaluation.AllFunction.NAME: _AllFunction(),
    _evaluation.ToIntegerFunction.NAME: _ToIntegerFunction(),
}

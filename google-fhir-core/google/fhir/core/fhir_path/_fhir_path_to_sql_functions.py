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
from typing import Dict, List, Optional, cast
import urllib.parse

from google.protobuf import message
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _navigation
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.fhir_path import fhir_path_options
from google.fhir.core.utils import url_utils
from google.fhir.r4.terminology import local_value_set_resolver

# TODO(b/201107372): Update FHIR-agnostic types to a protocol.
StructureDefinition = message.Message
ElementDefinition = message.Message
Constraint = message.Message


class _FhirPathFunctionStandardSqlEncoder(abc.ABC):
  """Returns a Standard SQL equivalent of a FHIRPath function."""

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    """Validates the input parameters given to this function.

    Args:
      function: The function `Expression` that should be type checked.
      operand: The `Expression` that this function is applied on.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.
      options: Optional settings for influencing validation behavior.

    Returns:
      A string describing any error encountered. Or None if validation passed.
    """
    del function, operand, walker, options
    return None

  @abc.abstractmethod
  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Returns the data type that this function returns."""

  @abc.abstractmethod
  def __call__(
      self,
      function: _ast.Function,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
      # Individual classes may specify additional kwargs they accept.
      **kwargs,
  ) -> _sql_data_types.Select:
    pass


class _CountFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns an integer representing the number of elements in a collection.

  By default, `_CountFunction` will return 0.
  """

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, operand, walker
    return _fhir_path_data_types.Integer

  def __call__(
      self,
      function: _ast.Function,
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

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, operand, walker
    return _fhir_path_data_types.Boolean

  def __call__(
      self,
      function: _ast.Function,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('empty() cannot be called without an operand.')

    sql_alias = 'empty_'
    sql_data_type = _sql_data_types.Boolean

    if not isinstance(
        function.parent.lhs.data_type, _fhir_path_data_types.Collection
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

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, operand, walker
    return _fhir_path_data_types.Boolean

  def __call__(
      self,
      function: _ast.Function,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('exists() cannot be called without an operand.')

    sql_alias = 'exists_'
    sql_data_type = _sql_data_types.Boolean

    # Check that the operand is not a collection and has no where part.
    # In situations where the `where` function filters out all results,
    # it causes the query to return 'no rows' which we later interpret
    # as 'passing validation' in our `sql_expressions_to_view.py`.
    if (
        not isinstance(
            function.parent.lhs.data_type, _fhir_path_data_types.Collection
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

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    del function, walker, options
    if (
        isinstance(operand.data_type, _fhir_path_data_types.Collection)
        and len(operand.data_type.types) > 1
    ):
      return (
          'first() can only process Collections with a single data type. '
          f'Got Collection with {operand.data_type.types} types.'
      )

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    # The return type is the type of the Collection. Currently, we do not
    # support Collections of multiple types. If the input is not a Collection,
    # the return type is the same type that was passed in.
    if isinstance(operand.data_type, _fhir_path_data_types.Collection):
      return next(iter(operand.data_type.types))

    return operand.data_type

  def __call__(
      self,
      function: _ast.Function,
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

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    del function, walker, options
    if (
        not isinstance(operand.data_type, _fhir_path_data_types.Collection)
        or next(iter(operand.data_type.types)) != _fhir_path_data_types.Boolean
    ):
      return (
          'anyTrue() must be called on a Collection of booleans. '
          f'Got type of {operand.data_type}.'
      )
    if len(operand.data_type.types) > 1:
      return (
          'anyTrue() can only process Collections with a single data type. '
          f'Got Collection with {operand.data_type.types} types.'
      )

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    return _fhir_path_data_types.Boolean

  def __call__(
      self,
      function: _ast.Function,
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

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, operand, walker
    return _fhir_path_data_types.Boolean

  def __call__(
      self,
      function: _ast.Function,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('hasValue() cannot be called without an operand.')

    sql_alias = 'has_value_'
    # TODO(b/234476234): HasValue Implementation Does Not Handle Collections
    # Correctly.
    # The spec says: "Returns true if the input collection contains a single
    # value which is a FHIR primitive, and it has a primitive value (e.g. as
    # opposed to not having a value and just having extensions)."
    # Currently we're just checking if the value is null.
    # We don't check if it's a primitive or not.
    # For collections, we're returning one row per collection element rather
    # than ensuring the collection contains a single value.
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

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, operand, walker
    return _fhir_path_data_types.Boolean

  def __call__(
      self,
      function: _ast.Function,
      operand_result: Optional[_sql_data_types.Select],
      unused_params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('not() cannot be called without an operand.')

    sql_alias = 'not_'
    sql_data_type = _sql_data_types.Boolean

    # TODO(b/234478081): Not Doesn't Handle Collections Correctly.
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

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    del function, walker, options
    # The matches function throws an error if its input operand is a collection.
    # It doesn't apply to elements that were repeated but have used
    # mapping / filter functions to target the individual elements inside them
    # (e.g. collection_of_items.all( item.matches(...) ) ).

    # Semantic analysis tracks this and tells us the current state of the
    # operand using `operand.data_type`.
    if isinstance(operand.data_type, _fhir_path_data_types.Collection):
      return (
          'Matches function expected lhs to be a scalar type but got: '
          f'{operand.data_type}'
      )
    return None

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, operand, walker
    return _fhir_path_data_types.Boolean

  def __call__(
      self,
      function: _ast.Function,
      operand_result: Optional[_sql_data_types.Select],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('matches() cannot be called without an operand.')

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

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    """Ensures memberOf is called on codes with a single valid URI string."""
    del walker

    # Ensure the operand is a code or collection of codes.
    if isinstance(operand.data_type, _fhir_path_data_types.Collection):
      if len(operand.data_type.types) != 1:
        return (
            'Unable to perform memberOf check against heterogenous collection '
            'with types %s'
            % operand.data_type.types
        )

      operand_type = list(operand.data_type.types)[0]
    else:
      operand_type = operand.data_type

    if (
        not _is_string_or_code(operand_type)
        and (not _is_coding(operand_type))
        and (not _is_codeable_concept(operand_type))
    ):
      return (
          'memberOf must be called on a string, code, coding or codeable '
          'concept, not %s' % operand_type
      )

    # Ensure memberOf is called with a single value set URI.
    params = function.params

    if len(params) != 1:
      return 'memberOf must be called with one value set URI.'

    if (
        not isinstance(params[0], _ast.Literal)
        or params[0].data_type != _fhir_path_data_types.String
    ):
      return (
          'memberOf must be called with a value set URI string, not %s.'
          % params[0]
      )

    value_set = params[0].value
    parsed = urllib.parse.urlparse(value_set)
    if not parsed.scheme and parsed.path:
      return 'memberOf must be called with a valid URI, not %s' % value_set

    if (
        options is not None
        and options.num_code_systems_per_value_set is not None
    ):
      # From the spec: If the valueset cannot be resolved as a uri to a value
      # set, an error is thrown.
      # https://build.fhir.org/fhirpath.html#functions
      num_code_systems = options.num_code_systems_per_value_set.get(value_set)
      if num_code_systems is None:
        return 'memberOf called with unknown value set: %s' % value_set

      # From the spec: When invoked on a string, returns true if the string is
      # equal to a code in the valueset, so long as the valueset only contains
      # one codesystem. If the valueset in this case contains more than one
      # codesystem, an error is thrown.
      # https://build.fhir.org/fhirpath.html#functions
      if num_code_systems > 1 and _is_string_or_code(operand_type):
        return (
            'memberOf called on string field but value set %s contains '
            'multiple code systems' % value_set
        )

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    return _type_of_mapping_over(_fhir_path_data_types.Boolean, operand)

  def __call__(  # pytype: disable=signature-mismatch  # overriding-parameter-type-checks
      self,
      function: _ast.Function,
      operand_result: _sql_data_types.IdentifierSelect,
      params_result: List[_sql_data_types.StandardSqlExpression],
      value_set_codes_table: Optional[str] = None,
      value_set_resolver: Optional[
          local_value_set_resolver.LocalResolver
      ] = None,
  ) -> _sql_data_types.Select:
    # Use the AST to figure out the type of the operand memberOf is called on.
    operand_node = function.parent.children[0]
    is_collection = isinstance(
        operand_node.data_type, _fhir_path_data_types.Collection
    )
    if is_collection:
      operand_type = list(operand_node.data_type.types)[0]
    else:
      operand_type = operand_node.data_type

    sql_alias = 'memberof_'

    # validate_and_get_error ensures the param is a literal.
    value_set_uri = cast(_ast.Literal, function.params[0]).value

    # See if the value set has a simple definition we can expand
    # ourselves. If so, expand the value set and generate an IN
    # expression validating if the codes in the column are members of
    # the value set.
    if value_set_resolver is not None:
      expanded_value_set = value_set_resolver.expand_value_set_url(
          value_set_uri
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
          value_set_uri,
          value_set_codes_table,
      )

    raise ValueError(
        'Unable to expand value set %s locally and no value set'
        ' definitions table provided. Unable to generate memberOf SQL.'
        % value_set_uri,
    )

  def _member_of_sql_against_inline_value_sets(
      self,
      sql_alias: str,
      operand_result: _sql_data_types.IdentifierSelect,
      operand_type: _fhir_path_data_types.FhirPathDataType,
      expanded_value_set: value_set_pb2.ValueSet,
  ) -> _sql_data_types.Select:
    """Generates memberOf SQL using an IN statement."""
    if _is_string_or_code(operand_type):
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

    if _is_coding(operand_type):
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
      operand_node: _ast.Expression,
      operand_type: _fhir_path_data_types.FhirPathDataType,
      value_set_param: str,
      value_set_codes_table: str,
  ) -> _sql_data_types.Select:
    """Generates memberOf SQL using a JOIN against a terminology table.."""
    is_collection = isinstance(
        operand_node.data_type, _fhir_path_data_types.Collection
    )

    is_string_or_code = _is_string_or_code(operand_type)
    is_coding = _is_coding(operand_type)
    is_codeable_concept = _is_codeable_concept(operand_type)

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

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, walker
    return _type_of_mapping_over(_fhir_path_data_types.String, operand)

  def __call__(  # pytype: disable=signature-mismatch  # overriding-parameter-type-checks
      self,
      function: _ast.Function,
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
    # reference target, e.g. patientId or organizationId. Therefore we
    # get the raw operand string and remove the surrounding quotes so we can
    # easily use it as part of a field name.
    sql_alias = 'idFor_'
    resource_type = params_result[0].as_operand().strip("'")
    # Make the first character lowercase to match the column names.
    resource_type = resource_type[:1].lower() + resource_type[1:]

    return dataclasses.replace(
        operand_result,
        select_part=operand_result.select_part.dot(
            f'{resource_type}Id', _sql_data_types.String, sql_alias=sql_alias
        ),
    )


# TODO(b/221322122): Consider refactoring this once we use a common tree.
class _OfTypeFunction(_FhirPathFunctionStandardSqlEncoder):
  """Returns the resource of the given type, typically used in choice types."""

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    is_collection = isinstance(
        operand.data_type, _fhir_path_data_types.Collection
    )
    chosen_type = str(function.params[0])

    # Propagate ofType's chosen type to the walker.
    walker.selected_choice_type = chosen_type

    input_fhir_type = _string_to_fhir_type(chosen_type)
    if is_collection:
      return _fhir_path_data_types.Collection(types={input_fhir_type})
    return input_fhir_type

  def __call__(  # pytype: disable=signature-mismatch  # overriding-parameter-type-checks
      self,
      function: _ast.Function,
      operand_result: Optional[_sql_data_types.IdentifierSelect],
      params_result: List[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('ofType() cannot be called without an operand.')

    if len(params_result) != 1:
      raise ValueError('OfType must have a data type parameter.')

    sql_alias = 'ofType_'
    operand_node = function.parent.children[0]
    is_collection = isinstance(
        operand_node.data_type, _fhir_path_data_types.Collection
    )
    attribute = str(function.params[0])

    return_type = _sql_data_types.Undefined
    if is_collection:
      return_type = _sql_data_types.OpaqueArray

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

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    del operand, walker, options
    params = function.params
    # If `criteria` is not given, or it's type is not bool, raise an error.
    if not params or params[0].data_type != _fhir_path_data_types.Boolean:
      param, data_type = (
          (params[0], params[0].data_type) if params else (None, None)
      )

      return (
          'Where function must have a `criteria` parameter that evaluates to '
          f'bool. Instead got `{param}` that evaluates to {data_type}.'
      )

    return None

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, walker
    # The return type of the where function is the type of its input collection.
    return operand.data_type

  def __call__(
      self,
      function: _ast.Function,
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

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    del operand, walker, options
    params = function.params
    # If `criteria` is not given, or it's type is not bool, raise an error.
    if not params or params[0].data_type != _fhir_path_data_types.Boolean:
      param, data_type = (
          (params[0], params[0].data_type) if params else (None, None)
      )

      return (
          'All function must have a `criteria` parameter that evaluates to '
          f'bool. Instead got `{param}` that evaluates to {data_type}.'
      )

    return None

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    del function, operand, walker
    return _fhir_path_data_types.Boolean

  def __call__(
      self,
      function: _ast.Function,
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
      if isinstance(
          function.parent.lhs.data_type, _fhir_path_data_types.Collection
      ):
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


def _is_string_or_code(
    fhir_type: _fhir_path_data_types.FhirPathDataType,
) -> bool:
  """Indicates if the type is a String or Code."""
  return isinstance(fhir_type, _fhir_path_data_types.String.__class__)


def _is_coding(fhir_type: _fhir_path_data_types.FhirPathDataType) -> bool:
  """Indicates if the type is a Coding."""
  return fhir_type.url == 'http://hl7.org/fhir/StructureDefinition/Coding'


def _is_codeable_concept(
    fhir_type: _fhir_path_data_types.FhirPathDataType,
) -> bool:
  """Indicates if the type is a Codeable Concept."""
  # If coedable concept is accessed through the `ofType` call, then it's type
  # comes up as `Any`. And because ofType is wired to only work with codeable
  # concepts we can assume that the incoming type is a coedable concept.
  # TODO(b/249832119): Handle this in a more deterministic way, because when
  # `ofType` gets extended to handle other types this will no longer be true.
  return (
      fhir_type.url == 'http://hl7.org/fhir/StructureDefinition/CodeableConcept'
      or fhir_type == _fhir_path_data_types.Any_
  )


def _string_to_fhir_type(
    type_code: str,
) -> _fhir_path_data_types.FhirPathDataType:
  """Returns the corresponding FHIR type for a given string if valid.

  Args:
   type_code: Relative URI for a potential FHIR type.

  Returns:
    This function returns Empty if the input string is not a valid FHIR type
    otherwise returns a corresponding _fhir_path_data_type.
  """
  type_code = type_code.lower()
  # String primitive has a different code than the other primitives.
  if type_code == 'string':
    return _fhir_path_data_types.String
  elif type_code == 'codeableconcept':
    # TODO(b/244184211): Refactor out needing to compute this mapping.
    return _fhir_path_data_types.Any_
  fhir_type = _fhir_path_data_types.primitive_type_from_type_code(type_code)
  if not fhir_type:
    return _fhir_path_data_types.Empty
  return fhir_type


def _type_of_mapping_over(
    fhir_type: _fhir_path_data_types.FhirPathDataType, operand: _ast.Expression
) -> _fhir_path_data_types.FhirPathDataType:
  """Finds the type of mapping `fhir_type` over `operand`.

  Args:
   fhir_type: The type being called in a mapping function over operand.
   operand: The operand being mapped over.

  Returns:
    `fhir_type` if `operand` is a scalar and Collection[`fhir_type`] if a
    collection.
  """
  if isinstance(operand.data_type, _fhir_path_data_types.Collection):
    return _fhir_path_data_types.Collection(types={fhir_type})
  else:
    return fhir_type


class _ToIntegerFunction(_FhirPathFunctionStandardSqlEncoder):
  """Casts the given operand to an integer.

  The spec for this function is found here:
  https://build.fhir.org/ig/HL7/FHIRPath/#integer-conversion-functions

  We differ from the spec in one respect. We do not return errors when called on
  collections with more than one element. Instead, we return the first element
  of the collection as an integer.
  """

  def validate_and_get_error(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ) -> Optional[str]:
    del function, walker, options
    if (
        isinstance(operand.data_type, _fhir_path_data_types.Collection)
        and len(operand.data_type.types) > 1
    ):
      return (
          'toInteger() can only process Collections with a single data type. '
          f'Got Collection with {operand.data_type.types} types.'
      )

  def return_type(
      self,
      function: _ast.Function,
      operand: _ast.Expression,
      walker: _navigation.FhirStructureDefinitionWalker,
  ) -> _fhir_path_data_types.FhirPathDataType:
    return _fhir_path_data_types.Integer

  def __call__(
      self,
      function: _ast.Function,
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
    operand_node = function.parent.children[0]
    if isinstance(operand_node.data_type, _fhir_path_data_types.Collection):
      operand_type = list(operand_node.data_type.types)[0]
    else:
      operand_type = operand_node.data_type

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
    if isinstance(operand_node.data_type, _fhir_path_data_types.Collection):
      first = FUNCTION_MAP[_ast.Function.Name.FIRST]
      operand_result = first(function, operand_result, [])

    return dataclasses.replace(
        operand_result,
        select_part=operand_result.select_part.cast(
            sql_data_type, _sql_alias=sql_alias
        ),
    )


FUNCTION_MAP: Dict[str, _FhirPathFunctionStandardSqlEncoder] = {
    _ast.Function.Name.COUNT: _CountFunction(),
    _ast.Function.Name.EMPTY: _EmptyFunction(),
    _ast.Function.Name.EXISTS: _ExistsFunction(),
    _ast.Function.Name.FIRST: _FirstFunction(),
    _ast.Function.Name.ANY_TRUE: _AnyTrueFunction(),
    _ast.Function.Name.HAS_VALUE: _HasValueFunction(),
    _ast.Function.Name.ID_FOR: _IdForFunction(),
    _ast.Function.Name.NOT: _NotFunction(),
    _ast.Function.Name.MATCHES: _MatchesFunction(),
    _ast.Function.Name.MEMBER_OF: _MemberOfFunction(),
    _ast.Function.Name.OF_TYPE: _OfTypeFunction(),
    _ast.Function.Name.WHERE: _WhereFunction(),
    _ast.Function.Name.ALL: _AllFunction(),
    _ast.Function.Name.TO_INTEGER: _ToIntegerFunction(),
}

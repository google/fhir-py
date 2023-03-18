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
"""Contains classes used for getting Spark SQL equivalents of FHIRPath functions."""

import copy
import dataclasses
from typing import Callable, Collection, Mapping, Optional

import immutabledict

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _sql_data_types


def count_function(
    function: _evaluation.CountFunction,
    operand_result: Optional[_sql_data_types.Select],
    params_result: Collection[_sql_data_types.StandardSqlExpression],
) -> _sql_data_types.Select:
  """Returns an integer representing the number of elements in a collection.

  By default, `_CountFunction` will return 0.

  Args:
    function: The FHIRPath AST `HasValueFunction` node
    operand_result: The expression which is being evaluated
    params_result: The parameter passed in to function

  Returns:
    A compiled Spark SQL expression.

  Raises:
    ValueError: When the function is called without an operand
  """
  del function, params_result  # Unused parameters in this function
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


def empty_function(
    function: _evaluation.EmptyFunction,
    operand_result: Optional[_sql_data_types.Select],
    params_result: Collection[_sql_data_types.StandardSqlExpression],
) -> _sql_data_types.Select:
  """Generates Spark SQL representing the FHIRPath empty() function.

  Returns `TRUE` if the input collection is empty and `FALSE` otherwise.

  This is the opposite of `_ExistsFunction`. If the operand is empty, then the
  result is `TRUE`.

  The returned SQL expression is a table of cardinality 1, whose value is of
  `BOOL` type. By default, `_EmptyFunction` will return `FALSE` if given no
  operand.

  Args:
    function: The FHIRPath AST `EmptyFunction` node
    operand_result: The expression which is being evaluated
    params_result: The parameter passed in to function

  Returns:
    A compiled Spark SQL expression.

  Raises:
    ValueError: When the function is called without an operand
  """
  del params_result  # Unused parameter in this function
  if operand_result is None:
    raise ValueError('empty() cannot be called without an operand.')

  sql_alias = 'empty_'
  sql_data_type = _sql_data_types.Boolean

  if not _fhir_path_data_types.returns_collection(
      function.parent_node().return_type()
  ):
    # We can use a less expensive scalar check.
    return dataclasses.replace(
        operand_result,
        select_part=operand_result.select_part.is_null(_sql_alias=sql_alias),
    )
  else:
    # We have to use a more expensive EXISTS check.
    return _sql_data_types.Select(
        select_part=_sql_data_types.RawExpression(
            'CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END',
            _sql_data_type=sql_data_type,
            _sql_alias=sql_alias,
        ),
        from_part=str(operand_result.to_subquery()),
        where_part=f'{operand_result.sql_alias} IS NOT NULL',
    )


def first_function(
    function: _evaluation.FirstFunction,
    operand_result: Optional[_sql_data_types.Select],
    params_result: Collection[_sql_data_types.StandardSqlExpression],
) -> _sql_data_types.Select:
  """Generates Spark SQL representing the FHIRPath first() function.

  Returns a collection with the first value of the operand collection.

  The returned SQL expression is a table with cardinality 0 or 1.

  Args:
    function: The FHIRPath AST `FirstFunction` node
    operand_result: The expression which is being evaluated
    params_result: The parameter passed in to function

  Returns:
    A compiled Spark SQL expression.

  Raises:
    ValueError: When the function is called without an operand
  """
  del function, params_result  # Unused parameters in this function
  if operand_result is None:
    raise ValueError('first() cannot be called without an operand.')

  # We append a limit 1 to get the first row in row order.
  # Note that if an ARRAY was unnested, row order may not match array order,
  # but for most FHIR this should not matter.
  result = copy.copy(operand_result)
  return _sql_data_types.Select(
      select_part=_sql_data_types.RawExpression(
          sql_expr=f'FIRST({result.sql_alias})',
          _sql_data_type=result.sql_data_type,
          _sql_alias=result.sql_alias,
      ),
      from_part=f'{result.to_subquery()}',
  )


def has_value_function(
    function: _evaluation.HasValueFunction,
    operand_result: Optional[_sql_data_types.Select],
    params_result: Collection[_sql_data_types.StandardSqlExpression],
) -> _sql_data_types.Select:
  """Generates Spark SQL representing the FHIRPath hasValue() function.

  Returns `TRUE` if the operand is a primitive with a `value` field.

  The input operand must be a collection of FHIR primitives of cardinality 1,
  and the value must be non-`NULL`.

  Args:
    function: The FHIRPath AST `HasValueFunction` node
    operand_result: The expression which is being evaluated
    params_result: The parameter passed in to function

  Returns:
    A compiled Spark SQL expression.

  Raises:
    ValueError: When the function is called without an operand
  """
  del function, params_result  # Unused parameters in this function
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
      select_part=operand_result.select_part.is_not_null(_sql_alias=sql_alias),
  )


def matches_function(
    function: _evaluation.MatchesFunction,
    operand_result: Optional[_sql_data_types.Select],
    params_result: Collection[_sql_data_types.StandardSqlExpression],
) -> _sql_data_types.Select:
  """Generates Spark SQL representing the FHIRPath matches() function.

  Returns `TRUE` if the operand matches the regex in the given param.

  This function takes one param (`pattern`) in addition to the operand. If
  `pattern` is not provided the matches function returns the empty set which in
  this function translates to NULL.

  The returned SQL expression is a table of cardinality 1, whose value is of
  `BOOL` type. By default, `_MatchesFunction` will return `FALSE` if given no
  operand.

  Returns an error In the event that the input collection contains multiple
  items.

  Args:
    function: The FHIRPath AST `MatchesFunction` node
    operand_result: The expression which is being evaluated
    params_result: The parameter passed in to function

  Returns:
    A compiled Spark SQL expression.

  Raises:
    ValueError: When the function is called without an operand
  """
  del function  # Unused parameter in this function
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
    param_to_evaluate = [param for param in params_result]
    return dataclasses.replace(
        operand_result,
        select_part=_sql_data_types.FunctionCall(
            name='REGEXP',
            params=(operand_result.select_part, param_to_evaluate[0]),
            _sql_alias=sql_alias,
            _sql_data_type=sql_data_type,
        ),
    )


FUNCTION_MAP: Mapping[str, Callable[..., _sql_data_types.Select]] = (
    immutabledict.immutabledict({
        _evaluation.CountFunction.NAME: count_function,
        _evaluation.EmptyFunction.NAME: empty_function,
        _evaluation.FirstFunction.NAME: first_function,
        _evaluation.HasValueFunction.NAME: has_value_function,
        _evaluation.MatchesFunction.NAME: matches_function,
    })
)

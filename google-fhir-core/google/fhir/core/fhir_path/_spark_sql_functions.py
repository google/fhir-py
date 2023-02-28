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

import abc
import dataclasses
from typing import Any, Iterable, Mapping, Optional

import immutabledict

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _sql_data_types


class _FhirPathFunctionSparkSqlEncoder(abc.ABC):
  """Returns a Spark SQL equivalent of a FHIRPath function."""

  def __call__(
      self,
      function: Any,
      operand_result: Optional[_sql_data_types.Select],
      params_result: Iterable[_sql_data_types.StandardSqlExpression],
      # Individual classes may specify additional kwargs they accept.
      **kwargs,
  ) -> _sql_data_types.Select:
    raise NotImplementedError('Subclasses *must* implement `__call__`.')


class _CountFunction(_FhirPathFunctionSparkSqlEncoder):
  """Returns an integer representing the number of elements in a collection.

  By default, `_CountFunction` will return 0.
  """

  def __call__(
      self,
      function: _evaluation.CountFunction,
      operand_result: Optional[_sql_data_types.Select],
      unused_params_result: Iterable[_sql_data_types.StandardSqlExpression],
  ) -> _sql_data_types.Select:
    if operand_result is None:
      raise ValueError('count() cannot be called without an operand.')

    if operand_result.from_part is None:
      # COUNT is an aggregation and requires a FROM. If there is not one
      # already, build a subquery for the FROM.
      return _sql_data_types.Select(
          select_part=_sql_data_types.CountCall(
              (
                  _sql_data_types.RawExpression(
                      operand_result.sql_alias,
                      _sql_data_type=operand_result.sql_data_type,
                  ),
              )
          ),
          from_part=str(operand_result.to_subquery()),
          where_part=operand_result.where_part,
      )
    else:
      # We don't need a sub-query because we already have a FROM.
      return dataclasses.replace(
          operand_result,
          select_part=_sql_data_types.CountCall((operand_result.select_part,)),
      )

FUNCTION_MAP: Mapping[str, _FhirPathFunctionSparkSqlEncoder] = (
    immutabledict.immutabledict(
        {_evaluation.CountFunction.NAME: _CountFunction()}
    )
)

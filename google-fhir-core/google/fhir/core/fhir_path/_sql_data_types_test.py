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
"""Tests basic container functionality for SQL data types."""

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import _sql_data_types


class DottedSelectStandardSqlExpressionTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='with_select_identifier',
          select_part=_sql_data_types.Identifier(
              'a', _sql_data_type=_sql_data_types.String
          ),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT a',
          expected_alias='a',
          expected_data_type=_sql_data_types.String,
      ),
      dict(
          testcase_name='with_select_identifier_alias',
          select_part=_sql_data_types.Identifier(
              'a', _sql_data_type=_sql_data_types.String, _sql_alias='apple'
          ),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT a AS apple',
          expected_alias='apple',
          expected_data_type=_sql_data_types.String,
      ),
      dict(
          testcase_name='with_identifier_to_subquery',
          select_part=_sql_data_types.Identifier(
              'a', _sql_data_type=_sql_data_types.String, _sql_alias='apple'
          ).to_subquery(),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT (SELECT a AS apple) AS apple',
          expected_alias='apple',
          expected_data_type=_sql_data_types.String,
      ),
      dict(
          testcase_name='with_select_identifier_path',
          select_part=_sql_data_types.Identifier(
              ('a', 'b'), _sql_data_type=_sql_data_types.String
          ),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT a.b',
          expected_alias='b',
          expected_data_type=_sql_data_types.String,
      ),
      dict(
          testcase_name='with_from_part',
          select_part=_sql_data_types.Identifier(
              ('a', 'b'), _sql_data_type=_sql_data_types.String
          ),
          from_part='c',
          limit_part=None,
          where_part=None,
          expected_sql='SELECT a.b\nFROM c',
          expected_alias='b',
          expected_data_type=_sql_data_types.String,
      ),
      dict(
          testcase_name='with_select_null',
          select_part=_sql_data_types.Identifier(
              ('a', 'b'), _sql_data_type=_sql_data_types.String
          ).is_null(),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT a.b IS NULL AS empty_',
          expected_alias='empty_',
          expected_data_type=_sql_data_types.Boolean,
      ),
      dict(
          testcase_name='with_select_not_null',
          select_part=_sql_data_types.Identifier(
              ('a', 'b'), _sql_data_type=_sql_data_types.String
          ).is_not_null(),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT a.b IS NOT NULL AS has_value_',
          expected_alias='has_value_',
          expected_data_type=_sql_data_types.Boolean,
      ),
      dict(
          testcase_name='with_subquery',
          select_part=_sql_data_types.Select(
              select_part=_sql_data_types.Identifier(
                  ('a', 'b'), _sql_data_type=_sql_data_types.String
              ),
              from_part=None,
          ).to_subquery(),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT (SELECT a.b) AS b',
          expected_alias='b',
          expected_data_type=_sql_data_types.String,
      ),
      dict(
          testcase_name='with_union',
          select_part=_sql_data_types.Select(
              select_part=_sql_data_types.Identifier(
                  'a', _sql_data_type=_sql_data_types.String
              ),
              from_part=None,
          )
          .union(
              _sql_data_types.Select(
                  select_part=_sql_data_types.Identifier(
                      'b', _sql_data_type=_sql_data_types.String
                  ),
                  from_part=None,
              ),
              True,
          )
          .to_subquery(),
          from_part='tbl',
          limit_part=None,
          where_part=None,
          expected_sql=(
              'SELECT (SELECT a\nUNION DISTINCT\nSELECT b) AS union_\nFROM tbl'
          ),
          expected_alias='union_',
          expected_data_type=_sql_data_types.String,
      ),
      dict(
          testcase_name='with_function_call',
          select_part=_sql_data_types.FunctionCall(
              'REGEXP_CONTAINS',
              [
                  _sql_data_types.Identifier(
                      ('a', 'b'), _sql_data_type=_sql_data_types.String
                  ),
                  _sql_data_types.RawExpression(
                      '"regex"', _sql_data_type=_sql_data_types.String
                  ),
              ],
              _sql_data_type=_sql_data_types.Boolean,
              _sql_alias='matches_',
          ),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT REGEXP_CONTAINS(\na.b, "regex") AS matches_',
          expected_alias='matches_',
          expected_data_type=_sql_data_types.Boolean,
      ),
      dict(
          testcase_name='with_nested_function_calls',
          select_part=_sql_data_types.FunctionCall(
              'NOT',
              [
                  _sql_data_types.FunctionCall(
                      'REGEXP_CONTAINS',
                      [
                          _sql_data_types.Identifier(
                              ('a', 'b'), _sql_data_type=_sql_data_types.String
                          ),
                          _sql_data_types.RawExpression(
                              '"regex"', _sql_data_type=_sql_data_types.String
                          ),
                      ],
                      _sql_data_type=_sql_data_types.Boolean,
                  )
              ],
              _sql_data_type=_sql_data_types.Boolean,
          ),
          from_part=None,
          limit_part=None,
          where_part=None,
          expected_sql='SELECT NOT(\nREGEXP_CONTAINS(\na.b, "regex")) AS not_',
          expected_alias='not_',
          expected_data_type=_sql_data_types.Boolean,
      ),
      dict(
          testcase_name='with_from_and_where_limit_int',
          select_part=_sql_data_types.Identifier(
              'a', _sql_data_type=_sql_data_types.String, _sql_alias='apple'
          ),
          from_part='c',
          limit_part=100,
          where_part='TRUE',
          expected_sql='SELECT a AS apple\nFROM c\nWHERE TRUE\nLIMIT 100',
          expected_alias='apple',
          expected_data_type=_sql_data_types.String,
      ),
  )
  def testToStr_rendersExpectedSQL(self, select_part, from_part, limit_part,
                                   where_part, expected_sql, expected_alias,
                                   expected_data_type):
    expression = _sql_data_types.Select(
        select_part=select_part,
        from_part=from_part,
        where_part=where_part,
        limit_part=limit_part)

    self.assertEqual(str(expression), expected_sql)
    self.assertEqual(expression.sql_alias, expected_alias)
    self.assertEqual(expression.sql_data_type, expected_data_type)


if __name__ == '__main__':
  absltest.main()

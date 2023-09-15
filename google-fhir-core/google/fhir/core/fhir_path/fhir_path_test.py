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
"""Tests Python FHIRPath functionality."""

import copy
import textwrap
from typing import Dict, List, Optional, cast
import unittest.mock

from google.cloud import bigquery

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.proto import fhirpath_replacement_list_pb2
from google.fhir.core.proto import validation_pb2
from google.fhir.r4.proto.core import codes_pb2
from google.fhir.r4.proto.core import datatypes_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core import fhir_errors
from google.fhir.core.fhir_path import _bigquery_interpreter
from google.fhir.core.fhir_path import _structure_definitions as sdefs
from google.fhir.core.fhir_path import context as context_lib
from google.fhir.core.fhir_path import fhir_path
from google.fhir.core.fhir_path import fhir_path_options
from google.fhir.core.fhir_path import fhir_path_test_base
from google.fhir.core.fhir_path import fhir_path_validator
from google.fhir.core.fhir_path import fhir_path_validator_v2
from google.fhir.r4 import primitive_handler


# TODO(b/244184211): Make FHIR-version agnostic (e.g. parameterize on module?)
# TODO(b/197976399): Move unit tests to snapshot testing framework.

# TODO(b/249835149): Add corresponding tests internally for all examples in the
# public FHIR views.


class FhirPathStandardSqlEncoderTest(
    fhir_path_test_base.FhirPathTestBase, parameterized.TestCase
):
  """Unit tests for `fhir_path.FhirPathStandardSqlEncoder`."""

  def assert_evaluation_node_sql_correct(
      self,
      structdef_name: str,
      fhir_path_expression: str,
      expected_sql_expression: str,
      select_scalars_as_array: bool = True,
      check_elm_nodes: bool = False,
      **bq_interpreter_args,
  ) -> None:
    builder = self.create_builder_from_str(structdef_name, fhir_path_expression)

    actual_sql_expression = _bigquery_interpreter.BigQuerySqlInterpreter(
        **bq_interpreter_args
    ).encode(builder, select_scalars_as_array=select_scalars_as_array)

    self.assertEqual(actual_sql_expression, expected_sql_expression)

  def test_element_defs_in_builder(self):
    """Tests passing element defs to children identifiers."""
    fhir_path_expression = 'bar.bats.struct.value'
    builder = self.create_builder_from_str('Foo', fhir_path_expression)

    expected_element_def = sdefs.build_element_definition(
        id_='Struct.value',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    self.assertEqual(
        builder.return_type.root_element_definition, expected_element_def
    )

  def test_element_defs_not_in_builder(self):
    """Tests that element defs will not exist if a function is called on the builder."""
    fhir_path_expression = 'bar.bats.struct'
    builder = self.create_builder_from_str('Foo', fhir_path_expression)
    expected_element_def = sdefs.build_element_definition(
        id_='Struct',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )

    self.assertEqual(
        builder.return_type.root_element_definition, expected_element_def
    )
    self.assertIsNone(builder.exists().return_type.root_element_definition)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_null',
          fhir_path_expression='{ }',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT NULL AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_true',
          fhir_path_expression='true',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT TRUE AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_false',
          fhir_path_expression='false',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT FALSE AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_string',
          fhir_path_expression="'Foo'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT \'Foo\' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_number_decimal',
          fhir_path_expression='3.14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT 3.14 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_number_large_decimal',
          # 32 decimal places
          fhir_path_expression='3.14141414141414141414141414141414',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT 3.14141414141414141414141414141414 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_number_integer',
          fhir_path_expression='314',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT 314 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_year',
          fhir_path_expression='@1970',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '1970' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_year_month',
          fhir_path_expression='@1970-01',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '1970-01' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_year_month_day',
          fhir_path_expression='@1970-01-01',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '1970-01-01' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_year_month_day_hours',
          fhir_path_expression='@2015-02-04T14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '2015-02-04T14' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_year_month_day_hours_minutes',
          fhir_path_expression='@2015-02-04T14:34',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '2015-02-04T14:34' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_year_month_day_hours_minutes_seconds',
          fhir_path_expression='@2015-02-04T14:34:28',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '2015-02-04T14:34:28' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name=(
              'with_date_time_year_month_day_hours_minutes_seconds_milli'
          ),
          fhir_path_expression='@2015-02-04T14:34:28.123',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '2015-02-04T14:34:28.123' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name=(
              'with_date_time_year_month_day_hours_minutes_seconds_milli_tz'
          ),
          fhir_path_expression='@2015-02-04T14:34:28.123+09:00',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '2015-02-04T14:34:28.123+09:00' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours',
          fhir_path_expression='@T14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours_minutes',
          fhir_path_expression='@T14:34',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14:34' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours_minutes_seconds',
          fhir_path_expression='@T14:34:28',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14:34:28' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours_minutes_seconds_milli',
          fhir_path_expression='@T14:34:28.123',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14:34:28.123' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_quantity',
          fhir_path_expression="10 'mg'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '10 \\'mg\\'' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_null',
          fhir_path_expression='{ }',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT NULL AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_true',
          fhir_path_expression='true',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT TRUE AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_false',
          fhir_path_expression='false',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT FALSE AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_string',
          fhir_path_expression="'Foo'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT \'Foo\' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_number_decimal',
          fhir_path_expression='3.14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT 3.14 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_number_large_decimal',
          # 32 decimal places
          fhir_path_expression='3.14141414141414141414141414141414',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT 3.14141414141414141414141414141414 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_number_integer',
          fhir_path_expression='314',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT 314 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_year',
          fhir_path_expression='@1970',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('1970-01-01' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_year_month',
          fhir_path_expression='@1970-01',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('1970-01-01' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_year_month_day',
          fhir_path_expression='@1970-01-01',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('1970-01-01' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_year_month_day_hours',
          fhir_path_expression='@2015-02-04T14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_year_month_day_hours_minutes',
          fhir_path_expression='@2015-02-04T14:34',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('2015-02-04T14:34:00+00:00' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_year_month_day_hours_minutes_seconds',
          fhir_path_expression='@2015-02-04T14:34:28',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name=(
              'with_date_time_year_month_day_hours_minutes_seconds_milli'
          ),
          fhir_path_expression='@2015-02-04T14:34:28.123',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('2015-02-04T14:34:28.123000+00:00' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name=(
              'with_date_time_year_month_day_hours_minutes_seconds_milli_tz'
          ),
          fhir_path_expression='@2015-02-04T14:34:28.123+09:00',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT SAFE_CAST('2015-02-04T14:34:28.123000+09:00' AS TIMESTAMP) AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours',
          fhir_path_expression='@T14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours_minutes',
          fhir_path_expression='@T14:34',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14:34' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours_minutes_seconds',
          fhir_path_expression='@T14:34:28',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14:34:28' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_time_hours_minutes_seconds_milli',
          fhir_path_expression='@T14:34:28.123',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '14:34:28.123' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_quantity',
          fhir_path_expression="10 'mg'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT '10 \\'mg\\'' AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_equal',
          fhir_path_expression='@2015-02-04T14:34:28 = @2015-02-04T14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (SAFE_CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) = SAFE_CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP)) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_equivalent',
          fhir_path_expression='@2015-02-04T14:34:28 ~ @2015-02-04T14',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (SAFE_CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) = SAFE_CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP)) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_v2_date_time_literal_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
  ):
    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
    )

  def test_encode_with_fhir_path_v2_select_scalars_as_array_false_for_literal_succeeds(
      self,
  ):
    fhir_path_expression = 'true'
    expected_sql_expression = '(SELECT TRUE AS literal_)'
    self.assert_evaluation_node_sql_correct(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=False,
    )

  def test_encode_select_scalars_as_array_false_for_literal_succeeds(self):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=None,
        fhir_path_expression='true',
        select_scalars_as_array=False,
    )
    expected_sql_expression = '(SELECT TRUE AS literal_)'
    self.assertEqual(actual_sql_expression, expected_sql_expression)

  def test_encode_with_no_element_definition_given_succeeds(self):
    fhir_path_expression = "inline.value = 'abc'"
    expected_sql_expression = textwrap.dedent("""\
          (SELECT (inline.value = 'abc') AS eq_)""")
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=None,
        fhir_path_expression=fhir_path_expression,
        select_scalars_as_array=False,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
        select_scalars_as_array=False,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_integer_addition',
          fhir_path_expression='1 + 2',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (1 + 2) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_addition',
          fhir_path_expression='3.14 + 1.681',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (3.14 + 1.681) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_division',
          fhir_path_expression='3 / 2',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (3 / 2) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_division',
          fhir_path_expression='3.14 / 1.681',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (3.14 / 1.681) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_modular_arithmetic',
          fhir_path_expression='2 mod 5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT MOD(2, 5) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_multiplication',
          fhir_path_expression='2 * 5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (2 * 5) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_multiplication',
          fhir_path_expression='2.124 * 5.72',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (2.124 * 5.72) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_subtraction',
          fhir_path_expression='2 - 5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (2 - 5) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_subtraction',
          fhir_path_expression='2.124 - 5.72',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (2.124 - 5.72) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_trunctated_division',
          fhir_path_expression='2 div 5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT DIV(2, 5) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_truncated_division',
          fhir_path_expression='2.124 div 5.72',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT DIV(2.124, 5.72) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_addition_and_multiplication',
          fhir_path_expression='(1 + 2) * 3',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT ((1 + 2) * 3) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_subtraction_and_division',
          fhir_path_expression='(21 - 6) / 3',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT ((21 - 6) / 3) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_addition_and_modular_arithmetic',
          fhir_path_expression='21 + 6 mod 5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (21 + MOD(6, 5)) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_addition_and_truncated_division',
          fhir_path_expression='21 + 6 div 5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (21 + DIV(6, 5)) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_string_concatenation_ampersand',
          fhir_path_expression="'foo' & 'bar'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT CONCAT('foo', 'bar') AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_string_concatenation_plus',
          fhir_path_expression="'foo' + 'bar'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT CONCAT('foo', 'bar') AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_arithmetic_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_boolean_and',
          fhir_path_expression='true and false',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT logic_
          FROM (SELECT (TRUE AND FALSE) AS logic_)
          WHERE logic_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_greater_than',
          fhir_path_expression='4 > 3',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT comparison_
          FROM (SELECT (4 > 3) AS comparison_)
          WHERE comparison_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_less_than',
          fhir_path_expression='3 < 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT comparison_
          FROM (SELECT (3 < 4) AS comparison_)
          WHERE comparison_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_less_than_or_equal_to',
          fhir_path_expression='3 <= 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT comparison_
          FROM (SELECT (3 <= 4) AS comparison_)
          WHERE comparison_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_less_than',
          fhir_path_expression='dateField < @2000-01-01',
          different_from_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT comparison_
          FROM (SELECT (SAFE_CAST(dateField AS TIMESTAMP) < SAFE_CAST('2000-01-01' AS TIMESTAMP)) AS comparison_)
          WHERE comparison_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='date_compared_with_timestamp',
          fhir_path_expression='dateField < @2000-01-01T14:34',
          different_from_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT comparison_
          FROM (SELECT (SAFE_CAST(dateField AS TIMESTAMP) < SAFE_CAST('2000-01-01T14:34:00+00:00' AS TIMESTAMP)) AS comparison_)
          WHERE comparison_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_or',
          fhir_path_expression='true or false',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT logic_
          FROM (SELECT (TRUE OR FALSE) AS logic_)
          WHERE logic_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_xor',
          fhir_path_expression='true xor false',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT logic_
          FROM (SELECT (TRUE <> FALSE) AS logic_)
          WHERE logic_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_implies',
          fhir_path_expression='true implies false',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT logic_
          FROM (SELECT (NOT TRUE OR FALSE) AS logic_)
          WHERE logic_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_boolean_relation_between_string_integer',
          fhir_path_expression="3 and 'true'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT logic_
          FROM (SELECT ((3 IS NOT NULL) AND ('true' IS NOT NULL)) AS logic_)
          WHERE logic_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_logical_relation_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      different_from_v2: bool = False,
  ):
    if not different_from_v2:
      actual_sql_expression = self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )
      self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
    )

  # TODO(b/191895721): Verify order-dependence of equivalence vs. equality
  @parameterized.named_parameters(
      dict(
          testcase_name='with_integer_equal',
          fhir_path_expression='3 = 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (3 = 4) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_equivalent',
          fhir_path_expression='3 ~ 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (3 = 4) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_equal',
          fhir_path_expression='@2015-02-04T14:34:28 = @2015-02-04T14',
          different_from_v2=True,
          check_elm_nodes=False,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT ('2015-02-04T14:34:28' = '2015-02-04T14') AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_date_time_equivalent',
          fhir_path_expression='@2015-02-04T14:34:28 ~ @2015-02-04T14',
          different_from_v2=True,
          check_elm_nodes=False,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT ('2015-02-04T14:34:28' = '2015-02-04T14') AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_not_equal_to',
          fhir_path_expression='3 != 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (3 != 4) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_not_equivalent_to',
          fhir_path_expression='3 !~ 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (3 != 4) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_equality_relation_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      different_from_v2: bool = True,
      check_elm_nodes: bool = True,
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    if not different_from_v2:
      self.assert_evaluation_node_sql_correct(
          'Foo',
          fhir_path_expression,
          expected_sql_expression,
          check_elm_nodes=check_elm_nodes,
      )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_equals_and_identifier_and_string_literal',
          fhir_path_expression="text = ''",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (text = '') AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_equivalent_and_identifier_and_string_literal',
          fhir_path_expression="text ~ ''",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (text = '') AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_not_equal_and_identifier_and_string_literal',
          fhir_path_expression="text != ''",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (text != '') AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_not_equivalent_and_two_identifiers',
          fhir_path_expression='text !~ text',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (text != text) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_equal_and_two_identifiers',
          fhir_path_expression='text = text',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT (text = text) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_is_not_null_operator',
          fhir_path_expression='text.exists() = true',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT ((text IS NOT NULL) = TRUE) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_is_null_operator',
          fhir_path_expression='text.empty() = true',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT eq_
          FROM (SELECT ((text IS NULL) = TRUE) AS eq_)
          WHERE eq_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_equality_relation_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.div,
        element_definition=self.div_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Div',
        fhir_path_expression,
        expected_sql_expression,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_integer_in',
          fhir_path_expression='3 in 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT mem_
          FROM (SELECT (3)
          IN (4) AS mem_)
          WHERE mem_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_contains',
          fhir_path_expression='3 contains 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT mem_
          FROM (SELECT (4)
          IN (3) AS mem_)
          WHERE mem_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_membership_relation_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_integer_union',
          fhir_path_expression='3 | 4',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT union_
          FROM (SELECT lhs_.literal_ AS union_
          FROM (SELECT 3 AS literal_) AS lhs_
          UNION DISTINCT
          SELECT rhs_.literal_ AS union_
          FROM (SELECT 4 AS literal_) AS rhs_)
          WHERE union_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_string_union',
          fhir_path_expression="'Foo' | 'Bar'",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT union_
          FROM (SELECT lhs_.literal_ AS union_
          FROM (SELECT 'Foo' AS literal_) AS lhs_
          UNION DISTINCT
          SELECT rhs_.literal_ AS union_
          FROM (SELECT 'Bar' AS literal_) AS rhs_)
          WHERE union_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_string_nested_union',
          fhir_path_expression="('Foo' | 'Bar') | ('Bats')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT union_
          FROM (SELECT lhs_.union_
          FROM (SELECT lhs_.literal_ AS union_
          FROM (SELECT 'Foo' AS literal_) AS lhs_
          UNION DISTINCT
          SELECT rhs_.literal_ AS union_
          FROM (SELECT 'Bar' AS literal_) AS rhs_) AS lhs_
          UNION DISTINCT
          SELECT rhs_.literal_ AS union_
          FROM (SELECT 'Bats' AS literal_) AS rhs_)
          WHERE union_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_union_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_integer_positive_polarity',
          fhir_path_expression='+5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT +5 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_positive_polarity',
          fhir_path_expression='+5.72',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT +5.72 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_negative_polarity',
          fhir_path_expression='-5',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT -5 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_negative_polarity',
          fhir_path_expression='-5.1349',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT literal_
          FROM (SELECT -5.1349 AS literal_)
          WHERE literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_positive_polarity_and_addition',
          fhir_path_expression='+5 + 10',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (+5 + 10) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_negative_polarity_and_addition',
          fhir_path_expression='-5 + 10',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (-5 + 10) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_negative_polarity_and_modular_arithmetic',
          fhir_path_expression='-5 mod 6',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT MOD(-5, 6) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_positive_polarity_and_modular_arithmetic',
          fhir_path_expression='+(7 mod 6)',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT pol_
          FROM (SELECT +MOD(7, 6) AS pol_)
          WHERE pol_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_negative_polarity_and_multiplication',
          fhir_path_expression='-(3.79 * 2.124)',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT pol_
          FROM (SELECT -(3.79 * 2.124) AS pol_)
          WHERE pol_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_decimal_negative_polarity_and_division',
          fhir_path_expression='-3.79 / 2.124',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT arith_
          FROM (SELECT (-3.79 / 2.124) AS arith_)
          WHERE arith_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_polarity_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_integer_indexer',
          fhir_path_expression='7[0]',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT indexed_literal_
          FROM (SELECT literal_ AS indexed_literal_
          FROM (SELECT ROW_NUMBER() OVER() AS row_,
          literal_
          FROM (SELECT 7 AS literal_)) AS inner_tbl
          WHERE (inner_tbl.row_ - 1) = 0)
          WHERE indexed_literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_integer_indexer_arithmetic_index',
          fhir_path_expression='7[0 + 1]',  # Out-of-bounds, empty table
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT indexed_literal_
          FROM (SELECT literal_ AS indexed_literal_
          FROM (SELECT ROW_NUMBER() OVER() AS row_,
          literal_
          FROM (SELECT 7 AS literal_)) AS inner_tbl
          WHERE (inner_tbl.row_ - 1) = (0 + 1))
          WHERE indexed_literal_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_string_indexer',
          fhir_path_expression="'foo'[0]",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT indexed_literal_
          FROM (SELECT literal_ AS indexed_literal_
          FROM (SELECT ROW_NUMBER() OVER() AS row_,
          literal_
          FROM (SELECT 'foo' AS literal_)) AS inner_tbl
          WHERE (inner_tbl.row_ - 1) = 0)
          WHERE indexed_literal_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_literal_indexer_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='index_field',
          fhir_path_expression='codeFlavors[0]',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT indexed_codeFlavors_element_
          FROM (SELECT codeFlavors_element_ AS indexed_codeFlavors_element_
          FROM (SELECT ROW_NUMBER() OVER() AS row_,
          codeFlavors_element_
          FROM (SELECT codeFlavors_element_
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset)) AS inner_tbl
          WHERE (inner_tbl.row_ - 1) = 0)
          WHERE indexed_codeFlavors_element_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_index_field_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(testcase_name='with_exists', fhir_path_expression='exists()'),
      dict(testcase_name='with_empty', fhir_path_expression='empty()'),
      dict(testcase_name='with_count', fhir_path_expression='count()'),
      dict(testcase_name='with_has_value', fhir_path_expression='hasValue()'),
      dict(
          testcase_name='with_matches', fhir_path_expression="matches('regex')"
      ),
      dict(
          testcase_name='with_matches_and_no_params',
          fhir_path_expression='matches()',
      ),
  )
  def test_encode_with_fhir_path_function_no_operand_raises_error(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(ValueError):
      self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )

    builder = self.create_builder_from_str('Foo', fhir_path_expression)
    with self.assertRaises(ValueError):
      _bigquery_interpreter.BigQuerySqlInterpreter().encode(builder)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_not',
          fhir_path_expression="(' ' contains 'history').not()",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT not_
          FROM (SELECT NOT(
          ('history')
          IN (' ')) AS not_)
          WHERE not_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_function_none_type_operand_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_collections_and_scalars_as_array_true',
          fhir_path_expression='bar.bats',
          select_scalars_as_array=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE bats_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_collections_and_scalars_as_array_false',
          fhir_path_expression='bar.bats',
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE bats_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_and_scalars_as_array_default',
          fhir_path_expression='inline.value',
          select_scalars_as_array=None,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT value
          FROM (SELECT inline.value)
          WHERE value IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_and_scalars_as_array_true',
          fhir_path_expression='inline.value',
          select_scalars_as_array=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT value
          FROM (SELECT inline.value)
          WHERE value IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_and_scalars_as_array_false',
          fhir_path_expression='inline.value',
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          (SELECT inline.value)"""),
      ),
      dict(
          testcase_name='with_scalar_comparison_and_scalars_as_array_false',
          fhir_path_expression="inline.value = 'abc'",
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          (SELECT (inline.value = 'abc') AS eq_)"""),
      ),
      dict(
          testcase_name='with_array_equality_as_array_false',
          fhir_path_expression='bar.bats.struct.value = stringList',
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent(
              """\
          (SELECT NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, value
          FROM (SELECT bats_element_.struct.value
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, stringList_element_
          FROM (SELECT stringList_element_
          FROM UNNEST(stringList) AS stringList_element_ WITH OFFSET AS element_offset)) AS rhs_) AS eq_)"""
          ),
      ),
      dict(
          testcase_name=(
              'with_scalar_complex_comparison_and_scalars_as_array_false'
          ),
          fhir_path_expression="bar.bats.struct.value = ('abc' | '123')",
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          (SELECT NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, value
          FROM (SELECT bats_element_.struct.value
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, union_
          FROM (SELECT lhs_.literal_ AS union_
          FROM (SELECT 'abc' AS literal_) AS lhs_
          UNION DISTINCT
          SELECT rhs_.literal_ AS union_
          FROM (SELECT '123' AS literal_) AS rhs_)) AS rhs_) AS eq_)"""),
      ),
  )
  def test_encode_with_select_scalars_as_array_generates_sql(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      select_scalars_as_array: Optional[bool],
  ):
    """Ensures the select_scalars_as_array flag is respected."""
    kwargs = {}
    if select_scalars_as_array is not None:
      kwargs['select_scalars_as_array'] = select_scalars_as_array

    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
        **kwargs,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
        **kwargs,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_choice_no_type',
          fhir_path_expression='choiceExample',
          # Return full choice structure if no sub-type is specified.
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT choiceExample
          FROM (SELECT choiceExample)
          WHERE choiceExample IS NOT NULL)"""),
          select_scalars_as_array=True,
      ),
      dict(
          testcase_name='with_choice_string_type',
          fhir_path_expression="choiceExample.ofType('string')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT ofType_
          FROM (SELECT choiceExample.string AS ofType_)
          WHERE ofType_ IS NOT NULL)"""),
          select_scalars_as_array=True,
      ),
      dict(
          testcase_name='with_choice_integer_type',
          fhir_path_expression="choiceExample.ofType('integer')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT ofType_
          FROM (SELECT choiceExample.integer AS ofType_)
          WHERE ofType_ IS NOT NULL)"""),
          select_scalars_as_array=True,
      ),
      dict(
          testcase_name='scalar_with_choice_integer_type',
          fhir_path_expression="choiceExample.ofType('integer')",
          expected_sql_expression=textwrap.dedent("""\
          (SELECT choiceExample.integer AS ofType_)"""),
          select_scalars_as_array=False,
      ),
      dict(
          testcase_name='array_with_choice',
          fhir_path_expression="multipleChoiceExample.ofType('integer')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT ofType_
          FROM (SELECT multipleChoiceExample_element_.integer AS ofType_
          FROM UNNEST(multipleChoiceExample) AS multipleChoiceExample_element_ WITH OFFSET AS element_offset)
          WHERE ofType_ IS NOT NULL)"""),
          select_scalars_as_array=False,
      ),
      dict(
          testcase_name='scalar_with_function',
          fhir_path_expression=(
              "choiceExample.ofType('CodeableConcept').exists()"
          ),
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          (SELECT choiceExample.CodeableConcept IS NOT NULL AS exists_)"""),
      ),
      dict(
          testcase_name='scalar_with_repeated_message_choice',
          fhir_path_expression="choiceExample.ofType('CodeableConcept').coding",
          # This option shouldn't matter in this case, but
          # .coding is returning an empty type object.
          select_scalars_as_array=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT coding_element_
          FROM (SELECT coding_element_
          FROM (SELECT choiceExample.CodeableConcept AS ofType_),
          UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset)
          WHERE coding_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='array_with_message_choice',
          fhir_path_expression=(
              "multipleChoiceExample.ofType('CodeableConcept').coding"
          ),
          # This option shouldn't matter in this case, but
          # .coding is returning an empty type object.
          select_scalars_as_array=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT coding_element_
          FROM (SELECT coding_element_
          FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS ofType_
          FROM UNNEST(multipleChoiceExample) AS multipleChoiceExample_element_ WITH OFFSET AS element_offset),
          UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset)
          WHERE coding_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='polymorphic_array_exists',
          fhir_path_expression='multipleChoiceExample.exists()',
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          (SELECT EXISTS(
          SELECT multipleChoiceExample_element_
          FROM (SELECT multipleChoiceExample_element_
          FROM UNNEST(multipleChoiceExample) AS multipleChoiceExample_element_ WITH OFFSET AS element_offset)
          WHERE multipleChoiceExample_element_ IS NOT NULL) AS exists_)"""),
      ),
      dict(
          testcase_name='array_with_function',
          fhir_path_expression=(
              "multipleChoiceExample.ofType('CodeableConcept').exists()"
          ),
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          (SELECT EXISTS(
          SELECT ofType_
          FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS ofType_
          FROM UNNEST(multipleChoiceExample) AS multipleChoiceExample_element_ WITH OFFSET AS element_offset)
          WHERE ofType_ IS NOT NULL) AS exists_)"""),
      ),
      dict(
          testcase_name='array_matches_all',
          fhir_path_expression="inline.numbers.all($this.matches('regex'))",
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent(
              """\
              (SELECT IFNULL(
              LOGICAL_AND(
              IFNULL(
              (SELECT REGEXP_CONTAINS(
              numbers_element_, 'regex') AS all_), FALSE)), TRUE) AS all_
              FROM (SELECT inline),
              UNNEST(inline.numbers) AS numbers_element_ WITH OFFSET AS element_offset)"""
          ),
      ),
      dict(
          testcase_name='array_with_message_choice_and_identifier',
          fhir_path_expression=(
              "multipleChoiceExample.ofType('CodeableConcept').coding.system"
          ),
          select_scalars_as_array=False,
          only_works_in_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT system
          FROM (SELECT coding_element_.system
          FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS ofType_
          FROM UNNEST(multipleChoiceExample) AS multipleChoiceExample_element_ WITH OFFSET AS element_offset),
          UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset)
          WHERE system IS NOT NULL)"""),
      ),
      dict(
          testcase_name='array_with_message_choice_and_equality',
          fhir_path_expression=(
              "multipleChoiceExample.ofType('CodeableConcept').coding.system ="
              " 'test'"
          ),
          select_scalars_as_array=False,
          only_works_in_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          (SELECT NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, system
          FROM (SELECT coding_element_.system
          FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS ofType_
          FROM UNNEST(multipleChoiceExample) AS multipleChoiceExample_element_ WITH OFFSET AS element_offset),
          UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
          FROM (SELECT 'test' AS literal_)) AS rhs_) AS eq_)"""),
      ),
      dict(
          testcase_name='array_with_message_choice_and_where',
          fhir_path_expression=(
              "multipleChoiceExample.ofType('CodeableConcept').coding.where(system"
              " = 'test')"
          ),
          only_works_in_v2=True,
          select_scalars_as_array=False,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT coding_element_
          FROM (SELECT coding_element_
          FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS ofType_
          FROM UNNEST(multipleChoiceExample) AS multipleChoiceExample_element_ WITH OFFSET AS element_offset),
          UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset
          WHERE NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, system
          FROM (SELECT system)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
          FROM (SELECT 'test' AS literal_)) AS rhs_))
          WHERE coding_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='scalar_with_repeated_message_choice_and_where',
          fhir_path_expression=(
              "choiceExample.ofType('CodeableConcept').coding.where(system ="
              " 'test')"
          ),
          select_scalars_as_array=False,
          only_works_in_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT coding_element_
          FROM (SELECT coding_element_
          FROM (SELECT choiceExample.CodeableConcept AS ofType_),
          UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset
          WHERE NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, system
          FROM (SELECT system)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
          FROM (SELECT 'test' AS literal_)) AS rhs_))
          WHERE coding_element_ IS NOT NULL)"""),
      ),
  )
  def test_encode_choice_type_generates_sql(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      select_scalars_as_array: Optional[bool],
      only_works_in_v2: bool = False,
  ):
    kwargs = {}
    if select_scalars_as_array is not None:
      kwargs['select_scalars_as_array'] = select_scalars_as_array
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
        **kwargs,
    )

    if not only_works_in_v2:
      self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression, **kwargs
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_string_field_generates_cast_sql',
          fhir_path_expression='bat.struct.value.toInteger()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT to_integer_
          FROM (SELECT CAST(
          bat.struct.value AS INT64) AS to_integer_)
          WHERE to_integer_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_un_castable_type_generates_empty_sql',
          fhir_path_expression='codeFlavor.coding.toInteger()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT to_integer_
          FROM (SELECT NULL AS to_integer_)
          WHERE to_integer_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_call_against_field_in_collection_applies_limit',
          fhir_path_expression='bar.bats.struct.value.toInteger()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT to_integer_
          FROM (SELECT CAST(
          bats_element_.struct.value AS INT64) AS to_integer_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset
          LIMIT 1)
          WHERE to_integer_ IS NOT NULL)"""),
      ),
  )
  def test_encode_to_integer_(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )

    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_binary_expression',
          fhir_path_expression='choiceExample + choiceExample',
      ),
      dict(
          testcase_name='with_not', fhir_path_expression='choiceExample.not()'
      ),
      dict(
          testcase_name='with_to_integer',
          fhir_path_expression='choiceExample.toInteger()',
      ),
      dict(
          testcase_name='with_id_for',
          fhir_path_expression="choiceExample.idFor('Patient')",
      ),
      dict(
          testcase_name='with_matches',
          fhir_path_expression="choiceExample.matches('regex')",
      ),
      dict(
          testcase_name='with_matches_and_no_params',
          fhir_path_expression='choiceExample.matches()',
      ),
  )
  def test_encode_functions_error_on_choice_type(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(ValueError):
      self.create_builder_from_str('Foo', fhir_path_expression)

  def test_encode_to_integer_validation_with_params_provided_raises_error(self):
    with self.assertRaises(ValueError):
      self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression='bat.struct.value.toInteger(123)',
      )

    with self.assertRaises(ValueError):
      self.create_builder_from_str('Foo', 'bat.struct.value.toInteger(123)')

  @parameterized.named_parameters(
      dict(
          testcase_name='with_not',
          fhir_path_expression="' '.contains('history').not()",
      )
  )
  def test_encode_with_invalid_fhir_path_expression_fails(
      self, fhir_path_expression: str
  ):
    with self.assertRaisesRegex(ValueError, 'Unsupported FHIRPath expression.'):
      _ = self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )

  def test_encode_with_unsupported_exists_parameter(self):
    builder = self.create_builder_from_str(
        'Foo', 'bar.bats.struct.exists(true)'
    )
    with self.assertRaisesRegex(ValueError, 'Unsupported FHIRPath expression'):
      _bigquery_interpreter.BigQuerySqlInterpreter().encode(
          builder, select_scalars_as_array=True
      )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_single_member_access',
          fhir_path_expression='bar',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT bar
          FROM (SELECT bar)
          WHERE bar IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_inline_member_access',
          fhir_path_expression='inline',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT inline
          FROM (SELECT inline)
          WHERE inline IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_nested_member_access',
          fhir_path_expression='bar.bats',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE bats_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_inline_nested_member_access',
          fhir_path_expression='inline.value',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT value
          FROM (SELECT inline.value)
          WHERE value IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_nested_member_sql_keyword_access',
          fhir_path_expression='bar.bats.struct',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT `struct`
          FROM (SELECT bats_element_.struct
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE `struct` IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_nested_member_fhir_path_keyword_access',
          fhir_path_expression='bar.bats.`div`',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT div
          FROM (SELECT bats_element_.div
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE div IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_nested_scalar_member_fhir_path_access',
          fhir_path_expression='bat.struct.anotherStruct.anotherValue',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT anotherValue
          FROM (SELECT bat.struct.anotherStruct.anotherValue)
          WHERE anotherValue IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_reference',
          v2_only=True,
          fhir_path_expression='ref.reference',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT reference
          FROM (SELECT IFNULL(ref.patientId, IFNULL(ref.observationId, ref.deviceId)) AS reference)
          WHERE reference IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_member_invocation_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      v2_only=False,
  ):
    if not v2_only:
      actual_sql_expression = self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )
      self.assertEqual(actual_sql_expression, expected_sql_expression)

    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_deepest_nested_member_sql_keyword_exists',
          fhir_path_expression='bar.bats.struct.exists()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT `struct`
          FROM (SELECT bats_element_.struct
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE `struct` IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_nested_member_fhir_path_keyword_exists',
          fhir_path_expression='bar.bats.`div`.exists()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT div
          FROM (SELECT bats_element_.div
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE div IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_member_exists_not',
          fhir_path_expression='bar.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT not_
          FROM (SELECT NOT(
          bar IS NOT NULL) AS not_)
          WHERE not_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_nested_member_exists_not',
          fhir_path_expression='bar.bats.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE bats_element_ IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_first',
          fhir_path_expression='bar.bats.first()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset
          LIMIT 1)
          WHERE bats_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_first_on_non_collection',
          fhir_path_expression='bar.first()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT bar
          FROM (SELECT bar
          LIMIT 1)
          WHERE bar IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_any_true',
          fhir_path_expression='boolList.anyTrue()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT _anyTrue
          FROM (SELECT LOGICAL_OR(
          boolList_element_) AS _anyTrue
          FROM (SELECT boolList_element_
          FROM UNNEST(boolList) AS boolList_element_ WITH OFFSET AS element_offset))
          WHERE _anyTrue IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_nested_member_sql_keyword_exists_not',
          fhir_path_expression='bar.bats.struct.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT `struct`
          FROM (SELECT bats_element_.struct
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE `struct` IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_member_empty',
          fhir_path_expression='bar.empty()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT empty_
          FROM (SELECT bar IS NULL AS empty_)
          WHERE empty_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_nested_member_sql_keyword_empty',
          fhir_path_expression='bar.bats.struct.empty()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT empty_
          FROM (SELECT NOT EXISTS(
          SELECT `struct`
          FROM (SELECT bats_element_.struct
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE `struct` IS NOT NULL) AS empty_)
          WHERE empty_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_member_count',
          fhir_path_expression='bar.count()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT count_
          FROM (SELECT COUNT(
          bar) AS count_
          FROM (SELECT bar))
          WHERE count_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_nested_member_sql_keyword_count',
          fhir_path_expression='bar.bats.struct.count()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT count_
          FROM (SELECT COUNT(
          bats_element_.struct) AS count_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE count_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_member_has_value',
          fhir_path_expression='bar.hasValue()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT has_value_
          FROM (SELECT bar IS NOT NULL AS has_value_)
          WHERE has_value_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deepest_member_sql_keyword_has_value',
          fhir_path_expression='bar.bats.struct.hasValue()',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT has_value_
          FROM (SELECT bats_element_.struct IS NOT NULL AS has_value_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE has_value_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_deep_member_matches',
          fhir_path_expression=(
              "bat.struct.anotherStruct.anotherValue.matches('foo_regex')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT matches_
          FROM (SELECT REGEXP_CONTAINS(
          bat.struct.anotherStruct.anotherValue, 'foo_regex') AS matches_)
          WHERE matches_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_logic_on_exists',
          fhir_path_expression=(
              '(bar.bats.struct.value.exists() and'
              ' bar.bats.struct.anotherValue.exists()).not()'
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT not_
          FROM (SELECT NOT(
          (EXISTS(
          SELECT value
          FROM (SELECT bats_element_.struct.value
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE value IS NOT NULL) AND EXISTS(
          SELECT anotherValue
          FROM (SELECT bats_element_.struct.anotherValue
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE anotherValue IS NOT NULL))) AS not_)
          WHERE not_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_code_member_of',
          fhir_path_expression=(
              "codeFlavor.code.memberOf('http://value.set/id')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT memberof_
          FROM UNNEST((SELECT IF(codeFlavor.code IS NULL, [], [
          EXISTS(
          SELECT 1
          FROM `VALUESET_VIEW` vs
          WHERE
          vs.valueseturi='http://value.set/id'
          AND vs.code=codeFlavor.code
          )]))) AS memberof_)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_code_member_of_value_set_version',
          fhir_path_expression=(
              "codeFlavor.code.memberOf('http://value.set/id|1.0')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT memberof_
          FROM UNNEST((SELECT IF(codeFlavor.code IS NULL, [], [
          EXISTS(
          SELECT 1
          FROM `VALUESET_VIEW` vs
          WHERE
          vs.valueseturi='http://value.set/id'
          AND vs.valuesetversion='1.0'
          AND vs.code=codeFlavor.code
          )]))) AS memberof_)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_vector_code_member_of',
          fhir_path_expression=(
              "codeFlavors.code.memberOf('http://value.set/id')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT matches.element_offset IS NOT NULL AS memberof_
          FROM (SELECT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset) AS all_
          LEFT JOIN (SELECT element_offset
          FROM UNNEST(ARRAY(SELECT element_offset FROM (
          SELECT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset
          INNER JOIN `VALUESET_VIEW` vs ON
          vs.valueseturi='http://value.set/id'
          AND vs.code=codeFlavors_element_.code
          ))) AS element_offset
          ) AS matches
          ON all_.element_offset=matches.element_offset
          ORDER BY all_.element_offset)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_coding_member_of',
          fhir_path_expression=(
              "codeFlavor.coding.memberOf('http://value.set/id')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT memberof_
          FROM UNNEST((SELECT IF(codeFlavor.coding IS NULL, [], [
          EXISTS(
          SELECT 1
          FROM `VALUESET_VIEW` vs
          WHERE
          vs.valueseturi='http://value.set/id'
          AND vs.system=codeFlavor.coding.system
          AND vs.code=codeFlavor.coding.code
          )]))) AS memberof_)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_coding_member_of_value_set_version',
          fhir_path_expression=(
              "codeFlavor.coding.memberOf('http://value.set/id|1.0')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT memberof_
          FROM UNNEST((SELECT IF(codeFlavor.coding IS NULL, [], [
          EXISTS(
          SELECT 1
          FROM `VALUESET_VIEW` vs
          WHERE
          vs.valueseturi='http://value.set/id'
          AND vs.valuesetversion='1.0'
          AND vs.system=codeFlavor.coding.system
          AND vs.code=codeFlavor.coding.code
          )]))) AS memberof_)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_vector_coding_member_of',
          fhir_path_expression=(
              "codeFlavors.coding.memberOf('http://value.set/id')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT matches.element_offset IS NOT NULL AS memberof_
          FROM (SELECT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset) AS all_
          LEFT JOIN (SELECT element_offset
          FROM UNNEST(ARRAY(SELECT element_offset FROM (
          SELECT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset
          INNER JOIN `VALUESET_VIEW` vs ON
          vs.valueseturi='http://value.set/id'
          AND vs.system=codeFlavors_element_.coding.system
          AND vs.code=codeFlavors_element_.coding.code
          ))) AS element_offset
          ) AS matches
          ON all_.element_offset=matches.element_offset
          ORDER BY all_.element_offset)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_codeable_concept_member_of',
          fhir_path_expression=(
              "codeFlavor.codeableConcept.memberOf('http://value.set/id')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT memberof_
          FROM UNNEST((SELECT IF(codeFlavor.codeableConcept IS NULL, [], [
          EXISTS(
          SELECT 1
          FROM UNNEST(codeFlavor.codeableConcept.coding) AS codings
          INNER JOIN `VALUESET_VIEW` vs ON
          vs.valueseturi='http://value.set/id'
          AND vs.system=codings.system
          AND vs.code=codings.code
          )]))) AS memberof_)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_vector_codeable_concept_member_of',
          fhir_path_expression=(
              "codeFlavors.codeableConcept.memberOf('http://value.set/id')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT matches.element_offset IS NOT NULL AS memberof_
          FROM (SELECT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset) AS all_
          LEFT JOIN (SELECT element_offset
          FROM UNNEST(ARRAY(SELECT element_offset FROM (
          SELECT DISTINCT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset,
          UNNEST(codeFlavors_element_.codeableConcept.coding) AS codings
          INNER JOIN `VALUESET_VIEW` vs ON
          vs.valueseturi='http://value.set/id'
          AND vs.system=codings.system
          AND vs.code=codings.code
          ))) AS element_offset
          ) AS matches
          ON all_.element_offset=matches.element_offset
          ORDER BY all_.element_offset)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_of_type_codeable_concept_member_of',
          fhir_path_expression="codeFlavor.ofType('codeableConcept').memberOf('http://value.set/id')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT memberof_
          FROM UNNEST((SELECT IF(codeFlavor.codeableConcept IS NULL, [], [
          EXISTS(
          SELECT 1
          FROM UNNEST(codeFlavor.codeableConcept.coding) AS codings
          INNER JOIN `VALUESET_VIEW` vs ON
          vs.valueseturi='http://value.set/id'
          AND vs.system=codings.system
          AND vs.code=codings.code
          )]))) AS memberof_)
          WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_vector_of_type_codeable_concept_member_of',
          fhir_path_expression="codeFlavors.ofType('codeableConcept').memberOf('http://value.set/id')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT memberof_
          FROM (SELECT matches.element_offset IS NOT NULL AS memberof_
          FROM (SELECT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset) AS all_
          LEFT JOIN (SELECT element_offset
          FROM UNNEST(ARRAY(SELECT element_offset FROM (
          SELECT DISTINCT element_offset
          FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset,
          UNNEST(codeFlavors_element_.codeableConcept.coding) AS codings
          INNER JOIN `VALUESET_VIEW` vs ON
          vs.valueseturi='http://value.set/id'
          AND vs.system=codings.system
          AND vs.code=codings.code
          ))) AS element_offset
          ) AS matches
          ON all_.element_offset=matches.element_offset
          ORDER BY all_.element_offset)
          WHERE memberof_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_member_function_invocation_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
  ):
    fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder(
        self.resources,
        options=fhir_path.SqlGenerationOptions(
            value_set_codes_table='VALUESET_VIEW'
        ),
    )

    actual_sql_expression = fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)
    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
        value_set_codes_table='VALUESET_VIEW',
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_scalar_code_member_of',
          fhir_path_expression="codeFlavor.code.memberOf('http://value.set/1')",
          expected_sql_expression=textwrap.dedent("""\
              ARRAY(SELECT memberof_
              FROM (SELECT (codeFlavor.code IS NULL) OR (codeFlavor.code IN ("code_1", "code_2")) AS memberof_)
              WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_code_member_of_another_value_set',
          fhir_path_expression="codeFlavor.code.memberOf('http://value.set/2')",
          expected_sql_expression=textwrap.dedent("""\
              ARRAY(SELECT memberof_
              FROM (SELECT (codeFlavor.code IS NULL) OR (codeFlavor.code IN ("code_3", "code_4", "code_5")) AS memberof_)
              WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_vector_code_member_of',
          fhir_path_expression=(
              "codeFlavors.code.memberOf('http://value.set/1')"
          ),
          expected_sql_expression=textwrap.dedent("""\
              ARRAY(SELECT memberof_
              FROM (SELECT (codeFlavors_element_.code IS NULL) OR (codeFlavors_element_.code IN ("code_1", "code_2")) AS memberof_
              FROM UNNEST(codeFlavors) AS codeFlavors_element_ WITH OFFSET AS element_offset)
              WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_coding_member_of',
          fhir_path_expression=(
              "codeFlavor.coding.memberOf('http://value.set/2')"
          ),
          expected_sql_expression=textwrap.dedent("""\
              ARRAY(SELECT memberof_
              FROM (SELECT (codeFlavor.coding IS NULL) OR (((codeFlavor.coding.system = "system_3") AND (codeFlavor.coding.code IN ("code_3", "code_4"))) OR ((codeFlavor.coding.system = "system_5") AND (codeFlavor.coding.code IN ("code_5")))) AS memberof_)
              WHERE memberof_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_codeable_concept_member_of',
          fhir_path_expression=(
              "codeFlavor.codeableConcept.memberOf('http://value.set/2')"
          ),
          expected_sql_expression=textwrap.dedent("""\
              ARRAY(SELECT memberof_
              FROM (SELECT (codeFlavor.codeableConcept.coding IS NULL) OR EXISTS(
              SELECT 1
              FROM UNNEST(codeFlavor.codeableConcept.coding)
              WHERE ((system = "system_3") AND (code IN ("code_3", "code_4"))) OR ((system = "system_5") AND (code IN ("code_5")))) AS memberof_)
              WHERE memberof_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_fhir_path_member_function_against_local_value_set_definitions_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    expanded_value_set_1 = value_set_pb2.ValueSet()
    expanded_value_set_1.url.value = 'http://value.set/1'

    code_1 = expanded_value_set_1.expansion.contains.add()
    code_1.code.value = 'code_1'
    code_1.system.value = 'system_1'

    code_2 = expanded_value_set_1.expansion.contains.add()
    code_2.code.value = 'code_2'
    code_2.system.value = 'system_2'

    expanded_value_set_2 = value_set_pb2.ValueSet()
    expanded_value_set_2.url.value = 'http://value.set/2'

    # The following two codes are in the same code system.
    code_3 = expanded_value_set_2.expansion.contains.add()
    code_3.code.value = 'code_3'
    code_3.system.value = 'system_3'

    code_4 = expanded_value_set_2.expansion.contains.add()
    code_4.code.value = 'code_4'
    code_4.system.value = 'system_3'

    code_4 = expanded_value_set_2.expansion.contains.add()
    code_4.code.value = 'code_5'
    code_4.system.value = 'system_5'

    fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder(
        self.resources,
        options=fhir_path.SqlGenerationOptions(
            # Build a mock package manager which returns resources for the value
            # sets above.
            value_set_codes_definitions=unittest.mock.Mock(
                get_resource={
                    expanded_value_set_1.url.value: expanded_value_set_1,
                    expanded_value_set_2.url.value: expanded_value_set_2,
                }.get
            )
        ),
    )
    actual_sql_expression = fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)

    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
        # Build a mock package manager which returns resources for the value
        # sets above.
        value_set_codes_definitions=unittest.mock.Mock(
            get_resource={
                expanded_value_set_1.url.value: expanded_value_set_1,
                expanded_value_set_2.url.value: expanded_value_set_2,
            }.get
        ),
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_where_and_no_operand',
          fhir_path_expression='where(true)',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT where_clause_
          FROM (SELECT NULL AS where_clause_)
          WHERE where_clause_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_where',
          fhir_path_expression="bat.struct.where(value='')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT `struct`
          FROM (SELECT bat.struct
          FROM (SELECT bat.struct.*)
          WHERE (value = ''))
          WHERE `struct` IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_where_and_empty',
          fhir_path_expression="bat.struct.where(value='').empty()",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT empty_
          FROM (SELECT bat.struct IS NULL AS empty_
          FROM (SELECT bat.struct.*)
          WHERE (value = ''))
          WHERE empty_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_chained_where',
          fhir_path_expression=(
              "bat.struct.where(value='').where(anotherValue='')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT `struct`
          FROM (SELECT bat.struct
          FROM (SELECT bat.struct.*)
          WHERE (value = '') AND (anotherValue = ''))
          WHERE `struct` IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_complex_where',
          fhir_path_expression="bat.struct.where(value='' and anotherValue='')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT `struct`
          FROM (SELECT bat.struct
          FROM (SELECT bat.struct.*)
          WHERE ((value = '') AND (anotherValue = '')))
          WHERE `struct` IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_where_and_this_and_value',
          fhir_path_expression="bat.struct.value.where($this='')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT value
          FROM (SELECT bat.struct.value
          FROM (SELECT bat.struct.value)
          WHERE (value = ''))
          WHERE value IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_where_and_this_and_another_value',
          fhir_path_expression="bat.struct.anotherValue.where($this='')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT anotherValue
          FROM (SELECT bat.struct.anotherValue
          FROM (SELECT bat.struct.anotherValue)
          WHERE (anotherValue = ''))
          WHERE anotherValue IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_all',
          fhir_path_expression="bat.struct.value.all($this='')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT (value = '') AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bat.struct.value))
          WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_all_and_identifier',
          fhir_path_expression="bat.struct.all(anotherValue = '')",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT (anotherValue = '') AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bat.struct))
          WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_all_and_multiple_identifiers',
          fhir_path_expression=(
              "bat.struct.all(anotherValue = '' and value = '')"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT ((anotherValue = '') AND (value = '')) AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bat.struct))
          WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_all_and_identifier_plus_this',
          fhir_path_expression="bat.struct.all(anotherValue = '' and $this)",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT ((anotherValue = '') AND (SELECT `struct` IS NOT NULL)) AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bat.struct))
          WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          # TODO(b/197153513): Remove unnecessary `(SELECT inline),` from the
          # below sql query.
          testcase_name='with_all_and_repeated_primitive_only_comparison',
          fhir_path_expression='inline.numbers.all($this > 0)',
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT (numbers_element_ > 0) AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT inline),
          UNNEST(inline.numbers) AS numbers_element_ WITH OFFSET AS element_offset)
          WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name=(
              'with_all_and_repeated_subfield_primitive_only_comparison'
          ),
          # TODO(b/253262668): Determine if this is a bug in the old
          # implementation or new implementation.
          fhir_path_expression="bar.bats.struct.all( value = '' )",
          different_in_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT (value = '') AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE all_ IS NOT NULL)"""),
          expected_sql_expression_v2=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, value
          FROM (SELECT value)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
          FROM (SELECT '' AS literal_)) AS rhs_) AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bats_element_.struct
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset))
          WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          # This test checks that we are semantically checking our parameters
          # because it uses an EXIST here for a repeated item in the operand
          # instead of assuming that it is a scalar.
          testcase_name='with_all_and_repeated_operand_uses_exist_function',
          fhir_path_expression='bar.all( bats.exists() )',
          different_in_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT EXISTS(
          SELECT bats_element_
          FROM (SELECT bats_element_
          FROM UNNEST(bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE bats_element_ IS NOT NULL) AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bar))
          WHERE all_ IS NOT NULL)"""),
          expected_sql_expression_v2=textwrap.dedent("""\
          ARRAY(SELECT all_
          FROM (SELECT IFNULL(
          LOGICAL_AND(
          IFNULL(
          (SELECT EXISTS(
          SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset)
          WHERE bats_element_ IS NOT NULL) AS all_), FALSE)), TRUE) AS all_
          FROM (SELECT bar))
          WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_where_and_repeated',
          fhir_path_expression='bar.bats.where( struct.exists() )',
          different_in_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset
          WHERE (`struct` IS NOT NULL))
          WHERE bats_element_ IS NOT NULL)"""),
          expected_sql_expression_v2=textwrap.dedent("""\
          ARRAY(SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset
          WHERE EXISTS(
          SELECT `struct`
          FROM (SELECT `struct`)
          WHERE `struct` IS NOT NULL))
          WHERE bats_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_where_and_repeated_and_exists',
          fhir_path_expression='bar.bats.where( struct = struct ).exists()',
          different_in_v2=True,
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset
          WHERE (`struct` = `struct`))
          WHERE bats_element_ IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)"""),
          expected_sql_expression_v2=textwrap.dedent("""\
          ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT bats_element_
          FROM (SELECT bats_element_
          FROM (SELECT bar),
          UNNEST(bar.bats) AS bats_element_ WITH OFFSET AS element_offset
          WHERE NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, `struct`
          FROM (SELECT `struct`)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, `struct`
          FROM (SELECT `struct`)) AS rhs_))
          WHERE bats_element_ IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_retrieve_nested_field',
          fhir_path_expression="bat.struct.where(value='').anotherValue",
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT anotherValue
          FROM (SELECT bat.struct.anotherValue
          FROM (SELECT bat.struct.*)
          WHERE (value = ''))
          WHERE anotherValue IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_multiple_where_clause_and_retrieve_nested_field',
          fhir_path_expression=(
              "bat.struct.where(value='').where(anotherValue='').anotherValue"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT anotherValue
          FROM (SELECT bat.struct.anotherValue
          FROM (SELECT bat.struct.*)
          WHERE (value = '') AND (anotherValue = ''))
          WHERE anotherValue IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_retrieve_nested_field_exists',
          fhir_path_expression=(
              "bat.struct.where(value='').anotherValue.exists()"
          ),
          expected_sql_expression=textwrap.dedent("""\
          ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT anotherValue
          FROM (SELECT bat.struct.anotherValue
          FROM (SELECT bat.struct.*)
          WHERE (value = ''))
          WHERE anotherValue IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_advanced_fhir_path_member_function_invocation_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      different_in_v2: bool = False,
      expected_sql_expression_v2: str = '',
  ):
    actual_sql_expression = self.fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEqual(actual_sql_expression, expected_sql_expression)

    if not different_in_v2:
      expected_sql_expression_v2 = expected_sql_expression
    self.assert_evaluation_node_sql_correct(
        'Foo', fhir_path_expression, expected_sql_expression_v2
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_simple_identifier',
          fhir_path_expression='bar',
      ),
      dict(
          testcase_name='with_simple_invocation',
          fhir_path_expression='bar.bats',
      ),
      dict(
          testcase_name='with_deepest_nested_member_function',
          fhir_path_expression='bar.bats.struct.exists()',
      ),
      dict(
          testcase_name='with_deepest_nested_advanced_member_function',
          fhir_path_expression="bat.struct.value.where($this='')",
      ),
  )
  def test_validate_with_valid_fhir_path_expressions_succeeds(
      self, fhir_path_expression: str
  ):
    error_reporter = self.fhir_path_encoder.validate(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertEmpty(error_reporter.errors)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_invalid_union_between_structs',
          fhir_path_expression='bar.bats | bar.bats.struct',
      ),
      dict(
          testcase_name='with_invalid_union_between_struct_literal',
          fhir_path_expression='bar.bats.struct | 2',
      ),
      dict(
          testcase_name='with_invalid_union_between_literal_struct',
          fhir_path_expression='3 | bar.bats.struct',
      ),
      dict(
          testcase_name='with_invalid_equality_between_structs',
          fhir_path_expression='bar.bats = bar.bats.struct',
      ),
      dict(
          testcase_name='with_matches_function_and_repeated_operand',
          fhir_path_expression="bar.bats.matches('[a-zA-Z0-9]*')",
      ),
      dict(
          testcase_name='with_invalid_equality_between_struct_literal',
          fhir_path_expression='bar.bats = 2',
      ),
      dict(
          testcase_name='with_invalid_equality_between_literal_struct',
          fhir_path_expression='3 = bar.bats.struct',
      ),
      dict(
          testcase_name='with_invalid_equivalence_between_structs',
          fhir_path_expression='bar.bats ~ bar.bats.struct',
      ),
      dict(
          testcase_name='with_invalid_equivalence_between_struct_literal',
          fhir_path_expression='bar.bats.struct ~ 2',
      ),
      dict(
          testcase_name='with_invalid_equivalence_between_literal_struct',
          fhir_path_expression='3 ~ bar.bats.struct',
      ),
      dict(
          testcase_name='with_invalid_comparison_between_structs',
          fhir_path_expression='bar.bats < bar.bats.struct',
      ),
      # TODO(b/193046163): Add support for arbitrary leading expressions
      dict(
          testcase_name='with_single_member_access_leading_expression',
          fhir_path_expression='(true or false).bar',
      ),
      dict(
          testcase_name='with_reference_type_lacking_id_for',
          fhir_path_expression='ref',
          only_v1=True,
      ),
  )
  def test_encode_with_unsupported_fhir_path_expression_raises_type_error(
      self, fhir_path_expression: str, only_v1: bool = False
  ):
    """Tests FHIRPath expressions that are unsupported for Standard SQL."""
    with self.assertRaises(TypeError) as te:
      _ = self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )
    self.assertIsInstance(te.exception, TypeError)

    if only_v1:
      return

    # V2 test
    with self.assertRaises(ValueError) as te:
      builder = self.create_builder_from_str('Foo', fhir_path_expression)
      _ = _bigquery_interpreter.BigQuerySqlInterpreter().encode(builder)
    self.assertIsInstance(te.exception, ValueError)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_all_and_matches',
          invalid_fhir_path_expression="stringList.matches('regex')",
          fhir_path_expression="stringList.all($this.matches('regex'))",
          expected_sql_expression=textwrap.dedent("""\
            ARRAY(SELECT all_
            FROM (SELECT IFNULL(
            LOGICAL_AND(
            IFNULL(
            (SELECT REGEXP_CONTAINS(
            stringList_element_, 'regex') AS all_), FALSE)), TRUE) AS all_
            FROM UNNEST(stringList) AS stringList_element_ WITH OFFSET AS element_offset)
            WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_all_nested_arithmetic_logic',
          invalid_fhir_path_expression='inline.numbers + 3 > 4',
          fhir_path_expression='inline.numbers.all($this + 3 > 4)',
          expected_sql_expression=textwrap.dedent("""\
            ARRAY(SELECT all_
            FROM (SELECT IFNULL(
            LOGICAL_AND(
            IFNULL(
            (SELECT ((numbers_element_ + 3) > 4) AS all_), FALSE)), TRUE) AS all_
            FROM (SELECT inline),
            UNNEST(inline.numbers) AS numbers_element_ WITH OFFSET AS element_offset)
            WHERE all_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_where_and_member_of',
          invalid_fhir_path_expression=(
              "codingList.memberOf('http://value.set/id'))"
          ),
          fhir_path_expression=(
              "codingList.where($this.memberOf('http://value.set/id'))"
          ),
          expected_sql_expression=textwrap.dedent("""\
            ARRAY(SELECT codingList_element_
            FROM (SELECT codingList_element_
            FROM UNNEST(codingList) AS codingList_element_ WITH OFFSET AS element_offset
            WHERE (SELECT matches.element_offset IS NOT NULL AS memberof_
            FROM (SELECT element_offset
            FROM None) AS all_
            LEFT JOIN (SELECT element_offset
            FROM UNNEST(ARRAY(SELECT element_offset FROM (
            SELECT element_offset
            FROM None
            INNER JOIN `VALUESET_VIEW` vs ON
            vs.valueseturi='http://value.set/id'
            AND vs.system=codingList_element_.system
            AND vs.code=codingList_element_.code
            ))) AS element_offset
            ) AS matches
            ON all_.element_offset=matches.element_offset
            ORDER BY all_.element_offset))
            WHERE codingList_element_ IS NOT NULL)"""),
      ),
  )
  def test_encode_with_array_succeeds(
      self,
      invalid_fhir_path_expression: str,
      fhir_path_expression: str,
      expected_sql_expression: str,
  ):
    # V2 test
    with self.assertRaises(ValueError) as te:
      builder = self.create_builder_from_str(
          'Foo', invalid_fhir_path_expression
      )
      _ = _bigquery_interpreter.BigQuerySqlInterpreter().encode(builder)
    self.assertIsInstance(te.exception, ValueError)

    self.assert_evaluation_node_sql_correct(
        'Foo',
        fhir_path_expression,
        expected_sql_expression,
        value_set_codes_table='VALUESET_VIEW',
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_where_function_and_no_criteria',
          fhir_path_expression='bat.struct.where()',
      ),
      dict(
          testcase_name='with_where_function_and_non_bool_criteria',
          fhir_path_expression='bat.struct.where(value)',
      ),
      dict(
          testcase_name='with_all_function_and_no_criteria',
          fhir_path_expression='bat.struct.all()',
      ),
      dict(
          testcase_name='with_member_of_function_and_no_value_set',
          fhir_path_expression='codeFlavor.code.memberOf()',
      ),
      dict(
          testcase_name='with_member_of_function_and_non_string_value_set',
          fhir_path_expression='codeFlavor.code.memberOf(1)',
      ),
      dict(
          testcase_name='with_member_of_function_and_invalid_uri',
          fhir_path_expression="codeFlavor.code.memberOf('not-a-uri')",
      ),
      dict(
          testcase_name='with_member_of_function_and_invalid_operand',
          fhir_path_expression="inline.memberOf('http://value.set/id')",
      ),
  )
  def test_fhir_path_functions_with_wrong_inputs_raises_errors_in_semantic_analysis(
      self, fhir_path_expression: str
  ):
    """Tests FHIRPath expressions that are unsupported for Standard SQL."""
    with self.assertRaisesRegex(
        TypeError, 'FHIR Path Error: Semantic Analysis;'
    ) as te:
      _ = self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )
    self.assertIsInstance(te.exception, TypeError)

    # V2 test
    with self.assertRaises(ValueError) as te:
      _ = self.create_builder_from_str('Foo', fhir_path_expression)
    self.assertIsInstance(te.exception, ValueError)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_unknown_value_set',
          fhir_path_expression=(
              "codeFlavor.code.memberOf('http://value.set/id')"
          ),
          options=fhir_path_options.SqlValidationOptions(
              num_code_systems_per_value_set={'something-else': 1}
          ),
      ),
      dict(
          testcase_name='with_string_against_too_many_code_systems',
          fhir_path_expression=(
              "codeFlavor.code.memberOf('http://value.set/id')"
          ),
          options=fhir_path_options.SqlValidationOptions(
              num_code_systems_per_value_set={'http://value.set/id': 2}
          ),
      ),
  )
  def test_fhir_path_member_of_with_wrong_inputs_raises_errors_in_semantic_analysis(
      self,
      fhir_path_expression: str,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ):
    fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder(
        self.resources,
        validation_options=options,
    )
    with self.assertRaisesRegex(
        TypeError, 'FHIR Path Error: Semantic Analysis;'
    ) as te:
      _ = fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )
    self.assertIsInstance(te.exception, TypeError)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_known_value_set',
          fhir_path_expression=(
              "codeFlavor.code.memberOf('http://value.set/id')"
          ),
          options=fhir_path_options.SqlValidationOptions(
              num_code_systems_per_value_set={'http://value.set/id': 1}
          ),
      ),
      dict(
          testcase_name='with_coding_against_multiple_code_systems',
          fhir_path_expression=(
              "codeFlavor.coding.memberOf('http://value.set/id')"
          ),
          options=fhir_path_options.SqlValidationOptions(
              num_code_systems_per_value_set={'http://value.set/id': 2}
          ),
      ),
      dict(
          testcase_name='with_codeable_concept_against_multiple_code_systems',
          fhir_path_expression=(
              "codeFlavor.codeableConcept.memberOf('http://value.set/id')"
          ),
          options=fhir_path_options.SqlValidationOptions(
              num_code_systems_per_value_set={'http://value.set/id': 2}
          ),
      ),
  )
  def test_fhir_path_member_of_with_correct_inputs_succeeds(
      self,
      fhir_path_expression: str,
      options: Optional[fhir_path_options.SqlValidationOptions] = None,
  ):
    fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder(
        self.resources,
        options=fhir_path.SqlGenerationOptions(
            value_set_codes_table='VALUESET_VIEW'
        ),
        validation_options=options,
    )
    result = fhir_path_encoder.encode(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )

    self.assertTrue(result)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_single_member_access_unknown',
          fhir_path_expression='unknown',
      ),
      dict(
          testcase_name='with_nested_member_access_unknown',
          fhir_path_expression='bar.bats.unknown',
      ),
      dict(
          testcase_name='with_deep_nested_member_access_unknown',
          fhir_path_expression='bar.bats.struct.unknown',
      ),
  )
  def test_encode_with_unknown_fhir_path_member_invocation_raises_value_error(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(ValueError) as ve:
      _ = self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )
    self.assertIsInstance(ve.exception, ValueError)

    # V2 test
    with self.assertRaises(ValueError) as te:
      _ = self.create_builder_from_str('Foo', fhir_path_expression)
    self.assertIsInstance(te.exception, ValueError)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_invalid_invocation_internal_expression',
          fhir_path_expression='foo.bar.exists().(2+3)',
      ),
      dict(
          testcase_name='with_invalid_invocation_no_identifier',
          fhir_path_expression='foo.bar.exists()..',
      ),
      dict(
          testcase_name='with_invalid_operation',
          fhir_path_expression='foo + +',
      ),
  )
  def test_encode_with_invalid_fhir_path_expression_raises_value_error(
      self, fhir_path_expression: str
  ):
    """Tests FHIRPath expressions that are syntactically incorrect."""
    with self.assertRaises(ValueError) as ve:
      _ = self.fhir_path_encoder.encode(
          structure_definition=self.foo,
          element_definition=self.foo_root,
          fhir_path_expression=fhir_path_expression,
      )
    self.assertIsInstance(ve.exception, ValueError)

    # V2 test
    with self.assertRaises(ValueError) as te:
      _ = self.create_builder_from_str('Foo', fhir_path_expression)
    self.assertIsInstance(te.exception, ValueError)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_where_function_and_no_criteria',
          fhir_path_expression='bat.struct.where()',
      ),
      dict(
          testcase_name='with_where_function_and_non_bool_criteria',
          fhir_path_expression='bat.struct.where(value)',
      ),
      dict(
          testcase_name='with_matches_function_and_repeated_operand',
          fhir_path_expression="bar.bats.matches('regex')",
      ),
      dict(
          testcase_name='with_member_of_function_and_no_value_set',
          fhir_path_expression='codeFlavor.code.memberOf()',
      ),
      dict(
          testcase_name='with_member_of_function_and_non_string_value_set',
          fhir_path_expression='codeFlavor.code.memberOf(1)',
      ),
      dict(
          testcase_name='with_member_of_function_and_invalid_uri',
          fhir_path_expression="codeFlavor.code.memberOf('not-a-uri')",
      ),
      dict(
          testcase_name='with_member_of_function_and_invalid_operand',
          fhir_path_expression="inline.memberOf('http://value.set/id')",
      ),
  )
  def test_validate_with_invalid_expressions_fails_and_populates_error_reporter(
      self, fhir_path_expression: str
  ):
    error_reporter = self.fhir_path_encoder.validate(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertNotEmpty(error_reporter.errors)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_single_member_access_unknown',
          fhir_path_expression='unknown',
      ),
      dict(
          testcase_name='with_nested_member_access_unknown',
          fhir_path_expression='bar.bats.unknown',
      ),
      dict(
          testcase_name='with_invalid_invocation_internal_expression',
          fhir_path_expression='foo.bar.exists().(2+3)',
      ),
      dict(
          testcase_name='with_invalid_invocation_no_identifier',
          fhir_path_expression='foo.bar.exists()..',
      ),
  )
  def test_validate_with_invalid_fhir_path_syntax_fails_and_populates_error_reporter(
      self, fhir_path_expression: str
  ):
    error_reporter = self.fhir_path_encoder.validate(
        structure_definition=self.foo,
        element_definition=self.foo_root,
        fhir_path_expression=fhir_path_expression,
    )
    self.assertNotEmpty(error_reporter.errors)

  def test_encode_with_fhir_slice_element_definition_is_skipped(self):
    """Creates a simple one-off resource with a slice and tests encoding.

    Slices are skipped during FHIRPath encoding. As such, we construct a test
    resource whose only field (other than its root) is a slice. During encoding,
    we will raise an exception, since the `FhirPathStandardSqlEncoder` is unable
    to find a corresponding `ElementDefinition` that is not a slice at <path>.
    """
    root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    soft_delete_slice = sdefs.build_element_definition(
        id_='Foo.bar:softDelete',
        path='Foo.bar',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(0, '*'),
        slice_name='softDelete',
    )
    resource = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[root, soft_delete_slice]
    )
    fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder([resource])
    with self.assertRaises(ValueError) as ve:
      _ = fhir_path_encoder.encode(
          structure_definition=resource,
          element_definition=root,
          fhir_path_expression='bar.exists()',
      )

    self.assertIsInstance(ve.exception, ValueError)

    # V2 test
    mock_context = context_lib.MockFhirPathContext([resource])
    with self.assertRaises(ValueError) as te:
      _ = self.create_builder_from_str(
          'Foo', 'bar.exists()', fhir_context=mock_context
      )
    self.assertIsInstance(te.exception, ValueError)

  def test_encode_with_inline_profiled_element_succeeds(self):
    """Creates a simple one-off resource graph with an inline profile.

    This ensures that, when "walking" a FHIR resource graph during FHIRPath
    Standard SQL encoding:
      (1) Check inline children for <curr_path>.<identifier>
      (2) Check children from the field's type for <type>.<identifier>

    Prioritize children results over any from the field's type.
    """
    string_datatype = sdefs.build_resource_definition(
        id_='string',
        element_definitions=[
            sdefs.build_element_definition(
                id_='string',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )

    # Foo type; note that "Foo.bar.value" inline profile is of cardinality 1.
    foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    foo_bar_inline = sdefs.build_element_definition(
        id_='Foo.bar',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    foo_bar_value_inline = sdefs.build_element_definition(
        id_='Foo.bar.value',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    foo = sdefs.build_resource_definition(
        id_='Foo',
        element_definitions=[foo_root, foo_bar_inline, foo_bar_value_inline],
    )

    # Bar type; note that "Bar.value" is of cardinality >= 1.
    bar_root = sdefs.build_element_definition(
        id_='Bar',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    bar_value = sdefs.build_element_definition(
        id_='Bar.value',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=0, max='*'),
    )
    bar = sdefs.build_resource_definition(
        id_='Bar', element_definitions=[bar_root, bar_value]
    )

    fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder(
        [string_datatype, foo, bar]
    )
    actual_sql_expression = fhir_path_encoder.encode(
        structure_definition=foo,
        element_definition=foo_root,
        fhir_path_expression='bar.value.exists()',
    )

    # We prioritize inline children, so we expect that a query reflecting
    # the constrained cardinality of 1 should be generated.
    expected_sql_expression = textwrap.dedent("""\
    ARRAY(SELECT exists_
    FROM (SELECT bar.value IS NOT NULL AS exists_)
    WHERE exists_ IS NOT NULL)""")
    self.assertEqual(actual_sql_expression, expected_sql_expression)


class FhirProfileStandardSqlEncoderTestBase(
    parameterized.TestCase, fhir_path_test_base.FhirPathTestBase
):
  """A base test class providing functionality for testing profile encoding.

  Tests should leverage one of the base `assert_...` methods which will create a
  profile of a resource whose base definition is within the graph of resources
  under test.

  The profile will replace the `ElementDefinition` at `element_definition_id`
  with an `ElementDefinition` who is constrained by a specified `constraint`.
  This profile is then added to the existing resource graph, and encoded to
  Standard SQL by an instance of the `fhir_path.FhirProfileStandardSqlEncoder`.

  Class Attributes:
    resources: A mapping from `StructureDefinition.url.value` to the associated
      `StructureDefinition`.
  """

  resources: Dict[str, structure_definition_pb2.StructureDefinition]

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls.resources: Dict[str, structure_definition_pb2.StructureDefinition] = {}

  def _create_profile_of(
      self,
      base_id: str,
      element_definition_id: str,
      constraint: datatypes_pb2.ElementDefinition.Constraint,
  ) -> structure_definition_pb2.StructureDefinition:
    """Returns a profile of an existing `StructureDefinition`.

    The returned profile will have the element definition at
    `element_definition_id` constrained by `constraint`.

    Args:
      base_id: The logical ID of a `StructureDefinition` that exists in
        `self.resources`.
      element_definition_id: The ID for inter-element referencing specifying the
        `ElementDefinition` within type of `base_id` that should be constrained.
      constraint: The FHIRPath `Constraint` to add to the `ElementDefinition` at
        `element_definition_id` in the profile.

    Returns:
      A profile of `base_id` with `constraint` applied to the
      `ElementDefinition` at `element_definition_id`.

    Raises:
      KeyError: In the event that the base resource with `base_id` is not mapped
        in `self.resources` with URL
        'http://hl7.org/fhir/StructureDefinition/<base_id>'.
      ValueError: In the event that duplicate `ElementDefinition`s exist with
      `element_definition_id`, or if no `ElementDefinition` with
      `element_definition_id` is found.
    """
    base_url = f'http://hl7.org/fhir/StructureDefinition/{base_id}'
    base_resource = self.resources[base_url]

    element_definitions = []
    element_definition_found = False

    # Build up the list of `ElementDefinition`s for the resulting profile,
    # ensuring that the `ElementDefinition` with id `element_definition_id` is
    # constrained by `constraint`.
    for element_definition in base_resource.snapshot.element:
      element_definition_cpy = copy.deepcopy(element_definition)
      if element_definition.id.value == element_definition_id:
        if element_definition_found:
          raise ValueError(
              f'Found duplicate ElementDefinition: {element_definition_id!r}.'
          )
        element_definition_found = True
        element_definition_cpy.constraint.append(constraint)
      element_definitions.append(element_definition_cpy)

    if not element_definition_found:
      raise ValueError(
          f'No ElementDefinition {element_definition_id!r} in type {base_id!r}.'
      )
    return self.build_profile(
        id_=base_id, element_definitions=element_definitions
    )

  def assert_constraint_is_equal_to_expression(
      self,
      *,
      base_id: str,
      element_definition_id: str,
      constraint: datatypes_pb2.ElementDefinition.Constraint,
      expected_sql_expression: str,
      expected_fhir_path_sql_expression: str,
      expected_severity: validation_pb2.ValidationSeverity = validation_pb2.ValidationSeverity.SEVERITY_ERROR,
      supported_in_v2: bool = False,
      expected_sql_expression_v2: Optional[str] = None,
  ) -> None:
    """Asserts that `expected_sql_expression` is generated."""

    # Create profile-under-test
    profile = self._create_profile_of(
        base_id,
        element_definition_id,
        constraint,
    )

    # Encode as Standard SQL expression
    all_resources = [profile] + list(self.resources.values())
    error_reporter = fhir_errors.ListErrorReporter()
    profile_std_sql_encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: all_resources),
        error_reporter,
    )
    actual_bindings = profile_std_sql_encoder.encode(profile)

    expected_column_base = element_definition_id.lower().replace('.', '_')
    expected_binding = validation_pb2.SqlRequirement(
        column_name=f'{expected_column_base}_key_1',
        sql_expression=expected_sql_expression,
        severity=expected_severity,
        type=validation_pb2.ValidationType.VALIDATION_TYPE_FHIR_PATH_CONSTRAINT,
        element_path=element_definition_id,
        fhir_path_key=constraint.key.value,
        fhir_path_expression=constraint.expression.value,
        fields_referenced_by_expression=(
            fhir_path_validator._fields_referenced_by_expression(
                constraint.expression.value
            )
        ),
    )

    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(error_reporter.warnings)
    self.assertListEqual(actual_bindings, [expected_binding])

    # Check that Ephemeral state is cleared.
    self.assertEmpty(profile_std_sql_encoder._ctx)
    self.assertEmpty(profile_std_sql_encoder._in_progress)
    self.assertEmpty(profile_std_sql_encoder._requirement_column_names)
    self.assertEmpty(profile_std_sql_encoder._element_id_to_regex_map)
    self.assertEmpty(profile_std_sql_encoder._regex_columns_generated)

    if supported_in_v2:
      error_reporter_v2 = fhir_errors.ListErrorReporter()
      profile_std_sql_encoder_v2 = (
          fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
              unittest.mock.Mock(
                  iter_structure_definitions=lambda: all_resources
              ),
              primitive_handler.PrimitiveHandler(),
              error_reporter_v2,
          )
      )
      actual_bindings_v2 = profile_std_sql_encoder_v2.encode(profile)
      self.assertEmpty(error_reporter_v2.errors)
      self.assertEmpty(error_reporter_v2.warnings)

      # Some v2 expressions differ from v1.
      # TODO(b/261065418): Update e2e tests to use v2 validator.
      if expected_sql_expression_v2:
        expected_binding = validation_pb2.SqlRequirement(
            column_name=f'{expected_column_base}_key_1',
            sql_expression=expected_sql_expression_v2,
            severity=expected_severity,
            type=validation_pb2.ValidationType.VALIDATION_TYPE_FHIR_PATH_CONSTRAINT,
            element_path=element_definition_id,
            fhir_path_key=constraint.key.value,
            fhir_path_expression=constraint.expression.value,
            fields_referenced_by_expression=(
                fhir_path_validator_v2._fields_referenced_by_expression(
                    constraint.expression.value
                )
            ),
        )

      expected_binding.fhir_path_sql_expression = (
          expected_fhir_path_sql_expression
      )

      self.assertListEqual(actual_bindings_v2, [expected_binding])

      # Check that Ephemeral state is cleared.
      self.assertEmpty(profile_std_sql_encoder_v2._ctx)
      self.assertEmpty(profile_std_sql_encoder_v2._in_progress)
      self.assertEmpty(profile_std_sql_encoder_v2._requirement_column_names)

  def assert_encoder_generates_expression_for_required_field(
      self,
      *,
      base_id: str,
      required_field: str,
      context_element_path: str,
      expected_column_name: str,
      description: str,
      expected_sql_expression: str,
      expected_fhir_path_sql_expression: str,
      expected_severity: validation_pb2.ValidationSeverity = validation_pb2.ValidationSeverity.SEVERITY_ERROR,
      fhir_path_key: Optional[str] = None,
      fhir_path_expression: Optional[str] = None,
      fields_referenced_by_expression: Optional[List[str]] = None,
      supported_in_v2: bool = False,
  ) -> None:
    """Asserts `expected_sql_expression` is generated for a required field."""

    resource = self.resources[
        f'http://hl7.org/fhir/StructureDefinition/{base_id}'
    ]

    # Encode as Standard SQL expression.
    all_resources = list(self.resources.values())
    error_reporter = fhir_errors.ListErrorReporter()
    profile_std_sql_encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: all_resources),
        error_reporter,
    )
    actual_bindings = profile_std_sql_encoder.encode(resource)

    if supported_in_v2:
      profile_std_sql_encoder_v2 = (
          fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
              unittest.mock.Mock(
                  iter_structure_definitions=lambda: all_resources
              ),
              primitive_handler.PrimitiveHandler(),
              error_reporter,
          )
      )
      actual_bindings_v2 = profile_std_sql_encoder_v2.encode(resource)

    # Replace optional params with defaults if needed.
    fhir_path_key = (
        fhir_path_key
        if fhir_path_key
        else (f'{required_field}-cardinality-is-valid')
    )
    fhir_path_expression = (
        fhir_path_expression
        if fhir_path_expression
        else (f'{required_field}.exists()')
    )

    expected_binding = validation_pb2.SqlRequirement(
        column_name=expected_column_name,
        sql_expression=expected_sql_expression,
        severity=expected_severity,
        type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
        element_path=context_element_path,
        description=description,
        fhir_path_key=fhir_path_key,
        fhir_path_expression=fhir_path_expression,
        fields_referenced_by_expression=fields_referenced_by_expression,
    )

    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(error_reporter.warnings)
    self.assertListEqual(actual_bindings, [expected_binding])
    if supported_in_v2:
      expected_binding.fhir_path_sql_expression = (
          expected_fhir_path_sql_expression
      )
      self.assertListEqual(actual_bindings_v2, [expected_binding])

  def assert_raises_fhir_path_encoding_error(
      self,
      *,
      base_id: str,
      element_definition_id: str,
      constraint: datatypes_pb2.ElementDefinition.Constraint,
      supported_in_v2: bool = False,
  ) -> None:
    """Asserts that a single error is raised."""

    # Create profile-under-test
    profile = self._create_profile_of(
        base_id,
        element_definition_id,
        constraint,
    )

    # Encode as Standard SQL expression
    all_resources = [profile] + list(self.resources.values())
    error_reporter = fhir_errors.ListErrorReporter()
    profile_std_sql_encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: all_resources),
        error_reporter,
    )
    _ = profile_std_sql_encoder.encode(profile)
    self.assertLen(error_reporter.errors, 1)
    self.assertEmpty(error_reporter.warnings)

    if supported_in_v2:
      error_reporter = fhir_errors.ListErrorReporter()
      profile_std_sql_encoder_v2 = (
          fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
              unittest.mock.Mock(
                  iter_structure_definitions=lambda: all_resources
              ),
              primitive_handler.PrimitiveHandler(),
              error_reporter,
          )
      )
      _ = profile_std_sql_encoder_v2.encode(profile)
      self.assertLen(error_reporter.errors, 1)
      self.assertEmpty(error_reporter.warnings)


class FhirProfileStandardSqlEncoderConfigurationTest(
    FhirProfileStandardSqlEncoderTestBase
):
  """Tests various configurations and behaviors of the profile encoder."""

  def add_regex_to_structure_definition(
      self,
      structure_definition: structure_definition_pb2.StructureDefinition,
      regex_value: str,
  ):
    """Adds regex `regex_value` to `structure_definition`."""
    snapshot_element = structure_definition.snapshot.element.add()
    snapshot_element.id.value = 'string.value'
    sub_type = snapshot_element.type.add()
    extension = sub_type.extension.add()
    extension.url.value = 'http://hl7.org/fhir/StructureDefinition/regex'
    extension.value.string_value.value = regex_value

  @parameterized.named_parameters(
      dict(
          testcase_name='with_add_value_set_bindings_option',
          options=fhir_path.SqlGenerationOptions(
              add_value_set_bindings=True,
              value_set_codes_table='VALUESET_VIEW',
          ),
          expected_sql=textwrap.dedent("""\
      (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
      FROM UNNEST(ARRAY(SELECT memberof_
      FROM (SELECT memberof_
      FROM UNNEST((SELECT IF(bar.code IS NULL, [], [
      EXISTS(
      SELECT 1
      FROM `VALUESET_VIEW` vs
      WHERE
      vs.valueseturi='http://value.set/id'
      AND vs.code=bar.code
      )]))) AS memberof_)
      WHERE memberof_ IS NOT NULL)) AS result_)"""),
      ),
      dict(
          testcase_name='with_value_set_codes_table_option',
          options=fhir_path.SqlGenerationOptions(
              add_value_set_bindings=True,
              value_set_codes_table=bigquery.TableReference(
                  bigquery.DatasetReference('project', 'dataset'), 'table'
              ),
          ),
          expected_sql=textwrap.dedent("""\
      (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
      FROM UNNEST(ARRAY(SELECT memberof_
      FROM (SELECT memberof_
      FROM UNNEST((SELECT IF(bar.code IS NULL, [], [
      EXISTS(
      SELECT 1
      FROM `project.dataset.table` vs
      WHERE
      vs.valueseturi='http://value.set/id'
      AND vs.code=bar.code
      )]))) AS memberof_)
      WHERE memberof_ IS NOT NULL)) AS result_)"""),
      ),
  )
  def test_encode_with_value_set_bindings_produces_value_set_constraint(
      self, options, expected_sql
  ):
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    foo_bar_element_definition = sdefs.build_element_definition(
        id_='Foo.bar',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, foo_bar_element_definition]
    )

    bar_root = sdefs.build_element_definition(
        id_='Bar', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    bar_code_element_definition = sdefs.build_element_definition(
        id_='Bar.code',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    bar_code_element_definition.binding.strength.value = 1
    bar_code_element_definition.binding.value_set.value = 'http://value.set/id'
    bar = sdefs.build_resource_definition(
        id_='Bar', element_definitions=[bar_root, bar_code_element_definition]
    )

    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(
            iter_structure_definitions=unittest.mock.Mock(
                return_value=[foo, bar]
            ),
            # When asked for value sets, return None to force joins against a
            # value sets table.
            get_resource=unittest.mock.Mock(return_value=None),
        ),
        error_reporter,
        options=options,
    )
    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 1)
    self.assertEqual(actual_bindings[0].element_path, 'Foo.bar')
    self.assertEqual(
        actual_bindings[0].fhir_path_expression,
        "code.memberOf('http://value.set/id')",
    )
    self.assertEqual(
        actual_bindings[0].fields_referenced_by_expression, ['code']
    )
    self.assertEqual(actual_bindings[0].sql_expression, expected_sql)

    error_reporter_v2 = fhir_errors.ListErrorReporter()
    encoder_v2 = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, bar]),
        primitive_handler.PrimitiveHandler(),
        error_reporter_v2,
        options=options,
    )
    actual_bindings_v2 = encoder_v2.encode(foo)
    self.assertEmpty(error_reporter_v2.warnings)
    self.assertEmpty(error_reporter_v2.errors)
    self.assertLen(actual_bindings_v2, 1)
    self.assertEqual(actual_bindings_v2[0].element_path, 'Foo.bar')
    self.assertEqual(
        actual_bindings_v2[0].fhir_path_expression,
        "code.memberOf('http://value.set/id')",
    )
    self.assertEqual(
        actual_bindings_v2[0].fields_referenced_by_expression, ['code']
    )
    self.assertEqual(actual_bindings_v2[0].sql_expression, expected_sql)

  def test_skip_keys_with_valid_resource_produces_no_constraints(self):
    # Setup resource with a defined constraint
    constraint = self.build_constraint(
        fhir_path_expression='false', key='always-fail-constraint-key'
    )
    foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
        constraints=[constraint],
    )
    profile = self.build_profile(id_='Foo', element_definitions=[foo_root])

    # Standup encoder; skip 'always-fail-constraint-key'
    error_reporter = fhir_errors.ListErrorReporter()
    options = fhir_path.SqlGenerationOptions(
        skip_keys=set(['always-fail-constraint-key'])
    )
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [profile]),
        error_reporter,
        options=options,
    )
    actual_bindings = encoder.encode(profile)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(actual_bindings)

    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [profile]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
        options=cast(fhir_path_validator_v2.SqlGenerationOptions, options),
    )
    actual_bindings = encoder.encode(profile)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(actual_bindings)

  def test_skip_keys_with_valid_resource_produces_no_constraints_v2(self):
    # Setup resource with a defined constraint
    constraint = self.build_constraint(
        fhir_path_expression='false', key='always-fail-constraint-key'
    )
    foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
        constraints=[constraint],
    )
    profile = self.build_profile(id_='Foo', element_definitions=[foo_root])

    # Standup encoder; skip 'always-fail-constraint-key'
    error_reporter = fhir_errors.ListErrorReporter()
    options = fhir_path.SqlGenerationOptions(
        skip_keys=set(['always-fail-constraint-key'])
    )
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [profile]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
        options=cast(fhir_path_validator_v2.SqlGenerationOptions, options),
    )
    actual_bindings = encoder.encode(profile)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(actual_bindings)

  def test_skip_slice_with_valid_resource_produces_no_constraints(self):
    constraint = self.build_constraint(fhir_path_expression='false')
    bar_root = sdefs.build_element_definition(
        id_='Bar',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
        constraints=[constraint],
    )
    bar = sdefs.build_resource_definition(
        id_='Bar', element_definitions=[bar_root]
    )

    # Setup resource with a defined constraint
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    bar_soft_delete_slice = sdefs.build_element_definition(
        id_='Foo.bar:softDelete',
        path='Foo.bar',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, bar_soft_delete_slice]
    )

    # Standup encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, bar]),
        error_reporter,
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEqual(
        error_reporter.errors,
        [
            'Conversion Error: Foo.bar; The given element is a slice that is '
            + 'not on an extension. This is not yet supported.'
        ],
    )
    self.assertEmpty(actual_bindings)

  def test_skip_slice_with_slice_on_extension_and_valid_resource_is_not_skipped(
      self,
  ):
    # Set up resource with a defined constraint
    constraint = self.build_constraint(fhir_path_expression='false')
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    extension_slice = sdefs.build_element_definition(
        id_='Foo.extension:softDelete',
        path='Foo.extension',
        type_codes=['Extension'],
        cardinality=sdefs.Cardinality(0, '*'),
        constraints=[constraint],
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, extension_slice]
    )

    # Extension resource.
    extension_root_element = sdefs.build_element_definition(
        id_='Extension',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    value_element_definition = sdefs.build_element_definition(
        id_='Extension.value[x]',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    extension = sdefs.build_resource_definition(
        id_='Extension',
        element_definitions=[
            extension_root_element,
            value_element_definition,
        ],
    )

    # Stand up encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo]),
        error_reporter,
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 1)

    # Stand up v2 encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, extension]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 1)

  def test_skip_slice_with_slice_on_extension_with_hyphen_and_valid_resource_is_not_skipped(
      self,
  ):
    # Set up resource with a defined constraint
    constraint = self.build_constraint(fhir_path_expression='false')
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    extension_slice = sdefs.build_element_definition(
        id_='Foo.extension:softDelete-hyphen',
        path='Foo.extension',
        type_codes=['Extension'],
        cardinality=sdefs.Cardinality(0, '*'),
        constraints=[constraint],
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, extension_slice]
    )

    # Extension resource.
    extension_root_element = sdefs.build_element_definition(
        id_='Extension',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    value_element_definition = sdefs.build_element_definition(
        id_='Extension.value[x]',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    extension = sdefs.build_resource_definition(
        id_='Extension',
        element_definitions=[
            extension_root_element,
            value_element_definition,
        ],
    )

    # Stand up v2 encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, extension]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 1)
    self.assertEqual(
        actual_bindings[0].column_name, 'foo_softdelete_hyphen_key_1'
    )

  def test_skip_extension_url_field(self):
    # Set up resource with a defined constraint
    constraint = self.build_constraint(fhir_path_expression='false')
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    extension_slice = sdefs.build_element_definition(
        id_='Foo.extension:url',
        path='Foo.extension',
        type_codes=['Extension'],
        cardinality=sdefs.Cardinality(0, '*'),
        constraints=[constraint],
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, extension_slice]
    )

    # Extension resource.
    extension_root_element = sdefs.build_element_definition(
        id_='Extension',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    url_element_definition = sdefs.build_element_definition(
        id_='Extension.url',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    value_element_definition = sdefs.build_element_definition(
        id_='Extension.value[x]',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    extension = sdefs.build_resource_definition(
        id_='Extension',
        element_definitions=[
            extension_root_element,
            value_element_definition,
            url_element_definition,
        ],
    )

    # Stand up v2 encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, extension]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 1)

  def test_choice_type_that_is_also_slice_on_extension_skips_regex_validation(
      self,
  ):
    # Set up resource.
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    extension_slice = sdefs.build_element_definition(
        id_='Foo.extension:softDelete',
        path='Foo.extension',
        type_codes=['Extension'],
        profiles=['http://hl7.org/fhir/StructureDefinition/CustomExtension'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, extension_slice]
    )

    # CustomExtension resource.
    custom_extension_root_element = sdefs.build_element_definition(
        id_='Extension',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    value_element_definition = sdefs.build_element_definition(
        id_='Extension.value[x]',
        type_codes=['string', 'int'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    custom_extension = sdefs.build_resource_definition(
        id_='CustomExtension',
        element_definitions=[
            custom_extension_root_element,
            value_element_definition,
        ],
    )

    # Primitive string structure definition.
    value_element_definition = sdefs.build_element_definition(
        id_='string.value',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    string_struct = sdefs.build_resource_definition(
        id_='string', element_definitions=[value_element_definition]
    )
    self.add_regex_to_structure_definition(string_struct, 'some regex')

    # Stand up encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(
            iter_structure_definitions=lambda: [  # pylint: disable=g-long-lambda
                foo,
                custom_extension,
                string_struct,
            ]
        ),
        error_reporter,
        options=fhir_path.SqlGenerationOptions(add_primitive_regexes=True),
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEqual(
        error_reporter.errors,
        [
            # Adding `+` to get rid of `implicit-str-concat` inside list
            # warning.
            'Validation Error: Foo; Element `Foo.softDelete` with type codes: '
            + "['string', 'int'], is a choice type which is not currently "
            + 'supported.'
        ],
    )
    self.assertEmpty(actual_bindings)

    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(
            iter_structure_definitions=lambda: [  # pylint: disable=g-long-lambda
                foo,
                custom_extension,
                string_struct,
            ]
        ),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
        options=fhir_path_validator_v2.SqlGenerationOptions(
            add_primitive_regexes=True
        ),
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEqual(
        error_reporter.errors,
        [
            # Adding `+` to get rid of `implicit-str-concat` inside list
            # warning.
            'FHIR Path Error: Foo.softDelete; softDelete; Element '
            + '`softDelete` is a choice type which is not currently supported.'
        ],
    )
    # Check that the only binding is over the choice type.
    self.assertLen(actual_bindings, 1)
    self.assertEqual(
        actual_bindings[0].fhir_path_expression,
        "softDelete.ofType('string').exists().toInteger() + "
        "softDelete.ofType('int').exists().toInteger() <= 1",
    )

  def test_non_choice_type_that_is_also_slice_on_extension_makes_regex_validation(
      self,
  ):
    # Set up resource.
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    extension_slice = sdefs.build_element_definition(
        id_='Foo.extension:softDelete',
        path='Foo.extension',
        type_codes=['Extension'],
        profiles=['http://hl7.org/fhir/StructureDefinition/CustomExtension'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, extension_slice]
    )

    # CustomExtension resource.
    custom_extension_root_element = sdefs.build_element_definition(
        id_='Extension',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    value_element_definition = sdefs.build_element_definition(
        id_='Extension.value[x]',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )

    custom_extension = sdefs.build_resource_definition(
        id_='CustomExtension',
        element_definitions=[
            custom_extension_root_element,
            value_element_definition,
        ],
    )

    # Primitive string structure definition.
    value_element_definition = sdefs.build_element_definition(
        id_='string.value',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    string_struct = sdefs.build_resource_definition(
        id_='string', element_definitions=[value_element_definition]
    )
    self.add_regex_to_structure_definition(string_struct, 'some regex')

    # Stand up encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(
            iter_structure_definitions=lambda: [  # pylint: disable=g-long-lambda
                foo,
                custom_extension,
                string_struct,
            ]
        ),
        error_reporter,
        options=fhir_path.SqlGenerationOptions(add_primitive_regexes=True),
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 1)
    self.assertEqual(
        actual_bindings[0].fhir_path_expression,
        "softDelete.all( $this.matches('^(some regex)$') )",
    )

  def test_encode_with_duplicate_sql_requirement_creates_constraint_and_logs_error(
      self,
  ):
    first_constraint = self.build_constraint(
        fhir_path_expression='false', key='some-key'
    )
    second_constraint = self.build_constraint(
        fhir_path_expression='false', key='some-key'
    )

    # Setup resource with a defined constraint
    foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(1, '1'),
        constraints=[first_constraint, second_constraint],
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root]
    )

    # Standup encoder
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo]),
        error_reporter,
    )

    # Ensure that we only produce a single Standard SQL requirement, and that
    # an error is logged since we were given a duplicate constraint.
    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertLen(error_reporter.errors, 1)
    self.assertLen(actual_bindings, 1)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_overlapping_constraints',
          bar_constraints=[
              fhir_path_test_base.FhirPathTestBase.build_constraint(
                  fhir_path_expression='false', key='some-key'
              ),
              fhir_path_test_base.FhirPathTestBase.build_constraint(
                  fhir_path_expression='false', key='another-key'
              ),
          ],
          foo_bar_constraints=[
              fhir_path_test_base.FhirPathTestBase.build_constraint(
                  fhir_path_expression='true', key='another-key'
              )
          ],
          expected_bindings=2,
      ),
      dict(
          testcase_name='with_disjoint_constraints',
          bar_constraints=[
              fhir_path_test_base.FhirPathTestBase.build_constraint(
                  fhir_path_expression='false', key='some-key'
              ),
              fhir_path_test_base.FhirPathTestBase.build_constraint(
                  fhir_path_expression='2', key='another-key'
              ),
          ],
          foo_bar_constraints=[
              fhir_path_test_base.FhirPathTestBase.build_constraint(
                  fhir_path_expression='true', key='another-another-key'
              )
          ],
          expected_bindings=3,
      ),
  )
  def test_encode_with_duplicate_sql_requirement_creates_constraint_and_logs_error_v2(
      self,
      bar_constraints: List[datatypes_pb2.ElementDefinition.Constraint],
      foo_bar_constraints: List[datatypes_pb2.ElementDefinition.Constraint],
      expected_bindings: int,
  ):
    # Setup resource with a defined constraint. Mimics the scenario where Bar
    # itself has a constraint and Foo defines a separate constraint on its own
    # Foo.bar field.
    foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(1, '1'),
    )

    bar_root = sdefs.build_element_definition(
        id_='Bar',
        type_codes=None,
        cardinality=sdefs.Cardinality(1, '1'),
        constraints=bar_constraints,
    )

    foo_bar = sdefs.build_element_definition(
        id_='Foo.bar',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(0, '*'),
        constraints=foo_bar_constraints,
    )

    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, foo_bar]
    )

    bar = sdefs.build_resource_definition(
        id_='Bar', element_definitions=[bar_root]
    )

    # Standup encoder v2
    error_reporter = fhir_errors.ListErrorReporter()
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, bar]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
    )

    # Ensure that a constraint is created for the cardinality of Foo, the
    # constraint from foo_bar and the third disjoint constraint in Bar.
    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, expected_bindings)

  def test_encode_with_replacement_replaces_constraint(self):
    first_constraint = self.build_constraint(
        fhir_path_expression='1 + 1', key='some-key'
    )
    second_constraint = self.build_constraint(
        fhir_path_expression='2 + 3', key='other-key'
    )

    # Setup resource with a defined constraint
    foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(1, '1'),
        constraints=[first_constraint, second_constraint],
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root]
    )

    # Standup encoder
    error_reporter = fhir_errors.ListErrorReporter()

    replace_list = fhirpath_replacement_list_pb2.FHIRPathReplacementList()
    _ = replace_list.replacement.add(
        element_path='Foo',
        expression_to_replace='1 + 1',
        replacement_expression='4 + 5',
    )

    options = fhir_path.SqlGenerationOptions(expr_replace_list=replace_list)
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo]),
        error_reporter,
        options=options,
    )

    # Ensure that we only produce a single Standard SQL requirement, and that
    # an error is logged since we were given a duplicate constraint.
    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 2)
    self.assertCountEqual(
        [binding.fhir_path_expression for binding in actual_bindings],
        ['2 + 3', '4 + 5'],
    )

    # Test with v2
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
        options=cast(fhir_path_validator_v2.SqlGenerationOptions, options),
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 2)

    self.assertCountEqual(
        [binding.fhir_path_expression for binding in actual_bindings],
        ['2 + 3', '4 + 5'],
    )

  def test_encode_with_replacement_and_no_element_path_replaces_constraint(
      self,
  ):
    first_constraint = self.build_constraint(
        fhir_path_expression='1 + 1', key='some-key'
    )
    second_constraint = self.build_constraint(
        fhir_path_expression='1 + 1', key='other-key'
    )

    # Setup resources with a defined constraint
    foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(1, '1'),
        constraints=[first_constraint],
    )
    bar = sdefs.build_element_definition(
        id_='Foo.bar',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(0, '*'),
        constraints=[second_constraint],
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, bar]
    )

    # Bar resource.
    bar_root = sdefs.build_element_definition(
        id_='Bar',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    bar = sdefs.build_resource_definition(
        id_='Bar', element_definitions=[bar_root]
    )

    # Standup encoder
    error_reporter = fhir_errors.ListErrorReporter()

    replace_list = fhirpath_replacement_list_pb2.FHIRPathReplacementList()
    _ = replace_list.replacement.add(
        element_path='',
        expression_to_replace='1 + 1',
        replacement_expression='4 + 5',
    )

    options = fhir_path.SqlGenerationOptions(expr_replace_list=replace_list)
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, bar]),
        error_reporter,
        options=options,
    )

    # Ensure that we only produce a single Standard SQL requirement, and that
    # an error is logged since we were given a duplicate constraint.
    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertLen(actual_bindings, 2)

    self.assertEqual(actual_bindings[0].fhir_path_expression, '4 + 5')
    self.assertEqual(actual_bindings[1].fhir_path_expression, '4 + 5')

    # Test v2
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: [foo, bar]),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
        options=cast(fhir_path_validator_v2.SqlGenerationOptions, options),
    )

    actual_bindings = encoder.encode(foo)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    # Unlike v1, a subquery is not generated for nonRoot elements, so the sql
    # generated is the same for both since the expression is just arithmetic and
    # does not reference any element in the field. Since the sql is the same,
    # one of the requirement sqls will be deleted upon return.
    self.assertLen(actual_bindings, 2)

    self.assertEqual(actual_bindings[0].fhir_path_expression, '4 + 5')
    self.assertEqual(actual_bindings[1].fhir_path_expression, '4 + 5')

  def test_encode_with_primitive_structure_definition_produces_no_constraints(
      self,
  ):
    # Setup primitive structure definition with 'always-fail-constraint-key'.
    constraint = self.build_constraint(
        fhir_path_expression='false', key='always-fail-constraint-key'
    )
    string_root = sdefs.build_element_definition(
        id_='string',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
        constraints=[constraint],
    )
    string = self.build_profile(id_='string', element_definitions=[string_root])

    # Setup resource with a defined constraint
    constraint = self.build_constraint(
        fhir_path_expression='false', key='always-fail-constraint-key'
    )
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    foo_name = sdefs.build_element_definition(
        id_='Foo.name',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    profile = self.build_profile(
        id_='Foo', element_definitions=[foo_root, foo_name]
    )

    # Standup encoder; adding profile and string structure definitions.
    error_reporter = fhir_errors.ListErrorReporter()
    options = fhir_path.SqlGenerationOptions(
        skip_keys=set(['always-fail-constraint-key'])
    )
    encoder = fhir_path_validator.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(
            iter_structure_definitions=lambda: [profile, string]
        ),
        error_reporter,
        options=options,
    )
    # We are expecting to not see 'always-fail-constraint-key' here because we
    # skipped encoding fields on the primitive `string`.
    actual_bindings = encoder.encode(profile)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(actual_bindings)

    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(
            iter_structure_definitions=lambda: [profile, string]
        ),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
        options=cast(fhir_path_validator_v2.SqlGenerationOptions, options),
    )
    # We are expecting to not see 'always-fail-constraint-key' here because we
    # skipped encoding fields on the primitive `string`.
    actual_bindings = encoder.encode(profile)
    self.assertEmpty(error_reporter.warnings)
    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(actual_bindings)


class FhirProfileStandardSqlEncoderV2ConstraintTest(
    FhirProfileStandardSqlEncoderTestBase
):
  """Tests Standard SQL encoding over synthetic types.

  The suite stands-up a list of synthetic resources for profiling and
  validation. The resources have the following structure:
  ```
  ChoiceTest {
    <string, integer> bar[x];
    <string> baz[x]
  }

  NestedChoiceTest {
    NestedChoice deep;
  }

  NestedChoice {
    <string, bool> deepChoice[x];
  }

  ReferenceTest {
    Reference(Patient | Device) bar
    Reference(Patient) baz
  }

  RepeatedReferenceTest {
    repeated Reference(Patient | Device) bar
  }
  ```
  """

  @classmethod
  def setUpClass(cls) -> None:
    super().setUpClass()

    # Definitions for testing choice types
    choice_test_root = sdefs.build_element_definition(
        id_='ChoiceTest', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    # Should generate SQL ensuring only one of string or integer is set.
    choice_test_bar_element_definition = sdefs.build_element_definition(
        id_='ChoiceTest.bar[x]',
        type_codes=['string', 'integer'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )

    # Should not generate any SQL because there is only one choice.
    choice_test_baz_element_definition = sdefs.build_element_definition(
        id_='ChoiceTest.baz[x]',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    choice_test = sdefs.build_resource_definition(
        id_='ChoiceTest',
        element_definitions=[
            choice_test_root,
            choice_test_bar_element_definition,
            choice_test_baz_element_definition,
        ],
    )

    nested_choice_test_root = sdefs.build_element_definition(
        id_='NestedChoiceTest',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
    )
    nested_choice_test_deep = sdefs.build_element_definition(
        id_='NestedChoiceTest.deep',
        type_codes=['NestedChoice'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    nested_choice_test = sdefs.build_resource_definition(
        id_='NestedChoiceTest',
        element_definitions=[nested_choice_test_root, nested_choice_test_deep],
    )

    nest_choice_root = sdefs.build_element_definition(
        id_='NestedChoice',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
    )
    nest_choice_choice = sdefs.build_element_definition(
        id_='NestedChoice.deepChoice[x]',
        type_codes=['string', 'bool'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    nest_choice = sdefs.build_resource_definition(
        id_='NestedChoice',
        element_definitions=[nest_choice_root, nest_choice_choice],
    )

    # Definitions for testing constraints against references.
    reference_datatype = sdefs.build_resource_definition(
        id_='Reference',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Reference',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )

    patient_datatype = sdefs.build_resource_definition(
        id_='Patient',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Patient',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )

    device_datatype = sdefs.build_resource_definition(
        id_='Device',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Device',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )

    reference_test_root = sdefs.build_element_definition(
        id_='ReferenceTest',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
    )

    # Should generate SQL ensuring only one of Patient or Device is set.
    reference_test_bar_element_definition = sdefs.build_element_definition(
        id_='ReferenceTest.bar',
        type_codes=['Reference'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    reference_test_bar_element_definition.type[0].target_profile.add(
        value='Patient'
    )
    reference_test_bar_element_definition.type[0].target_profile.add(
        value='Device'
    )

    # Should not generate any SQL because there is only one choice.
    reference_test_baz_element_definition = sdefs.build_element_definition(
        id_='ReferenceTest.baz',
        type_codes=['Reference'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    reference_test_baz_element_definition.type[0].target_profile.add(
        value='Patient'
    )

    reference_test = sdefs.build_resource_definition(
        id_='ReferenceTest',
        element_definitions=[
            reference_test_root,
            reference_test_bar_element_definition,
            reference_test_baz_element_definition,
        ],
    )

    # Ensure we handle repeated reference fields.
    repeated_reference_test_root = sdefs.build_element_definition(
        id_='RepeatedReferenceTest',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
    )

    # Should generate SQL ensuring only one of Patient or Device is
    # set for each element.
    repeated_reference_test_bar_element_definition = (
        sdefs.build_element_definition(
            id_='RepeatedReferenceTest.bar',
            type_codes=['Reference'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
        )
    )
    repeated_reference_test_bar_element_definition.type[0].target_profile.add(
        value='Patient'
    )
    repeated_reference_test_bar_element_definition.type[0].target_profile.add(
        value='Device'
    )

    repeated_reference_test = sdefs.build_resource_definition(
        id_='RepeatedReferenceTest',
        element_definitions=[
            repeated_reference_test_root,
            repeated_reference_test_bar_element_definition,
        ],
    )

    # Definitions needed for slices on codeable concept tests.
    coding = sdefs.build_resource_definition(
        id_='Coding',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Coding',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=1, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Coding.system',
                type_codes=['uri'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Coding.code',
                type_codes=['code'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Coding.version',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
        ],
    )

    codeable_concept = sdefs.build_resource_definition(
        id_='CodeableConcept',
        element_definitions=[
            sdefs.build_element_definition(
                id_='CodeableConcept',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=1, max='1'),
            ),
            sdefs.build_element_definition(
                id_='CodeableConcept.coding',
                type_codes=['Coding'],
                cardinality=sdefs.Cardinality(min=0, max='*'),
            ),
        ],
    )

    codeable_concept_slice_test = sdefs.build_resource_definition(
        id_='CodeableConceptSliceTest',
        element_definitions=[
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=1, max='1'),
            ),
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest.code',
                type_codes=['CodeableConcept'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            # Define a required slice on code.coding named 'SolarSystem'
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest.code.coding:SolarSystem',
                path='CodeableConceptSliceTest.code.coding',
                type_codes=['Coding'],
                cardinality=sdefs.Cardinality(min=1, max='1'),
                slice_name='SolarSystem',
            ),
            # code can be anything.
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest.code.coding:SolarSystem.code',
                path='CodeableConceptSliceTest.code.coding.code',
                type_codes=['code'],
                cardinality=sdefs.Cardinality(min=1, max='1'),
            ),
            # Fix a 'system' attribute on the slice.
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest.code.coding:SolarSystem.system',
                path='CodeableConceptSliceTest.code.coding.system',
                type_codes=['uri'],
                cardinality=sdefs.Cardinality(min=1, max='1'),
                fixed=datatypes_pb2.ElementDefinition.FixedX(
                    uri=datatypes_pb2.Uri(value='milky_way')
                ),
            ),
            # Fix a 'version' attribute on the slice.
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest.code.coding:SolarSystem.version',
                path='CodeableConceptSliceTest.code.coding.version',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(min=1, max='1'),
                fixed=datatypes_pb2.ElementDefinition.FixedX(
                    string_value=datatypes_pb2.String(value='final_frontier')
                ),
            ),
            # Define another slice on code.coding named 'Whatever.'
            # The slice has a min cardinality of 0, so it should not
            # generate any constraints.
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest.code.coding:Whatever',
                path='CodeableConceptSliceTest.code.coding',
                type_codes=['Coding'],
                cardinality=sdefs.Cardinality(min=0, max='*'),
                slice_name='SolarSystem',
            ),
            # Fix a 'system' attribute on the slice, but it doesn't
            # matter because the slice itself is not required.
            sdefs.build_element_definition(
                id_='CodeableConceptSliceTest.code.coding:Whatever.system',
                path='CodeableConceptSliceTest.code.coding.system',
                type_codes=['uri'],
                cardinality=sdefs.Cardinality(min=1, max='1'),
                fixed=datatypes_pb2.ElementDefinition.FixedX(
                    uri=datatypes_pb2.Uri(value='whatever')
                ),
            ),
        ],
    )

    backbone_resource = sdefs.build_resource_definition(
        id_='BackboneElement',
        element_definitions=[
            sdefs.build_element_definition(
                id_='BackboneElement',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )

    codeable_concept_backbone_slice_test = sdefs.build_resource_definition(
        id_='CodeableConceptBackboneSliceTest',
        element_definitions=[
            sdefs.build_element_definition(
                id_='CodeableConceptBackboneSliceTest',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=1, max='1'),
            ),
            # Define a backbone element named 'contact.'
            sdefs.build_element_definition(
                id_='CodeableConceptBackboneSliceTest.contact',
                type_codes=['BackboneElement'],
                cardinality=sdefs.Cardinality(0, '*'),
            ),
            sdefs.build_element_definition(
                id_='CodeableConceptBackboneSliceTest.contact.name',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(0, '1'),
            ),
            # Define a slice off the collection of 'contact' backbone elements.
            sdefs.build_element_definition(
                id_='CodeableConceptBackboneSliceTest.contact:BackboneSlice',
                path='CodeableConceptBackboneSliceTest.contact',
                type_codes=['BackboneElement'],
                cardinality=sdefs.Cardinality(2, '*'),
                slice_name='BackboneSlice',
            ),
            # Fix the names in this slice.
            sdefs.build_element_definition(
                id_='CodeableConceptBackboneSliceTest.contact:BackboneSlice.name',
                path='CodeableConceptBackboneSliceTest.contact.name',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(1, '1'),
                fixed=datatypes_pb2.ElementDefinition.FixedX(
                    string_value=datatypes_pb2.String(value='skeleton')
                ),
            ),
        ],
    )

    all_resources = [
        choice_test,
        nested_choice_test,
        nest_choice,
        patient_datatype,
        device_datatype,
        reference_datatype,
        reference_test,
        repeated_reference_test,
        coding,
        codeable_concept,
        codeable_concept_slice_test,
        backbone_resource,
        codeable_concept_backbone_slice_test,
    ]
    cls.resources = {resource.url.value: resource for resource in all_resources}

  @parameterized.named_parameters(
      dict(
          testcase_name='with_choice_type_encodes_choice_type_exclusivity',
          base_id='ChoiceTest',
          context_element_path='ChoiceTest',
          expected_sql_expression=textwrap.dedent("""\
            (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
            FROM UNNEST(ARRAY(SELECT comparison_
            FROM (SELECT ((CAST(
            bar.string IS NOT NULL AS INT64) + CAST(
            bar.integer IS NOT NULL AS INT64)) <= 1) AS comparison_)
            WHERE comparison_ IS NOT NULL)) AS result_)"""),
          fhir_path_expression=(
              "bar.ofType('string').exists().toInteger() +"
              " bar.ofType('integer').exists().toInteger() <= 1"
          ),
          fields_referenced_by_expression=['bar'],
      ),
      dict(
          testcase_name=(
              'with_nested_choice_type_encodes_choice_type_exclusivity'
          ),
          base_id='NestedChoiceTest',
          context_element_path='NestedChoiceTest.deep',
          expected_sql_expression=textwrap.dedent("""\
            (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
            FROM (SELECT ARRAY(SELECT comparison_
            FROM (SELECT ((CAST(
            deepChoice.string IS NOT NULL AS INT64) + CAST(
            deepChoice.bool IS NOT NULL AS INT64)) <= 1) AS comparison_)
            WHERE comparison_ IS NOT NULL) AS subquery_
            FROM (SELECT AS VALUE ctx_element_
            FROM UNNEST(ARRAY(SELECT deep
            FROM (SELECT deep)
            WHERE deep IS NOT NULL)) AS ctx_element_)),
            UNNEST(subquery_) AS result_)"""),
          fhir_path_expression=(
              "deepChoice.ofType('string').exists().toInteger() + "
              "deepChoice.ofType('bool').exists().toInteger() <= 1"
          ),
          fields_referenced_by_expression=['deepChoice'],
      ),
      dict(
          testcase_name=(
              'with_scalar_reference_type_encodes_reference_type_exclusivity'
          ),
          base_id='ReferenceTest',
          context_element_path='ReferenceTest',
          fields_referenced_by_expression=['bar'],
          fhir_path_expression=(
              "bar.idFor('Device').exists().toInteger() +"
              " bar.idFor('Patient').exists().toInteger() <= 1"
          ),
          expected_sql_expression=textwrap.dedent("""\
            (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
            FROM UNNEST(ARRAY(SELECT comparison_
            FROM (SELECT ((CAST(
            bar.deviceId IS NOT NULL AS INT64) + CAST(
            bar.patientId IS NOT NULL AS INT64)) <= 1) AS comparison_)
            WHERE comparison_ IS NOT NULL)) AS result_)"""),
      ),
      dict(
          testcase_name=(
              'with_vector_reference_type_encodes_reference_type_exclusivity'
          ),
          base_id='RepeatedReferenceTest',
          context_element_path='RepeatedReferenceTest',
          fields_referenced_by_expression=['bar', 'bar.bar'],
          fhir_path_expression=(
              'bar.all('
              "bar.idFor('Device').exists().toInteger() +"
              " bar.idFor('Patient').exists().toInteger() <= 1)"
          ),
          expected_sql_expression=textwrap.dedent("""\
              (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
              FROM UNNEST(ARRAY(SELECT all_
              FROM (SELECT IFNULL(
              LOGICAL_AND(
              IFNULL(
              (SELECT ((CAST(
              bar_element_.deviceId IS NOT NULL AS INT64) + CAST(
              bar_element_.patientId IS NOT NULL AS INT64)) <= 1) AS all_), FALSE)), TRUE) AS all_
              FROM UNNEST(bar) AS bar_element_ WITH OFFSET AS element_offset)
              WHERE all_ IS NOT NULL)) AS result_)"""),
      ),
      dict(
          testcase_name=(
              'with_codeable_concept_slice_encodes_system_requirement'
          ),
          base_id='CodeableConceptSliceTest',
          context_element_path='CodeableConceptSliceTest',
          fields_referenced_by_expression=[
              'code.coding',
              'code.coding.system',
              'code.coding.version',
          ],
          fhir_path_expression=(
              "code.coding.where(system = 'milky_way' and version ="
              " 'final_frontier').exists() and code.coding.where(system ="
              " 'milky_way' and version = 'final_frontier').count() <= 1"
          ),
          expected_sql_expression=textwrap.dedent("""\
              (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
              FROM UNNEST(ARRAY(SELECT logic_
              FROM (SELECT (EXISTS(
              SELECT coding_element_
              FROM (SELECT coding_element_
              FROM (SELECT code),
              UNNEST(code.coding) AS coding_element_ WITH OFFSET AS element_offset
              WHERE (NOT EXISTS(
              SELECT lhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, system
              FROM (SELECT system)) AS lhs_
              EXCEPT DISTINCT
              SELECT rhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
              FROM (SELECT 'milky_way' AS literal_)) AS rhs_) AND NOT EXISTS(
              SELECT lhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, version
              FROM (SELECT version)) AS lhs_
              EXCEPT DISTINCT
              SELECT rhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
              FROM (SELECT 'final_frontier' AS literal_)) AS rhs_)))
              WHERE coding_element_ IS NOT NULL) AND ((SELECT COUNT(
              coding_element_) AS count_
              FROM (SELECT code),
              UNNEST(code.coding) AS coding_element_ WITH OFFSET AS element_offset
              WHERE (NOT EXISTS(
              SELECT lhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, system
              FROM (SELECT system)) AS lhs_
              EXCEPT DISTINCT
              SELECT rhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
              FROM (SELECT 'milky_way' AS literal_)) AS rhs_) AND NOT EXISTS(
              SELECT lhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, version
              FROM (SELECT version)) AS lhs_
              EXCEPT DISTINCT
              SELECT rhs_.*
              FROM (SELECT ROW_NUMBER() OVER() AS row_, literal_
              FROM (SELECT 'final_frontier' AS literal_)) AS rhs_))) <= 1)) AS logic_)
              WHERE logic_ IS NOT NULL)) AS result_)"""),
      ),
      dict(
          testcase_name=(
              'with_codeable_concept_backbone_slice_encodes_system_requirement'
          ),
          base_id='CodeableConceptBackboneSliceTest',
          context_element_path='CodeableConceptBackboneSliceTest.contact',
          fields_referenced_by_expression=['contact', 'contact.name'],
          fhir_path_expression="contact.where(name = 'skeleton').count() >= 2",
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT comparison_
          FROM (SELECT ((SELECT COUNT(
          where_clause_) AS count_
          FROM (SELECT NULL AS where_clause_)) >= 2) AS comparison_)
          WHERE comparison_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT contact_element_
          FROM (SELECT contact_element_
          FROM UNNEST(contact) AS contact_element_ WITH OFFSET AS element_offset)
          WHERE contact_element_ IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
      ),
  )
  def test_encode(
      self,
      base_id: str,
      context_element_path: str,
      expected_sql_expression: str,
      fhir_path_expression: str,
      fields_referenced_by_expression: List[str],
  ):
    """Ensures we enforce an exclusivity constraint among choice type options."""
    error_reporter_v2 = fhir_errors.ListErrorReporter()
    all_resources = list(self.resources.values())
    encoder_v2 = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: all_resources),
        primitive_handler.PrimitiveHandler(),
        error_reporter_v2,
        options=fhir_path_validator_v2.SqlGenerationOptions(
            verbose_error_reporting=True
        ),
    )

    resource = self.resources[
        f'http://hl7.org/fhir/StructureDefinition/{base_id}'
    ]

    actual_bindings_v2 = encoder_v2.encode(resource)
    self.assertEmpty(error_reporter_v2.warnings)
    self.assertEmpty(error_reporter_v2.errors)
    self.assertLen(actual_bindings_v2, 1)
    self.assertEqual(actual_bindings_v2[0].element_path, context_element_path)
    self.assertEqual(
        actual_bindings_v2[0].fhir_path_expression, fhir_path_expression
    )
    self.assertEqual(
        actual_bindings_v2[0].fields_referenced_by_expression,
        fields_referenced_by_expression,
    )
    self.assertEqual(
        actual_bindings_v2[0].sql_expression, expected_sql_expression
    )


class FhirProfileStandardSqlEncoderCyclicResourceGraphTest(
    FhirProfileStandardSqlEncoderTestBase
):
  """Tests Standard SQL encoding over a cyclic FHIR resource graph."""

  @classmethod
  def setUpClass(cls) -> None:
    super().setUpClass()
    # Reference contains an optional identifier...
    reference_datatype = sdefs.build_resource_definition(
        id_='Reference',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Reference',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Reference.identifier',
                type_codes=['Identifier'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
                # Add a constraint to see how many times we visit this field.
                constraints=[
                    cls.build_constraint(
                        fhir_path_expression='0 < 1', key='from-reference'
                    )
                ],
            ),
        ],
    )

    # .. which contains an optional reference!
    identifier_datatype = sdefs.build_resource_definition(
        id_='Identifier',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Identifier',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Identifier.assigner',
                type_codes=['Reference'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
                # Add a constraint to see how many times we visit this field.
                constraints=[
                    cls.build_constraint(
                        fhir_path_expression='1 < 2', key='from-identifier'
                    )
                ],
            ),
        ],
    )

    # Risk assessment contains a reference. We should build SQL for
    # RiskAssessment.basedOn.identifier.assigner but recurse no
    # further. This matches data mapper's behaviour.
    risk_assessment_datatype = sdefs.build_resource_definition(
        id_='RiskAssessment',
        element_definitions=[
            sdefs.build_element_definition(
                id_='RiskAssessment',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='RiskAssessment.basedOn',
                type_codes=['Reference'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
        ],
    )

    all_resources = [
        reference_datatype,
        identifier_datatype,
        risk_assessment_datatype,
    ]
    cls.resources = {resource.url.value: resource for resource in all_resources}

  def test_encode_profile_with_cycle_builds_sql(self):
    error_reporter = fhir_errors.ListErrorReporter()

    all_resources = list(self.resources.values())
    encoder = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        unittest.mock.Mock(iter_structure_definitions=lambda: all_resources),
        primitive_handler.PrimitiveHandler(),
        error_reporter,
    )

    risk_assessment = self.resources[
        'http://hl7.org/fhir/StructureDefinition/RiskAssessment'
    ]

    actual_bindings = encoder.encode(risk_assessment)

    # We should have one constraint each from visiting
    # RiskAssessment.basedOn.identifier and
    # RiskAssessment.basedOn.identifier.assigner. We should have
    # stopped recursing before visiting
    # RiskAssessment.basedOn.identifier.assigner.identifier
    self.assertLen(actual_bindings, 2)
    self.assertCountEqual(
        [binding.element_path for binding in actual_bindings],
        [
            'RiskAssessment.basedOn.identifier',
            'RiskAssessment.basedOn.identifier.assigner',
        ],
    )
    self.assertCountEqual(
        [binding.fhir_path_key for binding in actual_bindings],
        ['from-reference', 'from-identifier'],
    )


class FhirProfileStandardSqlEncoderTest(FhirProfileStandardSqlEncoderTestBase):
  """A suite of tests against resources with scalar and repeated fields.

  For each test, the suite stands-up a list of synthetic resources for
  profiling and validation. The resources have the following structure:
  ```
  string {}
  integer {}
  float {}

  Address {
   string city;
   string state;
   integer zip;
  }
  Patient {
   string name;
   repeated Address addresses;

   Contact {
     string name;
   }
   Contact contact;
  }
  Position {
   float latitude;
   float longitude;
  }
  Location {
   repeated string ids;
   Address address;
   Position position;
  }
  HospitalInfo {
   string name;
   repeated Location locations;
  }
  Hospital {
   HospitalInfo info;
   repeated Patient patients;
  }
  ```

  Note that the in-line type, `Contact`, is derived from the core resource:
  `http://hl7.fhir/org/StructureDefinition/BackboneElement`.
  """

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    # Basic datatypes
    integer_datatype = sdefs.build_resource_definition(
        id_='integer',
        element_definitions=[
            sdefs.build_element_definition(
                id_='integer',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )
    float_datatype = sdefs.build_resource_definition(
        id_='float',
        element_definitions=[
            sdefs.build_element_definition(
                id_='float',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )
    string_datatype = sdefs.build_resource_definition(
        id_='string',
        element_definitions=[
            sdefs.build_element_definition(
                id_='string',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )
    backbone_element = sdefs.build_resource_definition(
        id_='BackboneElement',
        element_definitions=[
            sdefs.build_element_definition(
                id_='BackboneElement',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )

    # Position resource
    position_root = sdefs.build_element_definition(
        id_='Position', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    position_lat = sdefs.build_element_definition(
        id_='Position.latitude',
        type_codes=['float'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    position_lon = sdefs.build_element_definition(
        id_='Position.longitude',
        type_codes=['float'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    position = sdefs.build_resource_definition(
        id_='Position',
        element_definitions=[position_root, position_lat, position_lon],
    )

    # Location resource
    location_root = sdefs.build_element_definition(
        id_='Location', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    location_position = sdefs.build_element_definition(
        id_='Location.position',
        type_codes=['Position'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    location_name = sdefs.build_element_definition(
        id_='Location.name',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    location_address = sdefs.build_element_definition(
        id_='Location.address',
        type_codes=['Address'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    location_ids = sdefs.build_element_definition(
        id_='Location.ids',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    location = sdefs.build_resource_definition(
        id_='Location',
        element_definitions=[
            location_root,
            location_position,
            location_name,
            location_address,
            location_ids,
        ],
    )

    # Hospital info resource
    hospital_info_root = sdefs.build_element_definition(
        id_='HospitalInfo',
        type_codes=None,
        cardinality=sdefs.Cardinality(0, '1'),
    )
    hospital_info_name = sdefs.build_element_definition(
        id_='HospitalInfo.name',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    hospital_info_locations = sdefs.build_element_definition(
        id_='HospitalInfo.locations',
        type_codes=['Location'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    hospital_info = sdefs.build_resource_definition(
        id_='HospitalInfo',
        element_definitions=[
            hospital_info_root,
            hospital_info_name,
            hospital_info_locations,
        ],
    )

    # Hospital resource
    hospital_root = sdefs.build_element_definition(
        id_='Hospital', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    hospital_info_ = sdefs.build_element_definition(
        id_='Hospital.info',
        type_codes=['HospitalInfo'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    hospital_patients = sdefs.build_element_definition(
        id_='Hospital.patients',
        type_codes=['Patient'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    hospital = sdefs.build_resource_definition(
        id_='Hospital',
        element_definitions=[hospital_root, hospital_info_, hospital_patients],
    )

    # HumanName resource
    human_name_root = sdefs.build_element_definition(
        id_='HumanName', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    human_name_first = sdefs.build_element_definition(
        id_='HumanName.first',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    human_name_last = sdefs.build_element_definition(
        id_='HumanName.last',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    human_name = sdefs.build_resource_definition(
        id_='HumanName',
        element_definitions=[
            human_name_root,
            human_name_first,
            human_name_last,
        ],
    )

    # Patient resource
    patient_root = sdefs.build_element_definition(
        id_='Patient', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    patient_name = sdefs.build_element_definition(
        id_='Patient.name',
        type_codes=['HumanName'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    patient_addresses = sdefs.build_element_definition(
        id_='Patient.addresses',
        type_codes=['Address'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    patient_contact = sdefs.build_element_definition(
        id_='Patient.contact',
        type_codes=['BackboneElement'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    patient_contact_name = sdefs.build_element_definition(
        id_='Patient.contact.name',
        type_codes=['HumanName'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    patient = sdefs.build_resource_definition(
        id_='Patient',
        element_definitions=[
            patient_root,
            patient_name,
            patient_addresses,
            patient_contact,
            patient_contact_name,
        ],
    )

    # Address resource
    address_root = sdefs.build_element_definition(
        id_='Address', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    address_city = sdefs.build_element_definition(
        id_='Address.city',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    address_state = sdefs.build_element_definition(
        id_='Address.state',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    address_zip = sdefs.build_element_definition(
        id_='Address.zip',
        type_codes=['integer'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    address = sdefs.build_resource_definition(
        id_='Address',
        element_definitions=[
            address_root,
            address_city,
            address_state,
            address_zip,
        ],
    )

    all_resources = [
        # Mock core types
        integer_datatype,
        float_datatype,
        string_datatype,
        backbone_element,
        # Resources
        address,
        hospital,
        hospital_info,
        location,
        human_name,
        patient,
        position,
    ]
    cls.resources = {resource.url.value: resource for resource in all_resources}

  def test_encode_with_invalid_uninitialized_severity_logs_error(self):
    constraint = self.build_constraint(
        fhir_path_expression='true',
        severity=codes_pb2.ConstraintSeverityCode.INVALID_UNINITIALIZED,
    )
    self.assert_raises_fhir_path_encoding_error(
        base_id='Hospital',
        element_definition_id='Hospital',
        constraint=constraint,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_array_scalar_member_exists',
          fhir_path_expression='patients.name.exists()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT name
          FROM (SELECT patients_element_.name
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE name IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT name
          FROM (SELECT patients_element_.name
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE name IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_array_scalar_member_not_exists',
          fhir_path_expression='patients.name.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT name
          FROM (SELECT patients_element_.name
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE name IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT name
          FROM (SELECT patients_element_.name
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE name IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_scalar_array_member_exists',
          fhir_path_expression='info.locations.exists()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT locations_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE locations_element_ IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT locations_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE locations_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_array_member_exists_not',
          fhir_path_expression='info.locations.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT locations_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE locations_element_ IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT locations_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE locations_element_ IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_scalar_array_scalar_member_exists',
          fhir_path_expression='info.locations.address.exists()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT address
          FROM (SELECT locations_element_.address
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE address IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT address
          FROM (SELECT locations_element_.address
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE address IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_array_scalar_member_exists_not',
          fhir_path_expression='info.locations.address.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT address
          FROM (SELECT locations_element_.address
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE address IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT address
          FROM (SELECT locations_element_.address
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE address IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_scalar_array_scalar_scalar_member_exists',
          fhir_path_expression='info.locations.address.city.exists()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_array_scalar_scalar_member_exists_not',
          fhir_path_expression='info.locations.address.city.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_scalar_array_array_member_exists',
          fhir_path_expression='info.locations.ids.exists()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT ids_element_
          FROM (SELECT ids_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset),
          UNNEST(locations_element_.ids) AS ids_element_ WITH OFFSET AS element_offset)
          WHERE ids_element_ IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT ids_element_
          FROM (SELECT ids_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset),
          UNNEST(locations_element_.ids) AS ids_element_ WITH OFFSET AS element_offset)
          WHERE ids_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_scalar_array_array_member_exists_not',
          fhir_path_expression='info.locations.ids.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT ids_element_
          FROM (SELECT ids_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset),
          UNNEST(locations_element_.ids) AS ids_element_ WITH OFFSET AS element_offset)
          WHERE ids_element_ IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT ids_element_
          FROM (SELECT ids_element_
          FROM (SELECT locations_element_
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset),
          UNNEST(locations_element_.ids) AS ids_element_ WITH OFFSET AS element_offset)
          WHERE ids_element_ IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_array_array_member_exists',
          fhir_path_expression='patients.addresses.exists()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT addresses_element_
          FROM (SELECT addresses_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE addresses_element_ IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT addresses_element_
          FROM (SELECT addresses_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE addresses_element_ IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_array_array_member_exists_not',
          fhir_path_expression='patients.addresses.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT addresses_element_
          FROM (SELECT addresses_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE addresses_element_ IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT addresses_element_
          FROM (SELECT addresses_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE addresses_element_ IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_array_array_scalar_member_exists',
          fhir_path_expression='patients.addresses.city.exists()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL)"""),
      ),
      dict(
          testcase_name='with_array_array_scalar_member_exists_not',
          fhir_path_expression='patients.addresses.city.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_array_array_scalar_member_exists_and',
          fhir_path_expression=(
              'patients.addresses.city.exists() and '
              'patients.addresses.state.exists()'
          ),
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT logic_
          FROM (SELECT (EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL) AND EXISTS(
          SELECT state
          FROM (SELECT addresses_element_.state
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE state IS NOT NULL)) AS logic_)
          WHERE logic_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          (EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL) AND EXISTS(
          SELECT state
          FROM (SELECT addresses_element_.state
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE state IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_hospital_city_equals_patient_city',
          fhir_path_expression=(
              'info.locations.address.city = patients.addresses.city'
          ),
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT eq_
          FROM (SELECT NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_) AS eq_)
          WHERE eq_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent(
              """\
          NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_)"""
          ),
      ),
      dict(
          testcase_name='with_hospital_city_equivalent_to_patient_city',
          fhir_path_expression=(
              'info.locations.address.city ~ patients.addresses.city'
          ),
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT eq_
          FROM (SELECT NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_) AS eq_)
          WHERE eq_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent(
              """\
          NOT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_)"""
          ),
      ),
      dict(
          testcase_name='with_hospital_city_not_equals_patient_city',
          fhir_path_expression=(
              'info.locations.address.city != patients.addresses.city'
          ),
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT eq_
          FROM (SELECT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_) AS eq_)
          WHERE eq_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent(
              """\
          EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_)"""
          ),
      ),
      dict(
          testcase_name='with_hospital_city_not_equivalent_to_patient_city',
          fhir_path_expression=(
              'info.locations.address.city !~ patients.addresses.city'
          ),
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT eq_
          FROM (SELECT EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_) AS eq_)
          WHERE eq_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent(
              """\
          EXISTS(
          SELECT lhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT locations_element_.address.city
          FROM (SELECT info),
          UNNEST(info.locations) AS locations_element_ WITH OFFSET AS element_offset)) AS lhs_
          EXCEPT DISTINCT
          SELECT rhs_.*
          FROM (SELECT ROW_NUMBER() OVER() AS row_, city
          FROM (SELECT addresses_element_.city
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset),
          UNNEST(patients_element_.addresses) AS addresses_element_ WITH OFFSET AS element_offset)) AS rhs_)"""
          ),
      ),
  )
  def test_encode_with_root_fhir_path_constraint_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      expected_fhir_path_sql_expression: str,
  ):
    self.maxDiff = None
    constraint = self.build_constraint(
        fhir_path_expression=fhir_path_expression
    )
    self.assert_constraint_is_equal_to_expression(
        base_id='Hospital',
        element_definition_id='Hospital',
        constraint=constraint,
        expected_sql_expression=expected_sql_expression,
        expected_fhir_path_sql_expression=expected_fhir_path_sql_expression,
        supported_in_v2=True,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_scalar_member_access',
          fhir_path_expression='name',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT name
          FROM (SELECT name)
          WHERE name IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT patients_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE patients_element_ IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          name"""),
      ),
      dict(
          testcase_name='with_array_scalar_access',
          fhir_path_expression='addresses.city',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT city
          FROM (SELECT addresses_element_.city
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT patients_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE patients_element_ IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent(
              """\
          (SELECT addresses_element_.city
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)"""
          ),
      ),
      dict(
          testcase_name='with_literal_union_array_scalar_member',
          fhir_path_expression="'Hyrule' | addresses.state",
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT union_
          FROM (SELECT lhs_.literal_ AS union_
          FROM (SELECT \'Hyrule\' AS literal_) AS lhs_
          UNION DISTINCT
          SELECT rhs_.state AS union_
          FROM (SELECT addresses_element_.state
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset) AS rhs_)
          WHERE union_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT patients_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE patients_element_ IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent(
              """\
          (SELECT lhs_.literal_ AS union_
          FROM (SELECT \'Hyrule\' AS literal_) AS lhs_
          UNION DISTINCT
          SELECT rhs_.state AS union_
          FROM (SELECT addresses_element_.state
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset) AS rhs_)"""
          ),
      ),
      dict(
          testcase_name='with_array_array_scalar_member_exists_not',
          fhir_path_expression='addresses.city.exists().not()',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT not_
          FROM (SELECT NOT(
          EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL)) AS not_)
          WHERE not_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT patients_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE patients_element_ IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          NOT(
          EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL))"""),
      ),
      dict(
          testcase_name='with_array_array_scalar_member_exists_and_logical',
          fhir_path_expression=(
              'addresses.city.exists() and addresses.state.exists()'
          ),
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT logic_
          FROM (SELECT (EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL) AND EXISTS(
          SELECT state
          FROM (SELECT addresses_element_.state
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE state IS NOT NULL)) AS logic_)
          WHERE logic_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT patients_element_
          FROM (SELECT patients_element_
          FROM UNNEST(patients) AS patients_element_ WITH OFFSET AS element_offset)
          WHERE patients_element_ IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          (EXISTS(
          SELECT city
          FROM (SELECT addresses_element_.city
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE city IS NOT NULL) AND EXISTS(
          SELECT state
          FROM (SELECT addresses_element_.state
          FROM UNNEST(addresses) AS addresses_element_ WITH OFFSET AS element_offset)
          WHERE state IS NOT NULL))"""),
      ),
  )
  def test_encode_with_non_root_fhir_path_constraint_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      expected_fhir_path_sql_expression: str,
  ):
    """Tests that a "transitive constraint" is properly encoded.

    A "transitive constraint" is a constraint defined relative to a resource
    elsewhere in the FHIR resource graph than what we're querying against.

    Args:
      fhir_path_expression: The FHIRPath expression to encode.
      expected_sql_expression: The expected generated Standard SQL.
      expected_fhir_path_sql_expression: The expected generated Standard SQL
        without any contextual subqueries.
    """
    self.maxDiff = None
    constraint = self.build_constraint(
        fhir_path_expression=fhir_path_expression
    )
    self.assert_constraint_is_equal_to_expression(
        base_id='Hospital',
        element_definition_id='Hospital.patients',
        constraint=constraint,
        expected_sql_expression=expected_sql_expression,
        expected_fhir_path_sql_expression=expected_fhir_path_sql_expression,
        supported_in_v2=True,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_repeated_backbone_element_member_exists',
          fhir_path_expression='first.exists()',
          expected_sql_expression_v1=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT exists_
          FROM (SELECT first IS NOT NULL AS exists_)
          WHERE exists_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT name
          FROM (SELECT contact_element_.name
          FROM UNNEST(contact) AS contact_element_ WITH OFFSET AS element_offset)
          WHERE name IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_sql_expression_v2=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT exists_
          FROM (SELECT EXISTS(
          SELECT first
          FROM (SELECT first)
          WHERE first IS NOT NULL) AS exists_)
          WHERE exists_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT name
          FROM (SELECT contact_element_.name
          FROM UNNEST(contact) AS contact_element_ WITH OFFSET AS element_offset)
          WHERE name IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          EXISTS(
          SELECT first
          FROM (SELECT first)
          WHERE first IS NOT NULL)"""),
      )
  )
  def test_encode_with_backbone_element_constraint_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression_v1: str,
      expected_sql_expression_v2: str,
      expected_fhir_path_sql_expression: str,
  ):
    """Tests encoding of a "transitive constraint" defined on a BackboneElement.

    A "transitive constraint" is a constraint defined relative to a resource
    elsewhere in the FHIR resource graph than what we're querying against.

    Args:
      fhir_path_expression: The FHIRPath expression to encode.
      expected_sql_expression_v1: The expected generated Standard SQL from v1.
      expected_sql_expression_v2: The expected generated Standard SQL from v2.
      expected_fhir_path_sql_expression: The expected generated Standard SQL
        without any contextual subqueries.
    """
    constraint = self.build_constraint(
        fhir_path_expression=fhir_path_expression
    )
    self.assert_constraint_is_equal_to_expression(
        base_id='Patient',
        element_definition_id='Patient.contact.name',
        constraint=constraint,
        expected_sql_expression=expected_sql_expression_v1,
        expected_fhir_path_sql_expression=expected_fhir_path_sql_expression,
        supported_in_v2=True,
        expected_sql_expression_v2=expected_sql_expression_v2,
    )


# TODO(b/201111782): Add support in fhir_path_test.py for checking if we can
# encode more than one required field at a time..
class FhirProfileStandardSqlEncoderTestWithRequiredFields(
    FhirProfileStandardSqlEncoderTestBase
):
  """A suite of tests against a simple resource with a required field.

  For each test, the suite stands-up a list of synthetic resources for
  validation. The resources have the following structure:
  ```

  string {}

  Foo {
    string id; # This is a required field.
  }

  Bar{
    Deep deep;
  }
  Deep{
    string deeper; # This is a required field.
  }

  Baz{
    InlineElement {
      string value; # This is a required field.
    }
  }

  Tom{
    repeated string jerry; # This is a required field.
  }

  NewFoo {
    string someExtension; # This is a required field.
  }

  Foob {
    Bar boof; # The deep field is required for Foo.
  }
  """

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    cls.maxDiff = None

    string_datatype = sdefs.build_resource_definition(
        id_='string',
        element_definitions=[
            sdefs.build_element_definition(
                id_='string',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )
    backbone_element = sdefs.build_resource_definition(
        id_='BackboneElement',
        element_definitions=[
            sdefs.build_element_definition(
                id_='BackboneElement',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'),
            )
        ],
    )

    # Foob resource.
    foob_root = sdefs.build_element_definition(
        id_='Foob', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )

    foob_boof = sdefs.build_element_definition(
        id_='Foob.boof',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(1, '1'),
    )

    foob_boof_deep = sdefs.build_element_definition(
        id_='Foob.boof.deep',
        type_codes=['Deep'],
        cardinality=sdefs.Cardinality(1, '1'),
    )
    foob = sdefs.build_resource_definition(
        id_='Foob', element_definitions=[foob_root, foob_boof, foob_boof_deep]
    )

    # Foo resource.
    foo_root = sdefs.build_element_definition(
        id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    foo_id = sdefs.build_element_definition(
        id_='Foo.id',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(1, '1'),
    )
    foo = sdefs.build_resource_definition(
        id_='Foo', element_definitions=[foo_root, foo_id]
    )

    # Deep resource.
    deep_root = sdefs.build_element_definition(
        id_='Deep', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    deep_deeper = sdefs.build_element_definition(
        id_='Deep.deeper',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(1, '1'),
    )
    deep = sdefs.build_resource_definition(
        id_='Deep', element_definitions=[deep_root, deep_deeper]
    )

    # Bar resource.
    bar_root = sdefs.build_element_definition(
        id_='Bar', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    bar_deep = sdefs.build_element_definition(
        id_='Bar.deep',
        type_codes=['Deep'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    bar = sdefs.build_resource_definition(
        id_='Bar', element_definitions=[bar_root, bar_deep]
    )

    # Baz resource.
    baz_root = sdefs.build_element_definition(
        id_='Baz', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    inline_element_definition = sdefs.build_element_definition(
        id_='Baz.inline',
        type_codes=['BackboneElement'],
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    inline_value_element_definition = sdefs.build_element_definition(
        id_='Baz.inline.value',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    baz = sdefs.build_resource_definition(
        id_='Baz',
        element_definitions=[
            baz_root,
            inline_element_definition,
            inline_value_element_definition,
        ],
    )

    # Tom resource
    tom_root = sdefs.build_element_definition(
        id_='Tom', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    tom_jerry = sdefs.build_element_definition(
        id_='Tom.jerry',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(1, '5'),
    )
    tom = sdefs.build_resource_definition(
        id_='Tom', element_definitions=[tom_root, tom_jerry]
    )

    # NewFoo resource.
    new_foo_root = sdefs.build_element_definition(
        id_='NewFoo', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    new_foo_some_extension = sdefs.build_element_definition(
        id_='NewFoo.extension:someExtension',
        path='NewFoo.extension',
        cardinality=sdefs.Cardinality(1, '1'),
        type_codes=['Extension'],
        profiles=['http://hl7.org/fhir/StructureDefinition/CustomExtension'],
    )
    new_foo = sdefs.build_resource_definition(
        id_='NewFoo', element_definitions=[new_foo_root, new_foo_some_extension]
    )

    # CustomExtension resource.
    custom_extension_root_element = sdefs.build_element_definition(
        id_='Extension',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    value_element_definition = sdefs.build_element_definition(
        id_='Extension.value',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    custom_extension = sdefs.build_resource_definition(
        id_='CustomExtension',
        element_definitions=[
            custom_extension_root_element,
            value_element_definition,
        ],
    )

    all_resources = [
        string_datatype,
        backbone_element,
        foo,
        foob,
        deep,
        bar,
        baz,
        tom,
        new_foo,
        custom_extension,
    ]

    cls.resources = {resource.url.value: resource for resource in all_resources}

  @parameterized.named_parameters(
      dict(
          testcase_name='with_simple_required_field',
          base_id='Foo',
          required_field='id',
          context_element_path='Foo',
          expected_column_name='foo_id_cardinality_is_valid',
          description='The length of id must be maximum 1 and minimum 1.',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT id IS NOT NULL AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          (id IS NOT NULL)"""),
          fhir_path_expression='id.exists()',
          fields_referenced_by_expression=['id'],
      ),
      dict(
          testcase_name='with_deep_required_field',
          base_id='Bar',
          required_field='deeper',
          context_element_path='Bar.deep',
          expected_column_name='bar_deep_deeper_cardinality_is_valid',
          description='The length of deeper must be maximum 1 and minimum 1.',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT exists_
          FROM (SELECT deeper IS NOT NULL AS exists_)
          WHERE exists_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT deep
          FROM (SELECT deep)
          WHERE deep IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          (deeper IS NOT NULL)"""),
          fhir_path_expression='deeper.exists()',
          fields_referenced_by_expression=['deeper'],
      ),
      dict(
          testcase_name='with_child_of_backbone_element',
          base_id='Baz',
          required_field='value',
          context_element_path='Baz.inline',
          expected_column_name='baz_inline_value_cardinality_is_valid',
          description='The length of value must be maximum 1 and minimum 1.',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM (SELECT ARRAY(SELECT exists_
          FROM (SELECT value IS NOT NULL AS exists_)
          WHERE exists_ IS NOT NULL) AS subquery_
          FROM (SELECT AS VALUE ctx_element_
          FROM UNNEST(ARRAY(SELECT inline
          FROM (SELECT inline)
          WHERE inline IS NOT NULL)) AS ctx_element_)),
          UNNEST(subquery_) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          (value IS NOT NULL)"""),
          fhir_path_expression='value.exists()',
          fields_referenced_by_expression=['value'],
      ),
      dict(
          testcase_name='with_repeated_required_field',
          base_id='Tom',
          required_field='jerry',
          context_element_path='Tom',
          expected_column_name='tom_jerry_cardinality_is_valid',
          description='The length of jerry must be maximum 5 and minimum 1.',
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT logic_
          FROM (SELECT (((SELECT COUNT(
          jerry_element_) AS count_
          FROM UNNEST(jerry) AS jerry_element_ WITH OFFSET AS element_offset) <= 5) AND EXISTS(
          SELECT jerry_element_
          FROM (SELECT jerry_element_
          FROM UNNEST(jerry) AS jerry_element_ WITH OFFSET AS element_offset)
          WHERE jerry_element_ IS NOT NULL)) AS logic_)
          WHERE logic_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          (((SELECT COUNT(
          jerry_element_) AS count_
          FROM UNNEST(jerry) AS jerry_element_ WITH OFFSET AS element_offset) <= 5) AND EXISTS(
          SELECT jerry_element_
          FROM (SELECT jerry_element_
          FROM UNNEST(jerry) AS jerry_element_ WITH OFFSET AS element_offset)
          WHERE jerry_element_ IS NOT NULL))"""),
          fhir_path_expression='jerry.count() <= 5 and jerry.exists()',
          fields_referenced_by_expression=['jerry'],
      ),
      dict(
          testcase_name='with_slice_on_extension_that_is_required',
          base_id='NewFoo',
          required_field='someExtension',
          context_element_path='NewFoo',
          expected_column_name='newfoo_someextension_cardinality_is_valid',
          description=(
              'The length of someExtension must be maximum 1 and minimum 1.'
          ),
          expected_sql_expression=textwrap.dedent("""\
          (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
          FROM UNNEST(ARRAY(SELECT exists_
          FROM (SELECT someExtension IS NOT NULL AS exists_)
          WHERE exists_ IS NOT NULL)) AS result_)"""),
          expected_fhir_path_sql_expression=textwrap.dedent("""\
          (someExtension IS NOT NULL)"""),
          fhir_path_expression='someExtension.exists()',
          fields_referenced_by_expression=['someExtension'],
      ),
  )
  def test_encode_with_required_field_generates_sql(
      self,
      base_id: str,
      required_field: str,
      context_element_path: str,
      expected_column_name: str,
      expected_sql_expression: str,
      expected_fhir_path_sql_expression: str,
      description: Optional[str] = None,
      fhir_path_key: Optional[str] = None,
      fhir_path_expression: Optional[str] = None,
      fields_referenced_by_expression: Optional[List[str]] = None,
  ):
    self.assert_encoder_generates_expression_for_required_field(
        base_id=base_id,
        required_field=required_field,
        context_element_path=context_element_path,
        expected_column_name=expected_column_name,
        expected_sql_expression=expected_sql_expression,
        expected_fhir_path_sql_expression=expected_fhir_path_sql_expression,
        description=description,
        fhir_path_key=fhir_path_key,
        fhir_path_expression=fhir_path_expression,
        fields_referenced_by_expression=fields_referenced_by_expression,
        supported_in_v2=True,
    )

  def test_encode_with_overridden_field_in_nested_struct_generates_sql(self):
    """Tests that a Resource that overrides the cardinality of another field generates sql."""
    resource = self.resources['http://hl7.org/fhir/StructureDefinition/Foob']

    # Encode as Standard SQL expression.
    all_resources = list(self.resources.values())
    error_reporter = fhir_errors.ListErrorReporter()
    profile_std_sql_encoder_v2 = (
        fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
            unittest.mock.Mock(
                iter_structure_definitions=lambda: all_resources
            ),
            primitive_handler.PrimitiveHandler(),
            error_reporter,
        )
    )
    actual_bindings = profile_std_sql_encoder_v2.encode(resource)

    self.assertEmpty(error_reporter.errors)
    self.assertEmpty(error_reporter.warnings)
    self.assertLen(actual_bindings, 3)

    expected_binding_0 = validation_pb2.SqlRequirement(
        column_name='foob_boof_cardinality_is_valid',
        sql_expression=textwrap.dedent("""\
        (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
        FROM UNNEST(ARRAY(SELECT exists_
        FROM (SELECT boof IS NOT NULL AS exists_)
        WHERE exists_ IS NOT NULL)) AS result_)"""),
        fhir_path_sql_expression=textwrap.dedent("""\
        (boof IS NOT NULL)"""),
        severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
        type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
        element_path='Foob',
        description='The length of boof must be maximum 1 and minimum 1.',
        fhir_path_key='boof-cardinality-is-valid',
        fhir_path_expression='boof.exists()',
        fields_referenced_by_expression=['boof'],
    )
    expected_binding_1 = validation_pb2.SqlRequirement(
        column_name='foob_boof_deep_cardinality_is_valid',
        sql_expression=textwrap.dedent("""\
        (SELECT IFNULL(LOGICAL_AND(result_), TRUE)
        FROM (SELECT ARRAY(SELECT exists_
        FROM (SELECT deep IS NOT NULL AS exists_)
        WHERE exists_ IS NOT NULL) AS subquery_
        FROM (SELECT AS VALUE ctx_element_
        FROM UNNEST(ARRAY(SELECT boof
        FROM (SELECT boof)
        WHERE boof IS NOT NULL)) AS ctx_element_)),
        UNNEST(subquery_) AS result_)"""),
        fhir_path_sql_expression=textwrap.dedent("""\
        (deep IS NOT NULL)"""),
        severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
        type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
        element_path='Foob.boof',
        description='The length of deep must be maximum 1 and minimum 1.',
        fhir_path_key='deep-cardinality-is-valid',
        fhir_path_expression='deep.exists()',
        fields_referenced_by_expression=['deep'],
    )
    self.assertEqual(actual_bindings[0], expected_binding_0)
    self.assertEqual(actual_bindings[1], expected_binding_1)
    self.assertEqual(
        actual_bindings[2].fhir_path_key, 'deeper-cardinality-is-valid'
    )


if __name__ == '__main__':
  absltest.main()

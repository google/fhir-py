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
"""Tests Python FHIRPath functionality for Spark."""

from typing import Optional
import unittest.mock

from absl.testing import absltest
from absl.testing import parameterized

from google.fhir.r4.proto.core.resources import value_set_pb2

from google.fhir.core.fhir_path import _spark_interpreter
from google.fhir.core.fhir_path import fhir_path_test_base


from google.fhir.core.utils import fhir_package


_WITH_FHIRPATH_V2_DATETIME_LITERAL_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_null',
        'fhir_path_expression': '{ }',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT NULL AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_boolean_true',
        'fhir_path_expression': 'true',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT TRUE AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_boolean_false',
        'fhir_path_expression': 'false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT FALSE AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_string',
        'fhir_path_expression': "'Foo'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT 'Foo' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_number_decimal',
        'fhir_path_expression': '3.14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT 3.14 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_number_large_decimal',
        # 32 decimal places
        'fhir_path_expression': '3.14141414141414141414141414141414',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT 3.14141414141414141414141414141414 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_number_integer',
        'fhir_path_expression': '314',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT 314 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_year',
        'fhir_path_expression': '@1970',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('1970-01-01' AS TIMESTAMP) AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_year_month',
        'fhir_path_expression': '@1970-02',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('1970-02-01' AS TIMESTAMP) AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_year_month_day',
        'fhir_path_expression': '@1970-02-03',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('1970-02-03' AS TIMESTAMP) AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_time_year_month_day_hours',
        'fhir_path_expression': '@2015-02-04T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP) "
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_time_year_month_day_hours_minutes',
        'fhir_path_expression': '@2015-02-04T14:34',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:00+00:00' AS TIMESTAMP) "
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_time_year_month_day_hours_minutes_seconds',
        'fhir_path_expression': '@2015-02-04T14:34:28',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) "
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_date_time_year_month_day_hours_minutes_seconds_milli'
        ),
        'fhir_path_expression': '@2015-02-04T14:34:28.123',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:28.123000+00:00' "
            'AS TIMESTAMP) '
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_date_time_year_month_day_hours_minutes_seconds_milli_tz'
        ),
        'fhir_path_expression': '@2015-02-04T14:34:28.123+09:00',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:28.123000+09:00' "
            'AS TIMESTAMP) '
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_time_hours',
        'fhir_path_expression': '@T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_time_hours_minutes',
        'fhir_path_expression': '@T14:34',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14:34' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_time_hours_minutes_seconds',
        'fhir_path_expression': '@T14:34:28',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14:34:28' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_time_hours_minutes_seconds_milli',
        'fhir_path_expression': '@T14:34:28.123',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14:34:28.123' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_quantity',
        'fhir_path_expression': "10 'mg'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '10 'mg'' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_ARITHMETIC_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_integer_addition',
        'fhir_path_expression': '1 + 2',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (1 + 2) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_addition',
        'fhir_path_expression': '3.14 + 1.681',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (3.14 + 1.681) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_division',
        'fhir_path_expression': '3 / 2',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (3 / 2) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_division',
        'fhir_path_expression': '3.14 / 1.681',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (3.14 / 1.681) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_modular_arithmetic',
        'fhir_path_expression': '2 mod 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT MOD(2, 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_multiplication',
        'fhir_path_expression': '2 * 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2 * 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_multiplication',
        'fhir_path_expression': '2.124 * 5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2.124 * 5.72) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_subtraction',
        'fhir_path_expression': '2 - 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2 - 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_subtraction',
        'fhir_path_expression': '2.124 - 5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2.124 - 5.72) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_trunctated_division',
        'fhir_path_expression': '2 div 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT DIV(2, 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_truncated_division',
        'fhir_path_expression': '2.124 div 5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT DIV(2.124, 5.72) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_addition_and_multiplication',
        'fhir_path_expression': '(1 + 2) * 3',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT ((1 + 2) * 3) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_subtraction_and_division',
        'fhir_path_expression': '(21 - 6) / 3',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT ((21 - 6) / 3) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_addition_and_modular_arithmetic',
        'fhir_path_expression': '21 + 6 mod 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (21 + MOD(6, 5)) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_addition_and_truncated_division',
        'fhir_path_expression': '21 + 6 div 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (21 + DIV(6, 5)) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_string_concatenation_ampersand',
        'fhir_path_expression': "'foo' & 'bar'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            "FROM (SELECT CONCAT('foo', 'bar') AS arith_) "
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_string_concatenation_plus',
        'fhir_path_expression': "'foo' + 'bar'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            "FROM (SELECT CONCAT('foo', 'bar') AS arith_) "
            'WHERE arith_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_INDEXER_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_integer_indexer',
        'fhir_path_expression': '7[0]',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
            'element_at(COLLECT_LIST(literal_),0 + 1) AS indexed_literal_ FROM '
            '(SELECT 7 AS literal_)) WHERE indexed_literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_indexer_arithmetic_index',
        'fhir_path_expression': '7[0 + 1]',  # Out-of-bounds, empty table
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
            'element_at(COLLECT_LIST(literal_),(0 + 1) + 1) AS '
            'indexed_literal_ FROM (SELECT 7 AS literal_)) WHERE '
            'indexed_literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_string_indexer',
        'fhir_path_expression': "'foo'[0]",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
            'element_at(COLLECT_LIST(literal_),0 + 1) AS indexed_literal_ FROM '
            "(SELECT 'foo' AS literal_)) WHERE indexed_literal_ IS NOT NULL)"
        ),
    },
]

_WITH_FHIRPATH_V2_BOOLEAN_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_boolean_and',
        'fhir_path_expression': 'true and false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT TRUE AND FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_boolean_or',
        'fhir_path_expression': 'true or false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT TRUE OR FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_boolean_xor',
        'fhir_path_expression': 'true xor false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT TRUE <> FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_boolean_implies',
        'fhir_path_expression': 'true implies false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT NOT TRUE OR FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_boolean_relation_between_string_integer',
        'fhir_path_expression': "3 and 'true'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT (3 IS NOT NULL) AND '
            "('true' IS NOT NULL) AS logic_) WHERE logic_ IS NOT NULL)"
        ),
    },
]

_WITH_FHIRPATH_V2_COMPARISON_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_integer_greater_than',
        'fhir_path_expression': '4 > 3',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 4 > 3 AS '
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_less_than',
        'fhir_path_expression': '3 < 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 3 < 4 AS '
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_less_than_or_equal_to',
        'fhir_path_expression': '3 <= 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 3 <= 4 AS '
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_float_less_than_or_equal_to',
        'fhir_path_expression': '3.14159 <= 4.00000',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 3.14159 <= 4.00000'
            ' AS comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_string_greater_than',
        'fhir_path_expression': " 'a' > 'b'",
        'expected_sql_expression': (
            "(SELECT COLLECT_LIST(comparison_) FROM (SELECT 'a' > 'b' AS "
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_less_than',
        'fhir_path_expression': 'dateField < @2000-01-01',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) '
            'FROM (SELECT CAST(dateField AS TIMESTAMP) '
            "< CAST('2000-01-01' AS TIMESTAMP) AS comparison_) "
            'WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_date_compared_with_timestamp',
        'fhir_path_expression': 'dateField < @2000-01-01T14:34',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) '
            'FROM (SELECT CAST(dateField AS TIMESTAMP) '
            "< CAST('2000-01-01T14:34:00+00:00' AS TIMESTAMP) AS comparison_) "
            'WHERE comparison_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_POLARITY_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_integer_positive_polarity',
        'fhir_path_expression': '+5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT +5 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_positive_polarity',
        'fhir_path_expression': '+5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT +5.72 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_negative_polarity',
        'fhir_path_expression': '-5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT -5 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_negative_polarity',
        'fhir_path_expression': '-5.1349',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT -5.1349 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_positive_polarity_and_addition',
        'fhir_path_expression': '+5 + 10',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (+5 + 10) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_negative_polarity_and_addition',
        'fhir_path_expression': '-5 + 10',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (-5 + 10) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_integer_negative_polarity_and_modular_arithmetic'
        ),
        'fhir_path_expression': '-5 mod 6',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT MOD(-5, 6) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_integer_positive_polarity_and_modular_arithmetic'
        ),
        'fhir_path_expression': '+(7 mod 6)',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(pol_) '
            'FROM (SELECT +MOD(7, 6) AS pol_) '
            'WHERE pol_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_negative_polarity_and_multiplication',
        'fhir_path_expression': '-(3.79 * 2.124)',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(pol_) '
            'FROM (SELECT -(3.79 * 2.124) AS pol_) '
            'WHERE pol_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_decimal_negative_polarity_and_division',
        'fhir_path_expression': '-3.79 / 2.124',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (-3.79 / 2.124) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_MEMBERSHIP_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_integer_in',
        'fhir_path_expression': '3 in 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(mem_) '
            'FROM (SELECT (3) IN (4) AS mem_) '
            'WHERE mem_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_contains',
        'fhir_path_expression': '3 contains 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(mem_) '
            'FROM (SELECT (4) IN (3) AS mem_) '
            'WHERE mem_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_EQUALITY_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_integer_equal',
        'fhir_path_expression': '3 = 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 = 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_equivalent',
        'fhir_path_expression': '3 ~ 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 = 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_time_equal',
        'fhir_path_expression': '@2015-02-04T14:34:28 = @2015-02-04T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM '
            "(SELECT (CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) "
            "= CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP)) AS eq_) "
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_date_time_equivalent',
        'fhir_path_expression': '@2015-02-04T14:34:28 ~ @2015-02-04T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM '
            "(SELECT (CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) "
            "= CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP)) AS eq_) "
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_not_equal_to',
        'fhir_path_expression': '3 != 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 != 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_integer_not_equivalent_to',
        'fhir_path_expression': '3 !~ 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 != 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_complex_comparison_right_side_scalar',
        'fhir_path_expression': "bar.bats.struct.value = '123'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM (SELECT NOT EXISTS('
            " ARRAY_EXCEPT((SELECT ARRAY(value)), (SELECT ARRAY('123'))), x ->"
            ' x IS NOT NULL) AS eq_ FROM (SELECT (SELECT'
            ' bats_element_.struct.value FROM (SELECT bar) LATERAL VIEW'
            ' POSEXPLODE(bar.bats) AS index_bats_element_, bats_element_)))'
            ' WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_complex_comparison_left_side_scalar',
        'fhir_path_expression': " '123' = bar.bats.struct.value",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM (SELECT NOT EXISTS('
            " ARRAY_EXCEPT((SELECT ARRAY(value)), (SELECT ARRAY('123'))), x ->"
            ' x IS NOT NULL) AS eq_ FROM (SELECT (SELECT'
            ' bats_element_.struct.value FROM (SELECT bar) LATERAL VIEW'
            ' POSEXPLODE(bar.bats) AS index_bats_element_, bats_element_)))'
            ' WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_complex_comparison_right_side_union',
        'fhir_path_expression': "bar.bats.struct.value = ('abc' | '123')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM (SELECT NOT EXISTS('
            ' ARRAY_EXCEPT((SELECT ARRAY(value)), (SELECT ARRAY_AGG(union_)'
            " FROM (SELECT lhs_.literal_ AS union_ FROM (SELECT 'abc' AS"
            ' literal_) AS lhs_ UNION DISTINCT SELECT rhs_.literal_ AS union_'
            " FROM (SELECT '123' AS literal_) AS rhs_))), x -> x IS NOT NULL)"
            ' AS eq_ FROM (SELECT (SELECT bats_element_.struct.value FROM'
            ' (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS'
            ' index_bats_element_, bats_element_))) WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_complex_comparison_left_side_union',
        'fhir_path_expression': "('abc' | '123') = bar.bats.struct.value",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM (SELECT NOT EXISTS('
            ' ARRAY_EXCEPT((SELECT ARRAY(value)), (SELECT ARRAY_AGG(union_)'
            " FROM (SELECT lhs_.literal_ AS union_ FROM (SELECT 'abc' AS"
            ' literal_) AS lhs_ UNION DISTINCT SELECT rhs_.literal_ AS union_'
            " FROM (SELECT '123' AS literal_) AS rhs_))), x -> x IS NOT NULL)"
            ' AS eq_ FROM (SELECT (SELECT bats_element_.struct.value FROM'
            ' (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS'
            ' index_bats_element_, bats_element_))) WHERE eq_ IS NOT'
            ' NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_MEMBER_ACCESS_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_single_member_access',
        'fhir_path_expression': 'bar',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bar) FROM (SELECT bar) WHERE bar IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_inline_member_access',
        'fhir_path_expression': 'inline',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(inline) '
            'FROM (SELECT inline) '
            'WHERE inline IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_nested_member_access',
        'fhir_path_expression': 'bar.bats',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bats_element_) '
            'FROM (SELECT bats_element_ '
            'FROM (SELECT bar) '
            'LATERAL VIEW POSEXPLODE(bar.bats) AS index_bats_element_, '
            'bats_element_) '
            'WHERE bats_element_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_inline_nested_member_access',
        'fhir_path_expression': 'inline.value',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(value) '
            'FROM (SELECT inline.value) '
            'WHERE value IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_member_sql_keyword_access',
        'fhir_path_expression': 'bar.bats.struct',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(`struct`) '
            'FROM (SELECT bats_element_.struct '
            'FROM (SELECT bar) '
            'LATERAL VIEW POSEXPLODE(bar.bats) AS index_bats_element_, '
            'bats_element_) '
            'WHERE `struct` IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_member_fhir_path_keyword_access',
        'fhir_path_expression': 'bar.bats.`div`',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(div) '
            'FROM (SELECT bats_element_.div '
            'FROM (SELECT bar) '
            'LATERAL VIEW POSEXPLODE(bar.bats) AS index_bats_element_, '
            'bats_element_) '
            'WHERE div IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_scalar_member_fhir_path_access',
        'fhir_path_expression': 'bat.struct.anotherStruct.anotherValue',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(anotherValue) '
            'FROM (SELECT bat.struct.anotherStruct.anotherValue) '
            'WHERE anotherValue IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_first_element_being_repeated_member_fhir_path_access'
        ),
        'fhir_path_expression': 'boolList',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(boolList_element_) '
            'FROM (SELECT boolList_element_ '
            'FROM (SELECT EXPLODE(boolList_element_) AS boolList_element_ '
            'FROM (SELECT boolList AS boolList_element_))) '
            'WHERE boolList_element_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_OFTYPE_FUNCTION_SUCCEEDS_CASES = [
    # TODO(b/262544393): Add examples with exists() and where() once functions
    # are implemented
    {
        'testcase_name': '_with_choice_no_type',
        'fhir_path_expression': 'choiceExample',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(choiceExample) FROM (SELECT choiceExample)'
            ' WHERE choiceExample IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_choice_string_type',
        'fhir_path_expression': "choiceExample.ofType('string')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(ofType_) FROM (SELECT choiceExample.string AS'
            ' ofType_) WHERE ofType_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_choice_integer_type',
        'fhir_path_expression': "choiceExample.ofType('integer')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(ofType_) FROM (SELECT choiceExample.integer'
            ' AS ofType_) WHERE ofType_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_array_with_choice',
        'fhir_path_expression': "multipleChoiceExample.ofType('integer')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(ofType_) '
            'FROM (SELECT multipleChoiceExample_element_.integer AS ofType_ '
            'FROM (SELECT EXPLODE(multipleChoiceExample_element_) '
            'AS multipleChoiceExample_element_ '
            'FROM (SELECT multipleChoiceExample AS '
            'multipleChoiceExample_element_))) WHERE ofType_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_scalar_with_repeated_message_choice',
        'fhir_path_expression': (
            "choiceExample.ofType('CodeableConcept').coding"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(coding_element_) FROM (SELECT coding_element_'
            ' FROM (SELECT choiceExample.CodeableConcept AS ofType_) LATERAL'
            ' VIEW POSEXPLODE(ofType_.coding) AS index_coding_element_,'
            ' coding_element_) WHERE coding_element_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_array_with_message_choice',
        'fhir_path_expression': (
            "multipleChoiceExample.ofType('CodeableConcept').coding"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(coding_element_) FROM (SELECT coding_element_'
            ' FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS'
            ' ofType_ FROM (SELECT EXPLODE(multipleChoiceExample_element_) AS'
            ' multipleChoiceExample_element_ FROM (SELECT multipleChoiceExample'
            ' AS multipleChoiceExample_element_))) LATERAL VIEW'
            ' POSEXPLODE(ofType_.coding) AS index_coding_element_,'
            ' coding_element_) WHERE coding_element_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_array_with_message_choice_and_identifier',
        'fhir_path_expression': (
            "multipleChoiceExample.ofType('CodeableConcept').coding.system"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(system) FROM (SELECT coding_element_.system'
            ' FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS'
            ' ofType_ FROM (SELECT EXPLODE(multipleChoiceExample_element_) AS'
            ' multipleChoiceExample_element_ FROM (SELECT multipleChoiceExample'
            ' AS multipleChoiceExample_element_))) LATERAL VIEW'
            ' POSEXPLODE(ofType_.coding) AS index_coding_element_,'
            ' coding_element_) WHERE system IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_array_with_message_choice_and_equality',
        'fhir_path_expression': (
            "multipleChoiceExample.ofType('CodeableConcept').coding.system ="
            " 'test'"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM (SELECT NOT EXISTS('
            " ARRAY_EXCEPT((SELECT ARRAY(system)), (SELECT ARRAY('test'))), x"
            ' -> x IS NOT NULL) AS eq_ FROM (SELECT (SELECT'
            ' coding_element_.system FROM (SELECT'
            ' multipleChoiceExample_element_.CodeableConcept AS ofType_ FROM'
            ' (SELECT EXPLODE(multipleChoiceExample_element_) AS'
            ' multipleChoiceExample_element_ FROM (SELECT multipleChoiceExample'
            ' AS multipleChoiceExample_element_))) LATERAL VIEW'
            ' POSEXPLODE(ofType_.coding) AS index_coding_element_,'
            ' coding_element_))) WHERE eq_ IS NOT NULL)'
        ),
    },
]


_WITH_FHIRPATH_V2_FHIRPATH_FUNCTION_INVOCATION_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_member_count',
        'fhir_path_expression': 'bar.count()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(count_) '
            'FROM (SELECT COUNT( bar) AS count_ '
            'FROM (SELECT bar)) '
            'WHERE count_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_member_sql_keyword_count',
        'fhir_path_expression': 'bar.bats.struct.count()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(count_) '
            'FROM (SELECT COUNT( bats_element_.struct) AS count_ '
            'FROM (SELECT bar) '
            'LATERAL VIEW POSEXPLODE(bar.bats) AS index_bats_element_, '
            'bats_element_) '
            'WHERE count_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_member_empty',
        'fhir_path_expression': 'bar.empty()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(empty_) '
            'FROM (SELECT bar IS NULL AS empty_) '
            'WHERE empty_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_member_sql_keyword_empty',
        'fhir_path_expression': 'bar.bats.struct.empty()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(empty_) '
            'FROM (SELECT CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END '
            'AS empty_ FROM (SELECT bats_element_.struct '
            'FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS '
            'index_bats_element_, bats_element_) '
            'WHERE `struct` IS NOT NULL) WHERE empty_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_member_exists_not',
        'fhir_path_expression': 'bar.exists().not()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(not_) FROM (SELECT NOT( bar IS NOT NULL) AS'
            ' not_) WHERE not_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_nested_member_exists_not',
        'fhir_path_expression': 'bar.bats.exists().not()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(not_) FROM (SELECT NOT( CASE WHEN COUNT(*) ='
            ' 0 THEN FALSE ELSE TRUE END) AS not_ FROM (SELECT bats_element_'
            ' FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS'
            ' index_bats_element_, bats_element_) WHERE bats_element_ IS NOT'
            ' NULL) WHERE not_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_member_sql_keyword_exists_not',
        'fhir_path_expression': 'bar.bats.struct.exists().not()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(not_) FROM (SELECT NOT( CASE WHEN COUNT(*) ='
            ' 0 THEN FALSE ELSE TRUE END) AS not_ FROM (SELECT'
            ' bats_element_.struct FROM (SELECT bar) LATERAL VIEW'
            ' POSEXPLODE(bar.bats) AS index_bats_element_, bats_element_) WHERE'
            ' `struct` IS NOT NULL) WHERE not_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_logic_on_exists',
        'fhir_path_expression': (
            '(bar.bats.struct.value.exists() and'
            ' bar.bats.struct.anotherValue.exists()).not()'
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(not_) FROM (SELECT NOT( (SELECT CASE WHEN'
            ' COUNT(*) = 0 THEN FALSE ELSE TRUE END AS exists_ FROM (SELECT'
            ' bats_element_.struct.value FROM (SELECT bar) LATERAL VIEW'
            ' POSEXPLODE(bar.bats) AS index_bats_element_, bats_element_) WHERE'
            ' value IS NOT NULL) AND (SELECT CASE WHEN COUNT(*) = 0 THEN FALSE'
            ' ELSE TRUE END AS exists_ FROM (SELECT'
            ' bats_element_.struct.anotherValue FROM (SELECT bar) LATERAL VIEW'
            ' POSEXPLODE(bar.bats) AS index_bats_element_, bats_element_) WHERE'
            ' anotherValue IS NOT NULL)) AS not_) WHERE not_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_first',
        'fhir_path_expression': 'bar.bats.first()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bats_element_) FROM (SELECT bats_element_'
            ' FROM (SELECT FIRST(bats_element_) AS bats_element_ FROM (SELECT'
            ' bats_element_ FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats)'
            ' AS index_bats_element_, bats_element_))) WHERE bats_element_ IS'
            ' NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_first_on_non_collection',
        'fhir_path_expression': 'bar.first()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bar) FROM (SELECT bar FROM (SELECT FIRST(bar)'
            ' AS bar FROM (SELECT bar))) WHERE bar IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_member_has_value',
        'fhir_path_expression': 'bar.hasValue()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(has_value_) '
            'FROM (SELECT bar IS NOT NULL AS has_value_) '
            'WHERE has_value_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_member_sql_keyword_has_value',
        'fhir_path_expression': 'bar.bats.struct.hasValue()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(has_value_) '
            'FROM (SELECT bats_element_.struct IS NOT NULL AS has_value_ '
            'FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) '
            'AS index_bats_element_, bats_element_) '
            'WHERE has_value_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deep_member_matches',
        'fhir_path_expression': "bar.bats.struct.value.matches('foo_regex')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(matches_) '
            "FROM (SELECT REGEXP( bats_element_.struct.value, 'foo_regex') "
            'AS matches_ FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) '
            'AS index_bats_element_, bats_element_) '
            'WHERE matches_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deep_member_matches_no_pattern',
        'fhir_path_expression': 'bar.bats.struct.value.matches()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(matches_) '
            'FROM (SELECT NULL AS matches_) '
            'WHERE matches_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_array_scalar_member_exists',
        'fhir_path_expression': 'bar.exists()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(exists_) '
            'FROM (SELECT bar IS NOT NULL AS exists_) '
            'WHERE exists_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_member_sql_keyword_exists',
        'fhir_path_expression': 'bar.bats.exists()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(exists_) '
            'FROM (SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END '
            'AS exists_ FROM (SELECT bats_element_ '
            'FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS '
            'index_bats_element_, bats_element_) '
            'WHERE bats_element_ IS NOT NULL) '
            'WHERE exists_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_deepest_nested_member_sql_keyword_struct_exists'
        ),
        'fhir_path_expression': 'bar.bats.struct.exists()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(exists_) '
            'FROM (SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END '
            'AS exists_ FROM (SELECT bats_element_.struct '
            'FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS '
            'index_bats_element_, bats_element_) '
            'WHERE `struct` IS NOT NULL) '
            'WHERE exists_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_deepest_nested_member_fhir_path_keyword_exists',
        'fhir_path_expression': 'bar.bats.`div`.exists()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(exists_) '
            'FROM (SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END '
            'AS exists_ FROM (SELECT bats_element_.div '
            'FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS '
            'index_bats_element_, bats_element_) '
            'WHERE div IS NOT NULL) '
            'WHERE exists_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_all_and_identifier',
        'fhir_path_expression': "bat.struct.all(anotherValue = '')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(all_) FROM (SELECT IFNULL( BOOL_AND( IFNULL('
            " (SELECT (`struct`.anotherValue = '') AS all_), FALSE)), TRUE) AS"
            ' all_ FROM (SELECT bat.struct)) WHERE all_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_all_and_repeated_subfield_primitive_only_comparison'
        ),
        'fhir_path_expression': "bar.bats.struct.all( value = '' )",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(all_) FROM (SELECT IFNULL( BOOL_AND( IFNULL('
            ' (SELECT (SELECT NOT EXISTS( ARRAY_EXCEPT((SELECT ARRAY(value)),'
            " (SELECT ARRAY(''))), x -> x IS NOT NULL) AS eq_ FROM (SELECT"
            ' struct_element_.value)) AS all_), FALSE)), TRUE) AS all_ FROM'
            ' (SELECT bats_element_.struct FROM (SELECT bar) LATERAL VIEW'
            ' POSEXPLODE(bar.bats) AS index_bats_element_, bats_element_))'
            ' WHERE all_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_all_and_repeated_operand_uses_exist_function',
        'fhir_path_expression': 'bar.all( bats.exists() )',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(all_) FROM (SELECT IFNULL( BOOL_AND( IFNULL('
            ' (SELECT (SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END'
            ' AS exists_ FROM (SELECT bats_element_ FROM (SELECT bar) LATERAL'
            ' VIEW POSEXPLODE(bar.bats) AS index_bats_element_, bats_element_)'
            ' WHERE bats_element_ IS NOT NULL) AS all_), FALSE)), TRUE) AS all_'
            ' FROM (SELECT bar)) WHERE all_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_all_and_repeated_parent',
        'fhir_path_expression': 'bar.bats.all(struct.exists() )',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(all_) FROM (SELECT IFNULL( BOOL_AND( IFNULL('
            ' (SELECT (SELECT CASE WHEN COUNT(*) = 0 THEN FALSE ELSE TRUE END'
            ' AS exists_ FROM (SELECT bats_element_.struct) WHERE `struct` IS'
            ' NOT NULL) AS all_), FALSE)), TRUE) AS all_ FROM (SELECT bar)'
            ' LATERAL VIEW POSEXPLODE(bar.bats) AS index_bats_element_,'
            ' bats_element_) WHERE all_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_all_with_no_operand',
        'fhir_path_expression': 'all(bar.exists())',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(all_) FROM (SELECT TRUE AS all_) WHERE all_'
            ' IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_array_matches_all',
        'fhir_path_expression': "inline.numbers.all($this.matches('regex'))",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(all_) FROM (SELECT IFNULL( BOOL_AND( IFNULL('
            " (SELECT REGEXP( numbers_element_, 'regex') AS all_), FALSE)),"
            ' TRUE) AS all_ FROM (SELECT inline) LATERAL VIEW'
            ' POSEXPLODE(inline.numbers) AS index_numbers_element_,'
            ' numbers_element_) WHERE all_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_code_member_of',
        'fhir_path_expression': (
            "codeFlavor.code.memberOf('http://value.set/id')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT ISNOTNULL(memberof_)'
            ' AS memberof_ FROM (SELECT 1 AS memberof_ FROM `VALUESET_VIEW` vs'
            " WHERE vs.valueseturi='http://value.set/id'  AND"
            ' vs.code=codeFlavor.code) ) WHERE memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_code_member_of_value_set_version',
        'fhir_path_expression': (
            "codeFlavor.code.memberOf('http://value.set/id|1.0')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT ISNOTNULL(memberof_)'
            ' AS memberof_ FROM (SELECT 1 AS memberof_ FROM `VALUESET_VIEW` vs'
            " WHERE vs.valueseturi='http://value.set/id' AND"
            " vs.valuesetversion='1.0'  AND vs.code=codeFlavor.code) ) WHERE"
            ' memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_coding_member_of',
        'fhir_path_expression': (
            "codeFlavor.coding.memberOf('http://value.set/id')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT ISNOTNULL(memberof_)'
            ' AS memberof_ FROM (SELECT 1 AS memberof_ FROM `VALUESET_VIEW` vs'
            " WHERE vs.valueseturi='http://value.set/id'  AND"
            ' vs.system=codeFlavor.coding.system AND'
            ' vs.code=codeFlavor.coding.code)) WHERE memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_coding_member_of_value_set_version',
        'fhir_path_expression': (
            "codeFlavor.coding.memberOf('http://value.set/id|1.0')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT ISNOTNULL(memberof_)'
            ' AS memberof_ FROM (SELECT 1 AS memberof_ FROM `VALUESET_VIEW` vs'
            " WHERE vs.valueseturi='http://value.set/id' AND"
            " vs.valuesetversion='1.0'  AND vs.system=codeFlavor.coding.system"
            ' AND vs.code=codeFlavor.coding.code)) WHERE memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_codeable_concept_member_of',
        'fhir_path_expression': (
            "codeFlavor.codeableConcept.memberOf('http://value.set/id')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT ISNOTNULL(memberof_)'
            ' AS memberof_ FROM (SELECT 1 AS memberof_ FROM (SELECT'
            ' EXPLODE(codeableConcept.coding) AS codings FROM (SELECT'
            ' codeFlavor.codeableConcept) ) INNER JOIN `VALUESET_VIEW` vs ON'
            " vs.valueseturi='http://value.set/id'  AND"
            ' vs.system=codings.system AND vs.code=codings.code)) WHERE'
            ' memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_of_type_codeable_concept_member_of',
        'fhir_path_expression': "codeFlavor.ofType('codeableConcept').memberOf('http://value.set/id')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT ISNOTNULL(memberof_)'
            ' AS memberof_ FROM (SELECT 1 AS memberof_ FROM (SELECT'
            ' EXPLODE(ofType_.coding) AS codings FROM (SELECT'
            ' codeFlavor.codeableConcept AS ofType_) ) INNER JOIN'
            " `VALUESET_VIEW` vs ON vs.valueseturi='http://value.set/id'  AND"
            ' vs.system=codings.system AND vs.code=codings.code)) WHERE'
            ' memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_where_and_no_operand',
        'fhir_path_expression': 'where(true)',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(where_clause_) '
            'FROM (SELECT NULL AS where_clause_) '
            'WHERE where_clause_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_where',
        'fhir_path_expression': "bat.struct.where(value='')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(`struct`) '
            'FROM (SELECT bat.struct '
            'FROM (SELECT bat.struct.*) '
            "WHERE (`struct`.value = '')) "
            'WHERE `struct` IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_where_and_empty',
        'fhir_path_expression': "bat.struct.where(value='').empty())",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(empty_) '
            'FROM (SELECT bat.struct IS NULL AS empty_ '
            'FROM (SELECT bat.struct.*) '
            "WHERE (`struct`.value = '')) "
            'WHERE empty_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_chained_where',
        'fhir_path_expression': (
            "bat.struct.where(value='').where(anotherValue='')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(`struct`) '
            'FROM (SELECT bat.struct '
            'FROM (SELECT bat.struct.*) '
            "WHERE (`struct`.value = '') "
            "AND (`struct`.anotherValue = '')) "
            'WHERE `struct` IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_complex_where',
        'fhir_path_expression': (
            "bat.struct.where(value='' and anotherValue=''))"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(`struct`) '
            'FROM (SELECT bat.struct '
            'FROM (SELECT bat.struct.*) '
            "WHERE (`struct`.value = '') "
            "AND (`struct`.anotherValue = '')) "
            'WHERE `struct` IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_where_and_repeated',
        'fhir_path_expression': 'bar.bats.where( struct.exists() )',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bats_element_) FROM (SELECT bats_element_'
            ' FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) AS'
            ' index_bats_element_, bats_element_ WHERE (SELECT CASE WHEN'
            ' COUNT(*) = 0 THEN FALSE ELSE TRUE END AS exists_ FROM (SELECT'
            ' bats_element_.struct) WHERE `struct` IS NOT NULL)) WHERE'
            ' bats_element_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_retrieve_nested_field',
        'fhir_path_expression': "bat.struct.where(value='').anotherValue",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(anotherValue) '
            'FROM (SELECT bat.struct.anotherValue '
            'FROM (SELECT bat.struct.*) '
            "WHERE (`struct`.value = '')) "
            'WHERE anotherValue IS NOT NULL)'
        ),
    },
    {
        'testcase_name': (
            '_with_multiple_where_clause_and_retrieve_nested_field'
        ),
        'fhir_path_expression': (
            "bat.struct.where(value='').where(anotherValue='').anotherValue"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(anotherValue) '
            'FROM (SELECT bat.struct.anotherValue '
            'FROM (SELECT bat.struct.*) '
            "WHERE (`struct`.value = '') AND (`struct`.anotherValue = '')) "
            'WHERE anotherValue IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_retrieve_nested_field_exists',
        'fhir_path_expression': (
            "bat.struct.where(value='').anotherValue.exists()"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(exists_) '
            'FROM (SELECT CASE WHEN COUNT(*) = 0 '
            'THEN FALSE ELSE TRUE END AS exists_ '
            'FROM (SELECT bat.struct.anotherValue '
            "FROM (SELECT bat.struct.*) WHERE (`struct`.value = '')) "
            'WHERE anotherValue IS NOT NULL) '
            'WHERE exists_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_array_with_message_choice_and_where',
        'fhir_path_expression': (
            "multipleChoiceExample.ofType('CodeableConcept').coding.where(system"
            " = 'test')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(coding_element_) FROM (SELECT coding_element_'
            ' FROM (SELECT multipleChoiceExample_element_.CodeableConcept AS'
            ' ofType_ FROM (SELECT EXPLODE(multipleChoiceExample_element_) AS'
            ' multipleChoiceExample_element_ FROM (SELECT multipleChoiceExample'
            ' AS multipleChoiceExample_element_))) LATERAL VIEW'
            ' POSEXPLODE(ofType_.coding) AS index_coding_element_,'
            ' coding_element_ WHERE (SELECT NOT EXISTS( ARRAY_EXCEPT((SELECT'
            " ARRAY(system)), (SELECT ARRAY('test'))), x -> x IS NOT NULL) AS"
            ' eq_ FROM (SELECT coding_element_.system))) WHERE coding_element_'
            ' IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_scalar_with_repeated_message_choice_and_where',
        'fhir_path_expression': (
            "choiceExample.ofType('CodeableConcept').coding.where(system ="
            " 'test')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(coding_element_) FROM (SELECT'
            ' coding_element_ FROM (SELECT choiceExample.CodeableConcept AS'
            ' ofType_) LATERAL VIEW POSEXPLODE(ofType_.coding) AS'
            ' index_coding_element_, coding_element_ WHERE (SELECT NOT EXISTS('
            " ARRAY_EXCEPT((SELECT ARRAY(system)), (SELECT ARRAY('test'))), x"
            ' -> x IS NOT NULL) AS eq_ FROM (SELECT coding_element_.system)))'
            ' WHERE coding_element_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_any_true',
        'fhir_path_expression': 'boolList.anyTrue()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(_anyTrue) FROM (SELECT MAX('
            ' boolList_element_) AS _anyTrue FROM (SELECT boolList_element_'
            ' FROM (SELECT EXPLODE(boolList_element_) AS boolList_element_ FROM'
            ' (SELECT boolList AS boolList_element_)))) WHERE _anyTrue IS NOT'
            ' NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_NOOPERAND_RAISES_ERROR = [
    {'testcase_name': '_with_count', 'fhir_path_expression': 'count()'},
    {'testcase_name': '_with_empty', 'fhir_path_expression': 'empty()'},
    {'testcase_name': '_with_exists', 'fhir_path_expression': 'exists()'},
    {'testcase_name': '_with_first', 'fhir_path_expression': 'first()'},
    {'testcase_name': '_with_has_value', 'fhir_path_expression': 'hasValue()'},
    {'testcase_name': '_with_matches', 'fhir_path_expression': 'matches()'},
    {'testcase_name': '_with_of_type', 'fhir_path_expression': 'ofType()'},
    {'testcase_name': '_with_id_for', 'fhir_path_expression': 'idFor()'},
    {'testcase_name': '_with_all', 'fhir_path_expression': 'all()'},
    {'testcase_name': '_with_member_of', 'fhir_path_expression': 'memberOf()'},
    {'testcase_name': '_with_not', 'fhir_path_expression': 'not()'},
    {'testcase_name': '_with_any_true', 'fhir_path_expression': 'anyTrue()'},
]

_WITH_FHIRPATH_V2_FHIRPATH_FUNCTION_INVOCATION_RAISES_VALUE_ERROR = [
    {
        'testcase_name': '_with_array_scalar_member_exists',
        'fhir_path_expression': 'bar.exists(struct)',
    },
    {
        'testcase_name': '_with_where_function_and_no_criteria',
        'fhir_path_expression': 'bat.struct.where()',
    },
    {
        'testcase_name': '_with_where_function_and_non_bool_criteria',
        'fhir_path_expression': 'bat.struct.where(value)',
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_FUNCTION_INVOCATION_RAISES_NOT_IMPLEMENTED_ERROR = [
    {
        'testcase_name': '_with_where_and_repeated_and_exists',
        'fhir_path_expression': 'bar.bats.where( struct = struct ).exists()',
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_MEMBER_FUNCTION_UNION_FUNCTION_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_integer_union',
        'fhir_path_expression': '3 | 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(union_) '
            'FROM (SELECT lhs_.literal_ AS union_ '
            'FROM (SELECT 3 AS literal_) AS lhs_ '
            'UNION DISTINCT '
            'SELECT rhs_.literal_ AS union_ '
            'FROM (SELECT 4 AS literal_) AS rhs_) '
            'WHERE union_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_string_union',
        'fhir_path_expression': "'Foo' | 'Bar'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(union_) '
            'FROM (SELECT lhs_.literal_ AS union_ '
            "FROM (SELECT 'Foo' AS literal_) AS lhs_ "
            'UNION DISTINCT '
            'SELECT rhs_.literal_ AS union_ '
            "FROM (SELECT 'Bar' AS literal_) AS rhs_) "
            'WHERE union_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_string_nested_union',
        'fhir_path_expression': "('Foo' | 'Bar') | ('Bats')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(union_) '
            'FROM (SELECT lhs_.union_ '
            'FROM (SELECT lhs_.literal_ AS union_ '
            "FROM (SELECT 'Foo' AS literal_) AS lhs_ "
            'UNION DISTINCT '
            'SELECT rhs_.literal_ AS union_ '
            "FROM (SELECT 'Bar' AS literal_) AS rhs_) AS lhs_ "
            'UNION DISTINCT '
            'SELECT rhs_.literal_ AS union_ '
            "FROM (SELECT 'Bats' AS literal_) AS rhs_) "
            'WHERE union_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_MEMBER_OF_VECTOR_EXPRESSIONS_RAISES_ERROR = [
    {
        'testcase_name': '_with_vector_code_member_of',
        'fhir_path_expression': (
            "codeFlavors.code.memberOf('http://value.set/id')"
        ),
    },
    {
        'testcase_name': '_with_vector_codeable_concept_member_of',
        'fhir_path_expression': (
            "codeFlavors.codeableConcept.memberOf('http://value.set/id')"
        ),
    },
    {
        'testcase_name': '_with_vector_coding_member_of',
        'fhir_path_expression': (
            "codeFlavors.coding.memberOf('http://value.set/id')"
        ),
    },
    {
        'testcase_name': '_with_vector_of_type_codeable_concept_member_of',
        'fhir_path_expression': "codeFlavors.ofType('codeableConcept').memberOf('http://value.set/id')",
    },
]

_WITH_FHIRPATH_V2_MEMBER_OF_AGAINST_LOCAL_VALUESET_DEFINITIONS_SUCCEEDS_CASES = [
    {
        'testcase_name': '_with_scalar_code_member_of',
        'fhir_path_expression': (
            "codeFlavor.code.memberOf('http://value.set/1')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT (codeFlavor.code IS'
            ' NULL) OR (codeFlavor.code IN ("code_1", "code_2")) AS memberof_)'
            ' WHERE memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_code_member_of_another_value_set',
        'fhir_path_expression': (
            "codeFlavor.code.memberOf('http://value.set/2')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT (codeFlavor.code IS'
            ' NULL) OR (codeFlavor.code IN ("code_3", "code_4", "code_5")) AS'
            ' memberof_) WHERE memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_vector_code_member_of',
        'fhir_path_expression': (
            "codeFlavors.code.memberOf('http://value.set/1')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT'
            ' (codeFlavors_element_.code IS NULL) OR (codeFlavors_element_.code'
            ' IN ("code_1", "code_2")) AS memberof_ FROM (SELECT'
            ' EXPLODE(codeFlavors_element_) AS codeFlavors_element_ FROM'
            ' (SELECT codeFlavors AS codeFlavors_element_))) WHERE memberof_ IS'
            ' NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_coding_member_of',
        'fhir_path_expression': (
            "codeFlavor.coding.memberOf('http://value.set/2')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT (codeFlavor.coding IS'
            ' NULL) OR (((codeFlavor.coding.system = "system_3") AND'
            ' (codeFlavor.coding.code IN ("code_3", "code_4"))) OR'
            ' ((codeFlavor.coding.system = "system_5") AND'
            ' (codeFlavor.coding.code IN ("code_5")))) AS memberof_) WHERE'
            ' memberof_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_with_scalar_codeable_concept_member_of',
        'fhir_path_expression': (
            "codeFlavor.codeableConcept.memberOf('http://value.set/2')"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(memberof_) FROM (SELECT'
            ' (codeFlavor.codeableConcept.coding IS NULL) OR EXISTS( (SELECT 1'
            ' FROM EXPLODE(codeFlavor.codeableConcept.coding) WHERE'
            ' ((system = "system_3") AND (code IN ("code_3", "code_4"))) OR'
            ' ((system = "system_5") AND (code IN ("code_5")))), x -> x IS NOT'
            ' NULL) AS memberof_) WHERE memberof_ IS NOT NULL)'
        ),
    },
]


class FhirPathSparkSqlEncoderTest(
    fhir_path_test_base.FhirPathTestBase, parameterized.TestCase
):
  """Unit tests for `fhir_path.FhirPathSparkSqlEncoder`."""

  def assertEvaluationNodeSqlCorrect(
      self,
      structdef_name: str,
      fhir_path_expression: str,
      expected_sql_expression: str,
      select_scalars_as_array: bool = True,
      use_resource_alias: bool = False,
      value_set_codes_definitions: Optional[
          fhir_package.FhirPackageManager
      ] = None,
  ) -> None:
    builder = self.create_builder_from_str(structdef_name, fhir_path_expression)

    actual_sql_expression = _spark_interpreter.SparkSqlInterpreter(
        value_set_codes_table='VALUESET_VIEW',
        value_set_codes_definitions=value_set_codes_definitions,
    ).encode(
        builder,
        select_scalars_as_array=select_scalars_as_array,
        use_resource_alias=use_resource_alias,
    )
    self.assertEqual(
        actual_sql_expression.replace('\n', ' '), expected_sql_expression
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_DATETIME_LITERAL_SUCCEEDS_CASES
  )
  def test_encode_with_fhir_path_v2_date_time_literal_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        'Foo', fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_ARITHMETIC_SUCCEEDS_CASES)
  def test_encode_with_fhir_path_v2_literal_arithmetic_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_INDEXER_SUCCEEDS_CASES)
  def test_encode_with_fhir_path_v2_literal_indexer_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_BOOLEAN_SUCCEEDS_CASES)
  def test_encode_with_fhir_path_v2_literal_boolean_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_COMPARISON_SUCCEEDS_CASES)
  def test_encode_with_fhir_path_v2_literal_comparison_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_POLARITY_SUCCEEDS_CASES)
  def test_encode_with_fhir_path_v2_literal_polarity_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_EQUALITY_SUCCEEDS_CASES)
  def test_encode_with_fhir_path_v2_literal_equality_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_MEMBERSHIP_SUCCEEDS_CASES)
  def test_encode_with_fhir_path_v2_literal_membership_relation_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_MEMBER_ACCESS_SUCCEEDS_CASES
  )
  def test_encode_with_fhir_path_v2_member_access_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_OFTYPE_FUNCTION_SUCCEEDS_CASES
  )
  def test_encode_with_fhir_path_v2_of_type_invocation_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_FUNCTION_INVOCATION_SUCCEEDS_CASES
  )
  def test_encode_with_fhir_path_v2_function_invocation_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_MEMBER_OF_AGAINST_LOCAL_VALUESET_DEFINITIONS_SUCCEEDS_CASES
  )
  def test_encode_with_fhir_path_v2_member_function_against_local_value_set_definitions_succeeds(
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

    self.assertEvaluationNodeSqlCorrect(
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
      _WITH_FHIRPATH_V2_FHIRPATH_NOOPERAND_RAISES_ERROR
  )
  def test_encode_with_fhir_path_function_no_operand_raises_error(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(ValueError):
      builder = self.create_builder_from_str('Foo', fhir_path_expression)
      _spark_interpreter.SparkSqlInterpreter().encode(builder)

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_FUNCTION_INVOCATION_RAISES_VALUE_ERROR
  )
  def test_encode_with_fhir_path_function_invocation_raises_value_error(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(ValueError):
      self.create_builder_from_str('Foo', fhir_path_expression)

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_FUNCTION_INVOCATION_RAISES_NOT_IMPLEMENTED_ERROR
  )
  def test_encode_with_fhir_path_function_invocation_raises_not_implemented_error(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(NotImplementedError):
      builder = self.create_builder_from_str('Foo', fhir_path_expression)

      _spark_interpreter.SparkSqlInterpreter().encode(builder)

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_MEMBER_OF_VECTOR_EXPRESSIONS_RAISES_ERROR
  )
  def test_encode_with_fhir_path_v2_member_of_function_with_vector_expression_raises_error(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(NotImplementedError):
      builder = self.create_builder_from_str('Foo', fhir_path_expression)
      _spark_interpreter.SparkSqlInterpreter(
          value_set_codes_table='VALUESET_VIEW'
      ).encode(builder)

  def test_encode_with_fhir_path_v2_select_scalars_as_array_false_for_literal_succeeds(
      self,
  ):
    fhir_path_expression = 'true'
    expected_sql_expression = '(SELECT TRUE AS literal_)'
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=False,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_MEMBER_FUNCTION_UNION_FUNCTION_SUCCEEDS_CASES
  )
  def test_encode_with_fhir_path_member_v2_literal_union_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
    )

  def test_encode_with_fhir_path_member_v2_literal_root_succeeds(self):
    expected_sql_expression = (
        '(SELECT COLLECT_LIST(bar) FROM (SELECT Foo.bar) WHERE bar IS NOT NULL)'
    )
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression='Foo.bar',
        expected_sql_expression=expected_sql_expression,
        use_resource_alias=True,
    )

  def test_encode_with_fhir_path_member_v2_id_for_succeeds(self):
    self.assertEvaluationNodeSqlCorrect(
        structdef_name='Foo',
        fhir_path_expression="bar.idFor('Bats')",
        expected_sql_expression=(
            '(SELECT COLLECT_LIST(idFor_) FROM (SELECT bar.batsId AS'
            ' idFor_) WHERE idFor_ IS NOT NULL)'
        ),
    )


if __name__ == '__main__':
  absltest.main()

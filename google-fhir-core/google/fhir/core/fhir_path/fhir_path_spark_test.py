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

from google.protobuf import message
from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import _spark_interpreter
from google.fhir.core.fhir_path import fhir_path_test_base


_WITH_FHIRPATH_V2_DATETIME_LITERAL_SUCCEEDS_CASES = [
    {
        'testcase_name': '_withNull',
        'fhir_path_expression': '{ }',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT NULL AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withBooleanTrue',
        'fhir_path_expression': 'true',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT TRUE AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withBooleanFalse',
        'fhir_path_expression': 'false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT FALSE AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withString',
        'fhir_path_expression': "'Foo'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT 'Foo' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withNumberDecimal',
        'fhir_path_expression': '3.14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT 3.14 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withNumberLargeDecimal',
        # 32 decimal places
        'fhir_path_expression': '3.14141414141414141414141414141414',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT 3.14141414141414141414141414141414 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withNumberInteger',
        'fhir_path_expression': '314',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT 314 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateYear',
        'fhir_path_expression': '@1970',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('1970-01-01' AS TIMESTAMP) AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateYearMonth',
        'fhir_path_expression': '@1970-02',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('1970-02-01' AS TIMESTAMP) AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateYearMonthDay',
        'fhir_path_expression': '@1970-02-03',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('1970-02-03' AS TIMESTAMP) AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateTimeYearMonthDayHours',
        'fhir_path_expression': '@2015-02-04T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP) "
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateTimeYearMonthDayHoursMinutes',
        'fhir_path_expression': '@2015-02-04T14:34',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:00+00:00' AS TIMESTAMP) "
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateTimeYearMonthDayHoursMinutesSeconds',
        'fhir_path_expression': '@2015-02-04T14:34:28',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) "
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateTimeYearMonthDayHoursMinutesSecondsMilli',
        'fhir_path_expression': '@2015-02-04T14:34:28.123',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:28.123000+00:00' "
            'AS TIMESTAMP) '
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateTimeYearMonthDayHoursMinutesSecondsMilliTz',
        'fhir_path_expression': '@2015-02-04T14:34:28.123+09:00',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT CAST('2015-02-04T14:34:28.123000+09:00' "
            'AS TIMESTAMP) '
            'AS literal_) WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withTimeHours',
        'fhir_path_expression': '@T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withTimeHoursMinutes',
        'fhir_path_expression': '@T14:34',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14:34' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withTimeHoursMinutesSeconds',
        'fhir_path_expression': '@T14:34:28',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14:34:28' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withTimeHoursMinutesSecondsMilli',
        'fhir_path_expression': '@T14:34:28.123',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            "FROM (SELECT '14:34:28.123' AS literal_) "
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withQuantity',
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
        'testcase_name': '_withIntegerAddition',
        'fhir_path_expression': '1 + 2',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (1 + 2) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalAddition',
        'fhir_path_expression': '3.14 + 1.681',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (3.14 + 1.681) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerDivision',
        'fhir_path_expression': '3 / 2',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (3 / 2) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalDivision',
        'fhir_path_expression': '3.14 / 1.681',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (3.14 / 1.681) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerModularArithmetic',
        'fhir_path_expression': '2 mod 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT MOD(2, 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerMultiplication',
        'fhir_path_expression': '2 * 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2 * 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalMultiplication',
        'fhir_path_expression': '2.124 * 5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2.124 * 5.72) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerSubtraction',
        'fhir_path_expression': '2 - 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2 - 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalSubtraction',
        'fhir_path_expression': '2.124 - 5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (2.124 - 5.72) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerTrunctatedDivision',
        'fhir_path_expression': '2 div 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT DIV(2, 5) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalTruncatedDivision',
        'fhir_path_expression': '2.124 div 5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT DIV(2.124, 5.72) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerAdditionAndMultiplication',
        'fhir_path_expression': '(1 + 2) * 3',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT ((1 + 2) * 3) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerSubtractionAndDivision',
        'fhir_path_expression': '(21 - 6) / 3',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT ((21 - 6) / 3) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerAdditionAndModularArithmetic',
        'fhir_path_expression': '21 + 6 mod 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (21 + MOD(6, 5)) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerAdditionAndTruncatedDivision',
        'fhir_path_expression': '21 + 6 div 5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (21 + DIV(6, 5)) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withStringConcatenationAmpersand',
        'fhir_path_expression': "'foo' & 'bar'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            "FROM (SELECT CONCAT('foo', 'bar') AS arith_) "
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withStringConcatenationPlus',
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
        'testcase_name': '_withIntegerIndexer',
        'fhir_path_expression': '7[0]',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
            'element_at(COLLECT_LIST(literal_),0 + 1) AS indexed_literal_ FROM '
            '(SELECT 7 AS literal_)) WHERE indexed_literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerIndexerArithmeticIndex',
        'fhir_path_expression': '7[0 + 1]',  # Out-of-bounds, empty table
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
            'element_at(COLLECT_LIST(literal_),(0 + 1) + 1) AS '
            'indexed_literal_ FROM (SELECT 7 AS literal_)) WHERE '
            'indexed_literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withStringIndexer',
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
        'testcase_name': '_withBooleanAnd',
        'fhir_path_expression': 'true and false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT TRUE AND FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withBooleanOr',
        'fhir_path_expression': 'true or false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT TRUE OR FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withBooleanXor',
        'fhir_path_expression': 'true xor false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT TRUE <> FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withBooleanImplies',
        'fhir_path_expression': 'true implies false',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT NOT TRUE OR FALSE AS '
            'logic_) WHERE logic_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withBooleanRelationBetweenStringInteger',
        'fhir_path_expression': "3 and 'true'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(logic_) FROM (SELECT (3 IS NOT NULL) AND '
            "('true' IS NOT NULL) AS logic_) WHERE logic_ IS NOT NULL)"
        ),
    },
]

_WITH_FHIRPATH_V2_COMPARISON_SUCCEEDS_CASES = [
    {
        'testcase_name': '_withIntegerGreaterThan',
        'fhir_path_expression': '4 > 3',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 4 > 3 AS '
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerLessThan',
        'fhir_path_expression': '3 < 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 3 < 4 AS '
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerLessThanOrEqualTo',
        'fhir_path_expression': '3 <= 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 3 <= 4 AS '
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withFloatLessThanOrEqualTo',
        'fhir_path_expression': '3.14159 <= 4.00000',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) FROM (SELECT 3.14159 <= 4.00000'
            ' AS comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withStringGreaterThan',
        'fhir_path_expression': " 'a' > 'b'",
        'expected_sql_expression': (
            "(SELECT COLLECT_LIST(comparison_) FROM (SELECT 'a' > 'b' AS "
            'comparison_) WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateLessThan',
        'fhir_path_expression': 'dateField < @2000-01-01',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(comparison_) '
            'FROM (SELECT CAST(dateField AS TIMESTAMP) '
            "< CAST('2000-01-01' AS TIMESTAMP) AS comparison_) "
            'WHERE comparison_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_dateComparedWithTimestamp',
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
        'testcase_name': '_withIntegerPositivePolarity',
        'fhir_path_expression': '+5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT +5 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalPositivePolarity',
        'fhir_path_expression': '+5.72',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT +5.72 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerNegativePolarity',
        'fhir_path_expression': '-5',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT -5 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalNegativePolarity',
        'fhir_path_expression': '-5.1349',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(literal_) '
            'FROM (SELECT -5.1349 AS literal_) '
            'WHERE literal_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerPositivePolarityAndAddition',
        'fhir_path_expression': '+5 + 10',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (+5 + 10) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerNegativePolarityAndAddition',
        'fhir_path_expression': '-5 + 10',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT (-5 + 10) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerNegativePolarityAndModularArithmetic',
        'fhir_path_expression': '-5 mod 6',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(arith_) '
            'FROM (SELECT MOD(-5, 6) AS arith_) '
            'WHERE arith_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerPositivePolarityAndModularArithmetic',
        'fhir_path_expression': '+(7 mod 6)',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(pol_) '
            'FROM (SELECT +MOD(7, 6) AS pol_) '
            'WHERE pol_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalNegativePolarityAndMultiplication',
        'fhir_path_expression': '-(3.79 * 2.124)',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(pol_) '
            'FROM (SELECT -(3.79 * 2.124) AS pol_) '
            'WHERE pol_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDecimalNegativePolarityAndDivision',
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
        'testcase_name': '_withIntegerIn',
        'fhir_path_expression': '3 in 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(mem_) '
            'FROM (SELECT (3) IN (4) AS mem_) '
            'WHERE mem_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerContains',
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
        'testcase_name': '_withIntegerEqual',
        'fhir_path_expression': '3 = 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 = 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerEquivalent',
        'fhir_path_expression': '3 ~ 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 = 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateTimeEqual',
        'fhir_path_expression': '@2015-02-04T14:34:28 = @2015-02-04T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM '
            "(SELECT (CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) "
            "= CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP)) AS eq_) "
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDateTimeEquivalent',
        'fhir_path_expression': '@2015-02-04T14:34:28 ~ @2015-02-04T14',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM '
            "(SELECT (CAST('2015-02-04T14:34:28+00:00' AS TIMESTAMP) "
            "= CAST('2015-02-04T14:00:00+00:00' AS TIMESTAMP)) AS eq_) "
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerNotEqualTo',
        'fhir_path_expression': '3 != 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 != 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withIntegerNotEquivalentTo',
        'fhir_path_expression': '3 !~ 4',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT (3 != 4) AS eq_) '
            'WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withScalarComplexComparisonRightSideScalar',
        # TODO(b/262544393): Change to "bar.bats.struct.value = ('abc' | '123')"
        #                    when visit_union is implemented.
        'fhir_path_expression': "bar.bats.struct.value = '123'",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT NOT EXISTS('
            " ARRAY_EXCEPT((SELECT value), (SELECT ARRAY('123'))),"
            ' x -> x IS NOT NULL) AS eq_ '
            'FROM (SELECT COLLECT_LIST(*) AS value '
            'FROM (SELECT bats_element_.struct.value '
            'FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) '
            'AS index_bats_element_, bats_element_)))'
            ' WHERE eq_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withScalarComplexComparisonLeftSideScalar',
        'fhir_path_expression': " '123' = bar.bats.struct.value",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) '
            'FROM (SELECT NOT EXISTS('
            " ARRAY_EXCEPT((SELECT value), (SELECT ARRAY('123'))),"
            ' x -> x IS NOT NULL) AS eq_ '
            'FROM (SELECT COLLECT_LIST(*) AS value '
            'FROM (SELECT bats_element_.struct.value '
            'FROM (SELECT bar) LATERAL VIEW POSEXPLODE(bar.bats) '
            'AS index_bats_element_, bats_element_)))'
            ' WHERE eq_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_MEMBER_ACCESS_SUCCEEDS_CASES = [
    {
        'testcase_name': '_withSingleMemberAccess',
        'fhir_path_expression': 'bar',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bar) FROM (SELECT bar) WHERE bar IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withInlineMemberAccess',
        'fhir_path_expression': 'inline',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(inline) '
            'FROM (SELECT inline) '
            'WHERE inline IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withNestedMemberAccess',
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
        'testcase_name': '_withInlineNestedMemberAccess',
        'fhir_path_expression': 'inline.value',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(value) '
            'FROM (SELECT inline.value) '
            'WHERE value IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDeepestNestedMemberSqlKeywordAccess',
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
        'testcase_name': '_withDeepestNestedMemberFhirPathKeywordAccess',
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
        'testcase_name': '_withDeepestNestedScalarMemberFhirPathAccess',
        'fhir_path_expression': 'bat.struct.anotherStruct.anotherValue',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(anotherValue) '
            'FROM (SELECT bat.struct.anotherStruct.anotherValue) '
            'WHERE anotherValue IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withFirstElementBeingRepeatedMemberFhirPathAccess',
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
        'testcase_name': '_withChoiceNoType',
        'fhir_path_expression': 'choiceExample',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(choiceExample) FROM (SELECT choiceExample)'
            ' WHERE choiceExample IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withChoiceStringType',
        'fhir_path_expression': "choiceExample.ofType('string')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(ofType_) FROM (SELECT choiceExample.string AS'
            ' ofType_) WHERE ofType_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withChoiceIntegerType',
        'fhir_path_expression': "choiceExample.ofType('integer')",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(ofType_) FROM (SELECT choiceExample.integer'
            ' AS ofType_) WHERE ofType_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_ArrayWithChoice',
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
        'testcase_name': '_ScalarWithRepeatedMessageChoice',
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
        'testcase_name': '_ArrayWithMessageChoice',
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
        'testcase_name': '_ArrayWithMessageChoice_andIdentifier',
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
        'testcase_name': '_ArrayWithMessageChoice_andEquality',
        'fhir_path_expression': (
            "multipleChoiceExample.ofType('CodeableConcept').coding.system ="
            " 'test'"
        ),
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(eq_) FROM (SELECT NOT EXISTS('
            " ARRAY_EXCEPT((SELECT system), (SELECT ARRAY('test'))), x -> x IS"
            ' NOT NULL) AS eq_ FROM (SELECT COLLECT_LIST(*) AS system FROM'
            ' (SELECT coding_element_.system FROM (SELECT'
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
        'testcase_name': '_withMemberCount',
        'fhir_path_expression': 'bar.count()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(count_) '
            'FROM (SELECT COUNT( bar) AS count_ '
            'FROM (SELECT bar)) '
            'WHERE count_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDeepestNestedMemberSqlKeywordCount',
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
        'testcase_name': '_withMemberEmpty',
        'fhir_path_expression': 'bar.empty()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(empty_) '
            'FROM (SELECT bar IS NULL AS empty_) '
            'WHERE empty_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDeepestNestedMemberSqlKeywordEmpty',
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
        'testcase_name': '_withFirst',
        'fhir_path_expression': 'bar.bats.first()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bats_element_) '
            'FROM (SELECT FIRST(bats_element_) AS bats_element_ '
            'FROM (SELECT bats_element_ FROM (SELECT bar) '
            'LATERAL VIEW POSEXPLODE(bar.bats) AS index_bats_element_, '
            'bats_element_)) WHERE bats_element_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withFirstOnNonCollection',
        'fhir_path_expression': 'bar.first()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(bar) '
            'FROM (SELECT FIRST(bar) AS bar '
            'FROM (SELECT bar)) WHERE bar IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withMemberHasValue',
        'fhir_path_expression': 'bar.hasValue()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(has_value_) '
            'FROM (SELECT bar IS NOT NULL AS has_value_) '
            'WHERE has_value_ IS NOT NULL)'
        ),
    },
    {
        'testcase_name': '_withDeepestMemberSqlKeywordHasValue',
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
        'testcase_name': '_withDeepMemberMatches',
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
        'testcase_name': '_withDeepMemberMatchesNoPattern',
        'fhir_path_expression': 'bar.bats.struct.value.matches()',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(matches_) '
            'FROM (SELECT NULL AS matches_) '
            'WHERE matches_ IS NOT NULL)'
        ),
    },
]

_WITH_FHIRPATH_V2_FHIRPATH_NOOPERAND_RAISES_ERROR = [
    {'testcase_name': '_withCount', 'fhir_path_expression': 'count()'},
    {'testcase_name': '_withEmpty', 'fhir_path_expression': 'empty()'},
    {'testcase_name': '_withFirst', 'fhir_path_expression': 'first()'},
    {'testcase_name': '_withHasValue', 'fhir_path_expression': 'hasValue()'},
    {'testcase_name': '_withMatches', 'fhir_path_expression': 'matches()'},
    {'testcase_name': '_withOfType', 'fhir_path_expression': 'ofType()'},
]


class FhirPathSparkSqlEncoderTest(
    fhir_path_test_base.FhirPathTestBase, parameterized.TestCase
):
  """Unit tests for `fhir_path.FhirPathSparkSqlEncoder`."""

  def assertEvaluationNodeSqlCorrect(
      self,
      structdef: message.Message,
      fhir_path_expression: str,
      expected_sql_expression: str,
      select_scalars_as_array: bool = True,
  ) -> None:
    builder = self.create_builder_from_str(structdef, fhir_path_expression)

    actual_sql_expression = _spark_interpreter.SparkSqlInterpreter().encode(
        builder, select_scalars_as_array=select_scalars_as_array
    )
    self.assertEqual(
        actual_sql_expression.replace('\n', ' '), expected_sql_expression
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_DATETIME_LITERAL_SUCCEEDS_CASES
  )
  def testEncode_withFhirPathV2DateTimeLiteral_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        None, fhir_path_expression, expected_sql_expression
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_ARITHMETIC_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralArithmetic_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=None,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_INDEXER_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralIndexer_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=None,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_BOOLEAN_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralBoolean_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=None,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_COMPARISON_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralComparison_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=self.foo,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_POLARITY_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralPolarity_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=None,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )
    self.assertEvaluationNodeSqlCorrect(
        structdef=self.foo,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_EQUALITY_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralEquality_succeeds(
      self,
      fhir_path_expression: str,
      expected_sql_expression: str,
      select_scalars_as_array=True,
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=self.foo,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=select_scalars_as_array,
    )

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_MEMBERSHIP_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralMembershipRelation_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=None,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_MEMBER_ACCESS_SUCCEEDS_CASES
  )
  def testEncode_withFhirPathV2MemberAccess_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=self.foo,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_OFTYPE_FUNCTION_SUCCEEDS_CASES
  )
  def testEncode_withFhirPathV2OfTypeInvocation_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=self.foo,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_FUNCTION_INVOCATION_SUCCEEDS_CASES
  )
  def testEncode_withFhirPathV2FunctionInvocation_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str
  ):
    self.assertEvaluationNodeSqlCorrect(
        structdef=self.foo,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True,
    )

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_FHIRPATH_NOOPERAND_RAISES_ERROR
  )
  def testEncode_withFhirPathFunctionNoOperand_raisesError(
      self, fhir_path_expression: str
  ):
    with self.assertRaises(ValueError):
      builder = self.create_builder_from_str(self.foo, fhir_path_expression)
      _spark_interpreter.SparkSqlInterpreter().encode(builder)

  def testEncode_withFhirPathV2SelectScalarsAsArrayFalseForLiteral_succeeds(
      self,
  ):
    fhir_path_expression = 'true'
    expected_sql_expression = '(SELECT TRUE AS literal_)'
    self.assertEvaluationNodeSqlCorrect(
        structdef=self.foo,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=False,
    )


if __name__ == '__main__':
  absltest.main()

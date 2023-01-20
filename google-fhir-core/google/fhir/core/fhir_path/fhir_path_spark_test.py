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
from google.fhir.core.fhir_path import fhir_path_test_base


# TODO(b/262544393): add _withDateTimeEqual and _withDateTimeEquivalent
# tests when visit_equality is implemented
_WITH_FHIRPATH_V2_DATETIME_LITERAL_SUCCEEDS_CASES = [{
    'testcase_name':
        '_withNull',
    'fhir_path_expression':
        '{ }',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                'FROM (SELECT NULL AS literal_) '
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withBooleanTrue',
    'fhir_path_expression':
        'true',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                'FROM (SELECT TRUE AS literal_) '
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withBooleanFalse',
    'fhir_path_expression':
        'false',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                'FROM (SELECT FALSE AS literal_) '
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withString',
    'fhir_path_expression':
        "'Foo'",
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                "FROM (SELECT 'Foo' AS literal_) "
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withNumberDecimal',
    'fhir_path_expression':
        '3.14',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                'FROM (SELECT 3.14 AS literal_) '
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withNumberLargeDecimal',
    # 32 decimal places
    'fhir_path_expression':
        '3.14141414141414141414141414141414',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) '
         'FROM (SELECT 3.14141414141414141414141414141414 AS literal_) '
         'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withNumberInteger',
    'fhir_path_expression':
        '314',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                'FROM (SELECT 314 AS literal_) '
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDateYear',
    'fhir_path_expression':
        '@1970',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) '
         "FROM (SELECT TO_TIMESTAMP('1970-01-01', \"yyyy-MM-dd\") AS literal_) "
         'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDateYearMonth',
    'fhir_path_expression':
        '@1970-01',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) '
         "FROM (SELECT TO_TIMESTAMP('1970-01-01', \"yyyy-MM-dd\") AS literal_) "
         'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDateYearMonthDay',
    'fhir_path_expression':
        '@1970-01-01',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) '
         "FROM (SELECT TO_TIMESTAMP('1970-01-01', \"yyyy-MM-dd\") AS literal_) "
         'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDateTimeYearMonthDayHours',
    'fhir_path_expression':
        '@2015-02-04T14',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) FROM (SELECT '
         "TO_TIMESTAMP(DATE_FORMAT('2015-02-04T14:00:00+00:00', "
         "\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\")) AS literal_) WHERE literal_ IS NOT "
         'NULL)')
}, {
    'testcase_name':
        '_withDateTimeYearMonthDayHoursMinutes',
    'fhir_path_expression':
        '@2015-02-04T14:34',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) FROM (SELECT '
         "TO_TIMESTAMP(DATE_FORMAT('2015-02-04T14:34:00+00:00', "
         "\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\")) AS literal_) WHERE literal_ IS NOT "
         'NULL)')
}, {
    'testcase_name':
        '_withDateTimeYearMonthDayHoursMinutesSeconds',
    'fhir_path_expression':
        '@2015-02-04T14:34:28',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) FROM (SELECT '
         "TO_TIMESTAMP(DATE_FORMAT('2015-02-04T14:34:28+00:00', "
         "\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\")) AS literal_) WHERE literal_ IS NOT "
         'NULL)')
}, {
    'testcase_name':
        '_withDateTimeYearMonthDayHoursMinutesSecondsMilli',
    'fhir_path_expression':
        '@2015-02-04T14:34:28.123',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) FROM (SELECT '
         "TO_TIMESTAMP(DATE_FORMAT('2015-02-04T14:34:28.123000+00:00', "
         "\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\")) AS literal_) WHERE literal_ IS NOT "
         'NULL)')
}, {
    'testcase_name':
        '_withDateTimeYearMonthDayHoursMinutesSecondsMilliTz',
    'fhir_path_expression':
        '@2015-02-04T14:34:28.123+09:00',
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(literal_) FROM (SELECT '
         "TO_TIMESTAMP(DATE_FORMAT('2015-02-04T14:34:28.123000+09:00', "
         "\"yyyy-MM-dd'T'HH:mm:ss.SSSZ\")) AS literal_) WHERE literal_ IS NOT "
         'NULL)')
}, {
    'testcase_name':
        '_withTimeHours',
    'fhir_path_expression':
        '@T14',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                "FROM (SELECT '14' AS literal_) "
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withTimeHoursMinutes',
    'fhir_path_expression':
        '@T14:34',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                "FROM (SELECT '14:34' AS literal_) "
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withTimeHoursMinutesSeconds',
    'fhir_path_expression':
        '@T14:34:28',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                "FROM (SELECT '14:34:28' AS literal_) "
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withTimeHoursMinutesSecondsMilli',
    'fhir_path_expression':
        '@T14:34:28.123',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                "FROM (SELECT '14:34:28.123' AS literal_) "
                                'WHERE literal_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withQuantity',
    'fhir_path_expression':
        "10 'mg'",
    'expected_sql_expression': ('(SELECT COLLECT_LIST(literal_) '
                                "FROM (SELECT '10 'mg'' AS literal_) "
                                'WHERE literal_ IS NOT NULL)')
}]

_WITH_FHIRPATH_V2_ARITHMETIC_SUCCEEDS_CASES = [{
    'testcase_name':
        '_withIntegerAddition',
    'fhir_path_expression':
        '1 + 2',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (1 + 2) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDecimalAddition',
    'fhir_path_expression':
        '3.14 + 1.681',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (3.14 + 1.681) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerDivision',
    'fhir_path_expression':
        '3 / 2',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (3 / 2) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDecimalDivision',
    'fhir_path_expression':
        '3.14 / 1.681',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (3.14 / 1.681) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerModularArithmetic',
    'fhir_path_expression':
        '2 mod 5',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT MOD(2, 5) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerMultiplication',
    'fhir_path_expression':
        '2 * 5',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (2 * 5) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDecimalMultiplication',
    'fhir_path_expression':
        '2.124 * 5.72',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (2.124 * 5.72) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerSubtraction',
    'fhir_path_expression':
        '2 - 5',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (2 - 5) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDecimalSubtraction',
    'fhir_path_expression':
        '2.124 - 5.72',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (2.124 - 5.72) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerTrunctatedDivision',
    'fhir_path_expression':
        '2 div 5',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT DIV(2, 5) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withDecimalTruncatedDivision',
    'fhir_path_expression':
        '2.124 div 5.72',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT DIV(2.124, 5.72) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerAdditionAndMultiplication',
    'fhir_path_expression':
        '(1 + 2) * 3',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT ((1 + 2) * 3) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerSubtractionAndDivision',
    'fhir_path_expression':
        '(21 - 6) / 3',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT ((21 - 6) / 3) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerAdditionAndModularArithmetic',
    'fhir_path_expression':
        '21 + 6 mod 5',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (21 + MOD(6, 5)) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withIntegerAdditionAndTruncatedDivision',
    'fhir_path_expression':
        '21 + 6 div 5',
    'expected_sql_expression': ('(SELECT COLLECT_LIST(arith_) '
                                'FROM (SELECT (21 + DIV(6, 5)) AS arith_) '
                                'WHERE arith_ IS NOT NULL)')
}, {
    'testcase_name':
        '_withStringConcatenationAmpersand',
    'fhir_path_expression':
        "'foo' & 'bar'",
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(arith_) '
         'FROM (SELECT CONCAT(\'foo\', \'bar\') AS arith_) '
         'WHERE arith_ IS NOT NULL)'),
}, {
    'testcase_name':
        '_withStringConcatenationPlus',
    'fhir_path_expression':
        "'foo' + 'bar'",
    'expected_sql_expression':
        ('(SELECT COLLECT_LIST(arith_) '
         'FROM (SELECT CONCAT(\'foo\', \'bar\') AS arith_) '
         'WHERE arith_ IS NOT NULL)')
}]

_WITH_FHIRPATH_V2_INDEXER_SUCCEEDS_CASES = [
    {
        'testcase_name':
            '_withIntegerIndexer',
        'fhir_path_expression':
            '7[0]',
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
            'element_at(COLLECT_LIST(literal_),0 + 1) AS indexed_literal_ FROM '
            '(SELECT 7 AS literal_)) WHERE indexed_literal_ IS NOT NULL)')
    },
    {
        'testcase_name':
            '_withIntegerIndexerArithmeticIndex',
        'fhir_path_expression':
            '7[0 + 1]',  # Out-of-bounds, empty table
        'expected_sql_expression':
            ('(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
             'element_at(COLLECT_LIST(literal_),(0 + 1) + 1) AS '
             'indexed_literal_ FROM (SELECT 7 AS literal_)) WHERE '
             'indexed_literal_ IS NOT NULL)')
    },
    {
        'testcase_name':
            '_withStringIndexer',
        'fhir_path_expression':
            "'foo'[0]",
        'expected_sql_expression': (
            '(SELECT COLLECT_LIST(indexed_literal_) FROM (SELECT '
            'element_at(COLLECT_LIST(literal_),0 + 1) AS indexed_literal_ FROM '
            "(SELECT 'foo' AS literal_)) WHERE indexed_literal_ IS NOT NULL)")
    }
]


class FhirPathSparkSqlEncoderTest(fhir_path_test_base.FhirPathTestBase,
                                  parameterized.TestCase):
  """Unit tests for `fhir_path.FhirPathSparkSqlEncoder`."""

  def assertEvaluationNodeSqlCorrect(
      self,
      structdef: message.Message,
      fhir_path_expression: str,
      expected_sql_expression: str,
      select_scalars_as_array: bool = True) -> None:
    builder = self.create_builder_from_str(structdef, fhir_path_expression)

    actual_sql_expression = self.spark_interpreter.encode(
        builder, select_scalars_as_array=select_scalars_as_array)
    self.assertEqual(
        actual_sql_expression.replace('\n', ' '), expected_sql_expression)

  @parameterized.named_parameters(
      _WITH_FHIRPATH_V2_DATETIME_LITERAL_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2DateTimeLiteral_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str):
    self.assertEvaluationNodeSqlCorrect(None, fhir_path_expression,
                                        expected_sql_expression)

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_ARITHMETIC_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2Arithmetic_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str):
    self.assertEvaluationNodeSqlCorrect(
        structdef=None,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True)

  @parameterized.named_parameters(_WITH_FHIRPATH_V2_INDEXER_SUCCEEDS_CASES)
  def testEncode_withFhirPathV2LiteralIndexer_succeeds(
      self, fhir_path_expression: str, expected_sql_expression: str):
    self.assertEvaluationNodeSqlCorrect(
        structdef=None,
        fhir_path_expression=fhir_path_expression,
        expected_sql_expression=expected_sql_expression,
        select_scalars_as_array=True)

  def testEncode_withFhirPathV2SelectScalarsAsArrayFalseForLiteral_succeeds(
      self):
    fhir_path_expression = 'true'
    expected_sql_expression = '(SELECT TRUE AS literal_)'
    self.assertEvaluationNodeSqlCorrect(None, fhir_path_expression,
                                        expected_sql_expression, False)

if __name__ == '__main__':
  absltest.main()

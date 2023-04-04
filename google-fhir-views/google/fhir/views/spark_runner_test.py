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
"""Tests for spark_runner."""

import datetime

from typing import Optional
from unittest import mock
from sqlalchemy import engine

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import context
from google.fhir.r4 import r4_package
from google.fhir.views import r4
from google.fhir.views import spark_runner
from google.fhir.views import views


_WITH_VALUE_SET_TABLE_INIT_SUCCEEDS_CASES = [
    {
        'testcase_name': 'None_usesDefaultName',
        'value_set_codes_table': None,
        'expected_table_name': 'value_set_codes',
    },
    {
        'testcase_name': 'String_succeeds',
        'value_set_codes_table': 'my_custom_table',
        'expected_table_name': 'my_custom_table',
    },
]

_WITH_VIEW_DATASET_INIT_SUCCEEDS_CASES = [
    {
        'testcase_name': 'None_usesDefaultName',
        'view_dataset': None,
        'expected_dataset_name': 'test_dataset',
    },
    {
        'testcase_name': 'String_succeeds',
        'view_dataset': 'my_custom_dataset',
        'expected_dataset_name': 'my_custom_dataset',
    },
]


class SparkRunnerTest(parameterized.TestCase):
  """Tests the Spark Runner."""

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    self.mock_spark_engine = mock.create_autospec(engine.Engine, instance=True)
    self.runner = spark_runner.SparkRunner(
        query_engine=self.mock_spark_engine,
        fhir_dataset='default',
        value_set_codes_table='vs_table',
    )
    self._context = context.LocalFhirPathContext(r4_package.load_base_r4())
    self._views = r4.from_definitions(self._context)

  def AstAndExpressionTreeTestRunner(
      self,
      expected_output: str,
      view: views.View,
      runner: spark_runner.SparkRunner,
      limit: Optional[int] = None,
  ):
    actual_sql_expression = runner.to_sql(view, limit=limit)
    self.assertEqual(
        actual_sql_expression.replace('\n', ' '), expected_output)

  @parameterized.named_parameters(_WITH_VALUE_SET_TABLE_INIT_SUCCEEDS_CASES)
  def testInit_withValueSetTableAs(
      self, value_set_codes_table, expected_table_name
  ):
    """Tests initializing with a valueset table."""
    runner = spark_runner.SparkRunner(
        query_engine=self.mock_spark_engine,
        fhir_dataset='test_dataset',
        value_set_codes_table=value_set_codes_table,
    )
    self.assertEqual(runner._value_set_codes_table, expected_table_name)  # pylint: disable=protected-access

  @parameterized.named_parameters(_WITH_VIEW_DATASET_INIT_SUCCEEDS_CASES)
  def testInit_withViewDatasetAs(self, view_dataset, expected_dataset_name):
    """Tests initializing with a view dataset."""
    runner = spark_runner.SparkRunner(
        query_engine=self.mock_spark_engine,
        fhir_dataset='test_dataset',
        view_dataset=view_dataset,
    )
    self.assertEqual(runner._view_dataset, expected_dataset_name)  # pylint: disable=protected-access

  def testNestedSingleFieldInNestedArray_forPatient_returnsArray(self):
    """Tests selecting a single field in a nested array."""
    patient = self._views.view_of('Patient')
    simple_view = patient.select({
        'family_names': patient.name.family,
    })

    self.AstAndExpressionTreeTestRunner(
        expected_output=(
            'SELECT (SELECT COLLECT_LIST(family) '
            'FROM (SELECT name_element_.family FROM '
            '(SELECT EXPLODE(name_element_) AS name_element_ '
            'FROM (SELECT name AS name_element_))) '
            'WHERE family IS NOT NULL) AS family_names,'
            '(SELECT id) AS __patientId__ FROM `default`.Patient'
        ),
        view=simple_view,
        runner=self.runner,
    )

  def testNoSelectToSql_forPatient_succeeds(self):
    """Tests that a view with no select fields succeeds."""
    patient = self._views.view_of('Patient')
    self.AstAndExpressionTreeTestRunner(
        expected_output=(
            'SELECT *,(SELECT id) AS __patientId__ FROM `default`.Patient'
        ),
        view=patient,
        runner=self.runner,
    )

  def testSimpleSelectToSql_forPatient_succeeds(self):
    """Tests simple select."""
    patient = self._views.view_of('Patient')
    simple_view = patient.select(
        {'name': patient.name.given, 'birthDate': patient.birthDate}
    )

    self.AstAndExpressionTreeTestRunner(
        expected_output=(
            'SELECT (SELECT COLLECT_LIST(given_element_) FROM '
            '(SELECT given_element_ FROM (SELECT name_element_ FROM '
            '(SELECT EXPLODE(name_element_) AS name_element_ FROM '
            '(SELECT name AS name_element_))) '
            'LATERAL VIEW POSEXPLODE(name_element_.given) AS '
            'index_given_element_, given_element_) '
            'WHERE given_element_ IS NOT NULL) AS name,'
            '(SELECT CAST(birthDate AS TIMESTAMP) AS birthDate) AS birthDate,'
            '(SELECT id) AS __patientId__ FROM `default`.Patient'
        ),
        view=simple_view,
        runner=self.runner,
    )

  def testSnakeCaseTableName_forPatient_succeeds(self):
    """Tests snake_case_resource_tables setting."""
    snake_case_runner = spark_runner.SparkRunner(
        self.mock_spark_engine,
        'default',
        value_set_codes_table='vs_table',
        snake_case_resource_tables=True
    )

    patient = self._views.view_of('Patient')
    simple_view = patient.select({'birthDate': patient.birthDate})
    self.AstAndExpressionTreeTestRunner(
        expected_output=(
            'SELECT (SELECT CAST(birthDate AS TIMESTAMP) AS birthDate) AS '
            'birthDate,(SELECT id) AS __patientId__ FROM `default`.patient'
        ),
        view=simple_view,
        runner=snake_case_runner,
    )

  def testSimpleSelectAndWhereToSql_forPatient_succeeds(self):
    """Test simple select with where."""
    patient = self._views.view_of('Patient')
    active_patients_view = patient.select(
        {'name': patient.name.given, 'birthDate': patient.birthDate}
    ).where(patient.active)

    # TODO(b/208900793): Remove array offsets when the SQL generator can
    # return single values.
    expected_sql = (
        'SELECT (SELECT COLLECT_LIST(given_element_) '
        'FROM (SELECT given_element_ FROM (SELECT name_element_ FROM '
        '(SELECT EXPLODE(name_element_) AS name_element_ FROM '
        '(SELECT name AS name_element_))) '
        'LATERAL VIEW POSEXPLODE(name_element_.given) AS index_given_element_, '
        'given_element_) WHERE given_element_ IS NOT NULL) AS name,'
        '(SELECT CAST(birthDate AS TIMESTAMP) AS birthDate) AS birthDate,'
        '(SELECT id) AS __patientId__ FROM `default`.Patient '
        'WHERE (SELECT EXISTS(*, x -> x IS true) FROM '
        '(SELECT COLLECT_LIST(active) FROM (SELECT active) '
        'WHERE active IS NOT NULL))'
    )
    self.AstAndExpressionTreeTestRunner(
        expected_output=expected_sql,
        view=active_patients_view,
        runner=self.runner,
    )
    self.AstAndExpressionTreeTestRunner(
        expected_output=expected_sql + ' LIMIT 5',
        view=active_patients_view,
        runner=self.runner,
        limit=5,
    )

  def testSimpleSelectAndWhereWithDateFilterToSql_forPatient_succeeds(self):
    """Tests filtering with a date conditional."""
    pat = self._views.view_of('Patient')
    born_before_1960 = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.birthDate < datetime.date(1960, 1, 1))

    self.AstAndExpressionTreeTestRunner(
        expected_output=(
            'SELECT (SELECT COLLECT_LIST(given_element_) FROM '
            '(SELECT given_element_ FROM (SELECT name_element_ FROM '
            '(SELECT EXPLODE(name_element_) AS name_element_ FROM '
            '(SELECT name AS name_element_))) '
            'LATERAL VIEW POSEXPLODE(name_element_.given) AS '
            'index_given_element_, given_element_) '
            'WHERE given_element_ IS NOT NULL) AS name,'
            '(SELECT CAST(birthDate AS TIMESTAMP) AS birthDate) AS birthDate,'
            '(SELECT id) AS __patientId__ FROM `default`.Patient '
            'WHERE (SELECT EXISTS(*, x -> x IS true) FROM '
            '(SELECT COLLECT_LIST(comparison_) FROM '
            '(SELECT CAST(birthDate AS TIMESTAMP) < '
            "CAST('1960-01-01' AS TIMESTAMP) AS comparison_) "
            'WHERE comparison_ IS NOT NULL))'
        ),
        view=born_before_1960,
        runner=self.runner
    )

  def testSimpleSelectWithArrayOfDateToSql_forPatient_succeeds(self):
    """Tests selecting an array of dates."""
    pat = self._views.view_of('Patient')
    telecom = pat.select(
        {'name': pat.name.given, 'telecom': pat.telecom.period.start}
    )

    self.AstAndExpressionTreeTestRunner(
        expected_output=(
            'SELECT (SELECT COLLECT_LIST(given_element_) FROM (SELECT'
            ' given_element_ FROM (SELECT name_element_ FROM (SELECT'
            ' EXPLODE(name_element_) AS name_element_ FROM (SELECT name AS'
            ' name_element_))) LATERAL VIEW POSEXPLODE(name_element_.given) AS'
            ' index_given_element_, given_element_) WHERE given_element_ IS NOT'
            ' NULL) AS name,(SELECT COLLECT_LIST(start) FROM (SELECT'
            ' CAST(telecom_element_.period.start AS TIMESTAMP) AS start FROM'
            ' (SELECT EXPLODE(telecom_element_) AS telecom_element_ FROM'
            ' (SELECT telecom AS telecom_element_))) WHERE start IS NOT NULL)'
            ' AS telecom,(SELECT id) AS __patientId__ FROM `default`.Patient'
        ),
        view=telecom,
        runner=self.runner,
    )


if __name__ == '__main__':
  absltest.main()

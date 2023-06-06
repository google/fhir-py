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

import pandas
from sqlalchemy import engine

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import context
from google.fhir.r4 import r4_package
from google.fhir.views import r4
from google.fhir.views import spark_runner
from google.fhir.views import spark_value_set_manager
from google.fhir.views import views


_WITH_VALUE_SET_TABLE_INIT_SUCCEEDS_CASES = [
    {
        'testcase_name': 'none_uses_default_name',
        'value_set_codes_table': None,
        'expected_table_name': 'value_set_codes',
    },
    {
        'testcase_name': 'string_succeeds',
        'value_set_codes_table': 'my_custom_table',
        'expected_table_name': 'my_custom_table',
    },
]

_WITH_VIEW_DATASET_INIT_SUCCEEDS_CASES = [
    {
        'testcase_name': 'none_uses_default_name',
        'view_dataset': None,
        'expected_dataset_name': 'test_dataset',
    },
    {
        'testcase_name': 'string_succeeds',
        'view_dataset': 'my_custom_dataset',
        'expected_dataset_name': 'my_custom_dataset',
    },
]


class SparkRunnerTest(parameterized.TestCase):
  """Tests the Spark Runner."""

  def setUp(self):
    super().setUp()
    self.mock_read_sql_query = self.enter_context(
        mock.patch.object(pandas, 'read_sql_query', autospec=True)
    )

    self.addCleanup(mock.patch.stopall)
    self.mock_spark_engine = mock.create_autospec(engine.Engine, instance=True)
    self.runner = spark_runner.SparkRunner(
        query_engine=self.mock_spark_engine,
        fhir_dataset='default',
        value_set_codes_table='vs_table',
    )
    self._context = context.LocalFhirPathContext(r4_package.load_base_r4())
    self._views = r4.from_definitions(self._context)

  def ast_and_expression_tree_test_runner(
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
  def test_init_with_value_set_table_as(
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
  def test_init_with_view_dataset_as(self, view_dataset, expected_dataset_name):
    """Tests initializing with a view dataset."""
    runner = spark_runner.SparkRunner(
        query_engine=self.mock_spark_engine,
        fhir_dataset='test_dataset',
        view_dataset=view_dataset,
    )
    self.assertEqual(runner._view_dataset, expected_dataset_name)  # pylint: disable=protected-access

  def test_nested_single_field_in_nested_array_for_patient_returns_array(self):
    """Tests selecting a single field in a nested array."""
    patient = self._views.view_of('Patient')
    simple_view = patient.select({
        'family_names': patient.name.family,
    })

    self.ast_and_expression_tree_test_runner(
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

  def test_no_select_to_sql_for_patient_succeeds(self):
    """Tests that a view with no select fields succeeds."""
    patient = self._views.view_of('Patient')
    self.ast_and_expression_tree_test_runner(
        expected_output=(
            'SELECT *,(SELECT id) AS __patientId__ FROM `default`.Patient'
        ),
        view=patient,
        runner=self.runner,
    )

  def test_simple_select_to_sql_for_patient_succeeds(self):
    """Tests simple select."""
    patient = self._views.view_of('Patient')
    simple_view = patient.select(
        {'name': patient.name.given, 'birthDate': patient.birthDate}
    )

    self.ast_and_expression_tree_test_runner(
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

  def test_snake_case_table_name_for_patient_succeeds(self):
    """Tests snake_case_resource_tables setting."""
    snake_case_runner = spark_runner.SparkRunner(
        self.mock_spark_engine,
        'default',
        value_set_codes_table='vs_table',
        snake_case_resource_tables=True
    )

    patient = self._views.view_of('Patient')
    simple_view = patient.select({'birthDate': patient.birthDate})
    self.ast_and_expression_tree_test_runner(
        expected_output=(
            'SELECT (SELECT CAST(birthDate AS TIMESTAMP) AS birthDate) AS '
            'birthDate,(SELECT id) AS __patientId__ FROM `default`.patient'
        ),
        view=simple_view,
        runner=snake_case_runner,
    )

  def test_simple_select_and_where_to_sql_for_patient_succeeds(self):
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
    self.ast_and_expression_tree_test_runner(
        expected_output=expected_sql,
        view=active_patients_view,
        runner=self.runner,
    )
    self.ast_and_expression_tree_test_runner(
        expected_output=expected_sql + ' LIMIT 5',
        view=active_patients_view,
        runner=self.runner,
        limit=5,
    )

  def test_simple_select_and_where_with_date_filter_to_sql_for_patient_succeeds(
      self,
  ):
    """Tests filtering with a date conditional."""
    pat = self._views.view_of('Patient')
    born_before_1960 = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.birthDate < datetime.date(1960, 1, 1))

    self.ast_and_expression_tree_test_runner(
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
        runner=self.runner,
    )

  def test_simple_select_with_complex_filter_to_sql_for_patient_succeeds(self):
    """Tests filtering with a complex filter."""
    pats = self._views.view_of('Patient')

    current = pats.address.where(pats.address.period.empty()).first()

    simple_pats = pats.select({
        'id': pats.id,
        'gender': pats.gender,
        'birthdate': pats.birthDate,
        'street': current.line.first(),
        'city': current.city,
        'state': current.state,
        'zip': current.postalCode,
    })

    self.ast_and_expression_tree_test_runner(
        expected_output=(
            'SELECT (SELECT id) AS id,(SELECT gender) AS gender,(SELECT'
            ' CAST(birthDate AS TIMESTAMP) AS birthDate) AS birthdate,(SELECT'
            ' line_element_ FROM (SELECT FIRST(line_element_) AS line_element_'
            ' FROM (SELECT line_element_ FROM (SELECT address_element_ FROM'
            ' (SELECT FIRST(address_element_) AS address_element_ FROM (SELECT'
            ' address_element_ FROM (SELECT EXPLODE(address_element_) AS'
            ' address_element_ FROM (SELECT address AS address_element_)) WHERE'
            ' (SELECT CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS empty_'
            ' FROM (SELECT address_element_.period) WHERE period IS NOT'
            ' NULL)))) LATERAL VIEW POSEXPLODE(address_element_.line) AS'
            ' index_line_element_, line_element_))) AS street,(SELECT'
            ' address_element_.city FROM (SELECT FIRST(address_element_) AS'
            ' address_element_ FROM (SELECT address_element_ FROM (SELECT'
            ' EXPLODE(address_element_) AS address_element_ FROM (SELECT'
            ' address AS address_element_)) WHERE (SELECT CASE WHEN COUNT(*) ='
            ' 0 THEN TRUE ELSE FALSE END AS empty_ FROM (SELECT'
            ' address_element_.period) WHERE period IS NOT NULL)))) AS'
            ' city,(SELECT address_element_.state FROM (SELECT'
            ' FIRST(address_element_) AS address_element_ FROM (SELECT'
            ' address_element_ FROM (SELECT EXPLODE(address_element_) AS'
            ' address_element_ FROM (SELECT address AS address_element_)) WHERE'
            ' (SELECT CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS empty_'
            ' FROM (SELECT address_element_.period) WHERE period IS NOT'
            ' NULL)))) AS state,(SELECT address_element_.postalCode FROM'
            ' (SELECT FIRST(address_element_) AS address_element_ FROM (SELECT'
            ' address_element_ FROM (SELECT EXPLODE(address_element_) AS'
            ' address_element_ FROM (SELECT address AS address_element_)) WHERE'
            ' (SELECT CASE WHEN COUNT(*) = 0 THEN TRUE ELSE FALSE END AS empty_'
            ' FROM (SELECT address_element_.period) WHERE period IS NOT'
            ' NULL)))) AS zip,(SELECT id) AS __patientId__ FROM'
            ' `default`.Patient'
        ),
        view=simple_pats,
        runner=self.runner,
    )

  def test_simple_select_with_array_of_date_to_sql_for_patient_succeeds(self):
    """Tests selecting an array of dates."""
    pat = self._views.view_of('Patient')
    telecom = pat.select(
        {'name': pat.name.given, 'telecom': pat.telecom.period.start}
    )

    self.ast_and_expression_tree_test_runner(
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

  def test_query_to_data_frame_for_patient_succeeds(self):
    """Test to_dataframe()."""
    patient = self._views.view_of('Patient')
    simple_view = patient.select(
        {'gender': patient.gender, 'birthDate': patient.birthDate}
    )
    expected_df = pandas.DataFrame({
        'gender': ['male', 'male', 'female', 'female', 'male'],
        'birthDate': [
            '2002-01-01',
            '2009-08-02',
            '2016-10-17',
            '2017-05-23',
            '2003-01-17',
        ],
    })
    self.mock_read_sql_query.return_value = expected_df

    returned_df = self.runner.to_dataframe(simple_view)
    self.assertTrue(expected_df.equals(returned_df))

  def test_summarize_codes_for_observation_codeable_succeeds(self):
    obs = self._views.view_of('Observation')
    self.runner.summarize_codes(obs, obs.category)
    expected_sql = (
        'WITH c AS (SELECT (SELECT COLLECT_LIST(category_element_)\nFROM'
        ' (SELECT category_element_\nFROM (SELECT EXPLODE(category_element_) AS'
        ' category_element_ FROM (SELECT category AS'
        ' category_element_)))\nWHERE category_element_ IS NOT NULL) as target'
        ' FROM `default`.Observation) SELECT codings.system, codings.code,'
        ' codings.display, COUNT(*) count FROM c LATERAL VIEW EXPLODE(c.target)'
        ' AS concepts LATERAL VIEW EXPLODE(concepts.coding) AS codings GROUP BY'
        ' 1, 2, 3 ORDER BY count DESC'
    )
    self.mock_read_sql_query.assert_called_once_with(
        expected_sql, self.mock_spark_engine
    )

  def test_summarize_codes_for_observation_status_code_succeeds(self):
    obs = self._views.view_of('Observation')
    self.runner.summarize_codes(obs, obs.status)
    expected_sql = (
        'WITH c AS (SELECT (SELECT COLLECT_LIST(status)\nFROM (SELECT'
        ' status)\nWHERE status IS NOT NULL) as target FROM'
        ' `default`.Observation) SELECT code, COUNT(*) count FROM c LATERAL'
        ' VIEW EXPLODE(c.target) as code GROUP BY 1 ORDER BY count DESC'
    )
    self.mock_read_sql_query.assert_called_once_with(
        expected_sql, self.mock_spark_engine
    )

  def test_summarize_codes_for_observation_coding_succeeds(self):
    obs = self._views.view_of('Observation')
    self.runner.summarize_codes(obs, obs.code.coding)
    expected_sql = (
        'WITH c AS (SELECT (SELECT COLLECT_LIST(coding_element_)\nFROM (SELECT'
        ' coding_element_\nFROM (SELECT code) LATERAL VIEW'
        ' POSEXPLODE(code.coding) AS index_coding_element_,'
        ' coding_element_)\nWHERE coding_element_ IS NOT NULL) as target FROM'
        ' `default`.Observation) SELECT codings.system, codings.code,'
        ' codings.display, COUNT(*) count FROM c LATERAL VIEW EXPLODE(c.target)'
        ' AS codings GROUP BY 1, 2, 3 ORDER BY count DESC'
    )
    self.mock_read_sql_query.assert_called_once_with(
        expected_sql, self.mock_spark_engine
    )

  def test_summarize_codes_for_observation_non_code_field_raises_error(self):
    obs = self._views.view_of('Observation')
    with self.assertRaises(ValueError):
      self.runner.summarize_codes(obs, obs.referenceRange)

  def test_create_view_for_patient_succeeds(self):
    """Tests creating a view for a Patient."""
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    )
    self.runner.create_database_view(simple_view, 'simple_patient_view')
    expected_sql = (
        'CREATE OR REPLACE VIEW '
        'default.simple_patient_view AS\n'
        f'{self.runner.to_sql(simple_view, include_patient_id_col=False)}'
    )
    self.mock_spark_engine.execute.assert_called_once_with(expected_sql)

  @mock.patch.object(
      spark_value_set_manager.SparkValueSetManager,
      'materialize_value_sets',
      autospec=True,
  )
  def test_materialize_value_set_delegates_to_manager(
      self, mock_materialize_value_sets
  ):
    """Tests inserting data through mock call."""
    mock_value_sets = [mock.MagicMock(), mock.MagicMock()]

    self.runner.materialize_value_sets(mock_value_sets, 100)

    mock_materialize_value_sets.assert_called_once_with(
        mock.ANY, mock_value_sets, 100
    )

  @mock.patch.object(
      spark_value_set_manager.SparkValueSetManager,
      'materialize_value_set_expansion',
      autospec=True,
  )
  def test_materialize_value_set_expansion_delegates_to_manager(
      self, mock_materialize_value_set_expansion
  ):
    """Tests materialize through mock calls."""
    mock_expander = mock.MagicMock()

    self.runner.materialize_value_set_expansion(
        ['url-1', 'url-2'], mock_expander, None, 100
    )

    mock_materialize_value_set_expansion.assert_called_once_with(
        mock.ANY, ['url-1', 'url-2'], mock_expander, None, 100
    )


if __name__ == '__main__':
  absltest.main()

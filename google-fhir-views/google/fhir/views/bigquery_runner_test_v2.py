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
"""Tests for bigquery_runner."""

import datetime
import textwrap
from unittest import mock

from google.cloud import bigquery

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import context
from google.fhir.r4 import r4_package
from google.fhir.r4.terminology import terminology_service_client
from google.fhir.r4.terminology import value_sets
from google.fhir.views import bigquery_runner
from google.fhir.views import r4
from google.fhir.views import views


class BigqueryRunnerTest(parameterized.TestCase):
  """Tests the bigquery runner running on v2."""

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._fhir_package = r4_package.load_base_r4()

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    self.mock_bigquery_client = mock.create_autospec(
        bigquery.Client, instance=True)
    self.mock_bigquery_client.project = 'test_project'
    self.runner = bigquery_runner.BigQueryRunner(
        self.mock_bigquery_client,
        'test_dataset',
        value_set_codes_table='vs_project.vs_dataset.vs_table',
        internal_default_to_v2_runner=True)
    self._context = context.LocalFhirPathContext(self._fhir_package)
    self._views = r4.from_definitions(self._context)

  def AstAndExpressionTreeTestRunner(
      self,
      expected_output: str,
      view: views.View,
      bq_runner: bigquery_runner.BigQueryRunner = None,
      limit: int = None):
    if not bq_runner:
      bq_runner = self.runner
    self.assertMultiLineEqual(expected_output,
                              bq_runner.to_sql(view, limit=limit))

  @parameterized.named_parameters(
      dict(
          testcase_name='None_usesDefaultName',
          value_set_codes_table=None,
          expected_table_name=bigquery.table.TableReference.from_string(
              'test_project.test_dataset.value_set_codes'),
      ),
      dict(
          testcase_name='String_succeeds',
          value_set_codes_table='project.dataset.table',
          expected_table_name=bigquery.table.TableReference.from_string(
              'project.dataset.table',),
      ),
      dict(
          testcase_name='StringWithNoProject_succeeds',
          value_set_codes_table='dataset.table',
          expected_table_name=bigquery.table.TableReference.from_string(
              'test_project.dataset.table',),
      ),
      dict(
          testcase_name='TableReference_succeeds',
          value_set_codes_table=bigquery.table.TableReference.from_string(
              'project.dataset.table'),
          expected_table_name=bigquery.table.TableReference.from_string(
              'project.dataset.table'),
      ),
      dict(
          testcase_name='Table_succeeds',
          value_set_codes_table=bigquery.table.Table(
              bigquery.table.TableReference.from_string(
                  'project.dataset.table')),
          expected_table_name=bigquery.table.TableReference.from_string(
              'project.dataset.table'),
      ),
  )
  def testInit_withValueSetTableAs(self, value_set_codes_table,
                                   expected_table_name):
    """Tests initializing with a valueset table."""
    runner = bigquery_runner.BigQueryRunner(
        self.mock_bigquery_client,
        'test_dataset',
        value_set_codes_table=value_set_codes_table)
    self.assertEqual(runner._value_set_codes_table, expected_table_name)  # pylint: disable=protected-access

  def testNestedSingleFieldInNestedArray_forPatient_returnsArray(self):
    """Tests selecting a single field in a nested array."""
    pat = self._views.view_of('Patient')
    simple_view = (
        pat.select({
            'family_names': pat.name.family,
        }))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
          SELECT ARRAY(SELECT family
          FROM (SELECT name_element_.family
          FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset)
          WHERE family IS NOT NULL) AS family_names,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient"""
                       ), simple_view)

  def testNoSelectToSql_forPatient_succeeds(self):
    """Tests that a view with no select fields succeeds."""
    pat = self._views.view_of('Patient')
    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
          SELECT *,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient"""
                       ), pat)

  def testSimpleSelectToSql_forPatient_succeeds(self):
    """Tests simple select."""
    pat = self._views.view_of('Patient')
    simple_view = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,(SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient"""
                       ), simple_view)

  def testSnakeCaseTableName_forPatient_succeeds(self):
    """Tests snake_case_resource_tables setting."""
    snake_case_runner = bigquery_runner.BigQueryRunner(
        self.mock_bigquery_client,
        'test_dataset',
        value_set_codes_table='vs_project.vs_dataset.vs_table',
        snake_case_resource_tables=True,
        internal_default_to_v2_runner=True)

    pat = self._views.view_of('Patient')
    simple_view = pat.select({'birthDate': pat.birthDate})
    self.AstAndExpressionTreeTestRunner(
        expected_output=textwrap.dedent("""\
        SELECT (SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.patient"""
                                       ),
        view=simple_view,
        bq_runner=snake_case_runner)

    med_rec = self._views.view_of('MedicationRequest')
    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
          SELECT *,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.medication_request"""
                       ),
        view=med_rec,
        bq_runner=snake_case_runner)

  def testSimpleSelectAndWhereToSql_forPatient_succeeds(self):
    """Test simple select with where."""
    pat = self._views.view_of('Patient')
    active_patients_view = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }).where(pat.active))

    # TODO: Remove array offsets when the SQL generator can
    # return single values.
    expected_sql = textwrap.dedent("""\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,(SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT active
        FROM (SELECT active)
        WHERE active IS NOT NULL)) AS logic_)""")
    self.AstAndExpressionTreeTestRunner(expected_sql, active_patients_view)
    self.AstAndExpressionTreeTestRunner(
        expected_sql + ' LIMIT 5', active_patients_view, limit=5)

  def testInvalidLimit_forPatient_fails(self):
    """Test invalid limit."""
    pat = self._views.view_of('Patient')
    patient_names = pat.select({
        'name': pat.name.given,
    })
    with self.assertRaises(ValueError):
      self.runner.to_dataframe(patient_names, limit=-1)

  def testSimpleSelectAndWhereWithDateFilterToSql_forPatient_succeeds(self):
    """Tests filtering with a date conditional."""
    pat = self._views.view_of('Patient')
    born_before_1960 = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }).where(pat.birthDate < datetime.date(1960, 1, 1)))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,(SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT comparison_
        FROM (SELECT (PARSE_TIMESTAMP("%Y-%m-%d", birthDate) < PARSE_TIMESTAMP("%Y-%m-%d", '1960-01-01')) AS comparison_)
        WHERE comparison_ IS NOT NULL)) AS logic_)"""), born_before_1960)

  def testSimpleSelectWithArrayOfDateToSql_forPatient_succeeds(self):
    """Tests selecting an array of dates."""
    pat = self._views.view_of('Patient')
    telecom = (
        pat.select({
            'name': pat.name.given,
            'telecom': pat.telecom.period.start
        }))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,ARRAY(SELECT start
        FROM (SELECT PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez", telecom_element_.period.start) AS start
        FROM UNNEST(telecom) AS telecom_element_ WITH OFFSET AS element_offset)
        WHERE start IS NOT NULL) AS telecom,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient"""
                       ), telecom)

  def testQueryToDataFrame_forPatient_succeeds(self):
    """Test to_dataframe()."""
    pat = self._views.view_of('Patient')
    simple_view = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }))

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    expected_mock_df = mock_job.result.return_value.to_dataframe.return_value
    self.mock_bigquery_client.query.return_value = mock_job

    returned_df = self.runner.to_dataframe(simple_view)
    # Ensure expected SQL was passed to BigQuery and the dataframe was returned
    # up the stack.
    expected_sql = self.runner.to_sql(simple_view, include_patient_id_col=False)
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    self.assertEqual(expected_mock_df, returned_df)

  def testTimestampComparison_succeeds(self):
    """Test timestamp comparison."""

    start_date = datetime.date(2012, 1, 1)
    end_date = datetime.date(2013, 1, 1)

    eob = self._views.view_of('ExplanationOfBenefit')
    eob_view = eob.where(
        eob.addItem.serviced.ofType('Date') >= start_date,
        eob.addItem.serviced.ofType('Date') < end_date)

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        SELECT *,(SELECT patient.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.ExplanationOfBenefit
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT comparison_
        FROM (SELECT ((SELECT PARSE_TIMESTAMP("%Y-%m-%d", addItem_element_.serviced.Date) AS ofType_
        FROM UNNEST(addItem) AS addItem_element_ WITH OFFSET AS element_offset) >= PARSE_TIMESTAMP("%Y-%m-%d", '2012-01-01')) AS comparison_)
        WHERE comparison_ IS NOT NULL)) AS logic_) AND (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT comparison_
        FROM (SELECT ((SELECT PARSE_TIMESTAMP("%Y-%m-%d", addItem_element_.serviced.Date) AS ofType_
        FROM UNNEST(addItem) AS addItem_element_ WITH OFFSET AS element_offset) < PARSE_TIMESTAMP("%Y-%m-%d", '2013-01-01')) AS comparison_)
        WHERE comparison_ IS NOT NULL)) AS logic_)"""), eob_view)

  def testWhereMemberOfToSql_withValuesFromContext_succeeds(self):
    """Test memberOf with value set."""
    pat = self._views.view_of('Patient')
    unmarried_value_set = r4.value_set('urn:test:valueset').with_codes(
        'http://hl7.org/fhir/v3/MaritalStatus', ['U', 'S']).build()

    # Test loading code values from context, which could be loaded from
    # an external service in future implementations.
    self._context.add_local_value_set(unmarried_value_set)
    active_patients_view = (
        pat.select({
            'birthDate': pat.birthDate
        }).where(pat.maritalStatus.memberOf(unmarried_value_set.url.value)))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "S" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "U" as code)
        SELECT (SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT memberof_
        FROM (SELECT memberof_
        FROM UNNEST((SELECT IF(maritalStatus IS NULL, [], [
        EXISTS(
        SELECT 1
        FROM UNNEST(maritalStatus.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='urn:test:valueset'
        AND vs.system=codings.system
        AND vs.code=codings.code
        )]))) AS memberof_)
        WHERE memberof_ IS NOT NULL)) AS logic_)"""), active_patients_view)

  def testWhereMemberOfToSql_withVersionedValuesFromContext_succeeds(self):
    """Tests memberOf with versioned value set."""
    pat = self._views.view_of('Patient')
    unmarried_value_set = r4.value_set('urn:test:valueset').with_codes(
        'http://hl7.org/fhir/v3/MaritalStatus',
        ['U', 'S']).with_version('1.0').build()

    # Test loading code values from context, which could be loaded from
    # an external service in future implementations.
    self._context.add_local_value_set(unmarried_value_set)
    active_patients_view = (
        pat.select({
            'birthDate': pat.birthDate
        }).where(
            pat.maritalStatus.memberOf(f'{unmarried_value_set.url.value}')))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "S" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "U" as code)
        SELECT (SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT memberof_
        FROM (SELECT memberof_
        FROM UNNEST((SELECT IF(maritalStatus IS NULL, [], [
        EXISTS(
        SELECT 1
        FROM UNNEST(maritalStatus.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='urn:test:valueset'
        AND vs.system=codings.system
        AND vs.code=codings.code
        )]))) AS memberof_)
        WHERE memberof_ IS NOT NULL)) AS logic_)"""), active_patients_view)

  def testWhereMemberOfToSql_withValuesSetInConstraintOperand_succeeds(self):
    """Tests memberOf with valueset and comparison."""
    pat = self._views.view_of('Patient')
    unmarried_value_set = r4.value_set('urn:test:valueset').with_codes(
        'http://hl7.org/fhir/v3/MaritalStatus',
        ['U', 'S']).with_version('1.0').build()

    # Ensure we still find the value set within the memberOf call when the
    # memberOf is itself an operand.
    self._context.add_local_value_set(unmarried_value_set)
    active_patients_view = (
        pat.select({
            'birthDate': pat.birthDate
        }).where(
            # pylint: disable=g-explicit-bool-comparison singleton-comparison
            pat.maritalStatus.memberOf(f'{unmarried_value_set.url.value}') ==
            True))
    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "S" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "U" as code)
        SELECT (SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT eq_
        FROM (SELECT ((SELECT memberof_
        FROM UNNEST((SELECT IF(maritalStatus IS NULL, [], [
        EXISTS(
        SELECT 1
        FROM UNNEST(maritalStatus.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='urn:test:valueset'
        AND vs.system=codings.system
        AND vs.code=codings.code
        )]))) AS memberof_) = TRUE) AS eq_)
        WHERE eq_ IS NOT NULL)) AS logic_)"""), active_patients_view)

  def testWhereMemberOfToSql_withLiteralValues_succeeds(self):
    """Tests memberOf with literal value set."""
    obs = self._views.view_of('Observation')

    # Use a value set proto in the expression, so they are not loaded from
    # context.
    hba1c_value_set = r4.value_set('urn:test:valueset').with_codes(
        'http://loinc.org', ['10346-5', '10486-9']).build()
    hba1c_obs_view = (
        obs.select({
            'id': obs.id,
            'status': obs.status,
            'time': obs.issued,
        }).where(obs.code.memberOf(hba1c_value_set)))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://loinc.org" as system, "10346-5" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://loinc.org" as system, "10486-9" as code)
        SELECT (SELECT id) AS id,(SELECT status) AS status,(SELECT PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez", issued) AS issued) AS time,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.Observation
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT memberof_
        FROM (SELECT memberof_
        FROM UNNEST((SELECT IF(code IS NULL, [], [
        EXISTS(
        SELECT 1
        FROM UNNEST(code.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='urn:test:valueset'
        AND vs.system=codings.system
        AND vs.code=codings.code
        )]))) AS memberof_)
        WHERE memberof_ IS NOT NULL)) AS logic_)"""), hba1c_obs_view)

  def testWhereMemberOfToSql_withVersionedLiteralValues_succeeds(self):
    """Tests memberOf with literal values in the valueset and versions."""
    obs = self._views.view_of('Observation')

    # Use a value set proto in the expression, so they are not loaded from
    # context.
    hba1c_value_set = r4.value_set('urn:test:valueset').with_codes(
        'http://loinc.org', ['10346-5', '10486-9']).with_version('1.0').build()
    hba1c_obs_view = (
        obs.select({
            'id': obs.id,
            'status': obs.status,
            'time': obs.issued,
        }).where(obs.code.memberOf(hba1c_value_set)))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://loinc.org" as system, "10346-5" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://loinc.org" as system, "10486-9" as code)
        SELECT (SELECT id) AS id,(SELECT status) AS status,(SELECT PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez", issued) AS issued) AS time,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.Observation
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT memberof_
        FROM (SELECT memberof_
        FROM UNNEST((SELECT IF(code IS NULL, [], [
        EXISTS(
        SELECT 1
        FROM UNNEST(code.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='urn:test:valueset'
        AND vs.system=codings.system
        AND vs.code=codings.code
        )]))) AS memberof_)
        WHERE memberof_ IS NOT NULL)) AS logic_)"""), hba1c_obs_view)

  def testWhereMemberOf_fromNestedField_succeeds(self):
    """Tests member of with a given value set."""
    next_of_kin_value_set = r4.value_set('urn:test:valueset').with_codes(
        'http://terminology.hl7.org/CodeSystem/v2-0131', ['N']).build()
    pat = self._views.view_of('Patient')
    simple_view = (
        pat.select({
            'name': pat.name.given,
        }).where(pat.contact.relationship.memberOf(next_of_kin_value_set)))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://terminology.hl7.org/CodeSystem/v2-0131" as system, "N" as code)
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT memberof_
        FROM (SELECT matches.element_offset IS NOT NULL AS memberof_
        FROM (SELECT element_offset
        FROM (SELECT contact_element_
        FROM UNNEST(contact) AS contact_element_ WITH OFFSET AS element_offset),
        UNNEST(contact_element_.relationship) AS relationship_element_ WITH OFFSET AS element_offset) AS all_
        LEFT JOIN (SELECT element_offset
        FROM UNNEST(ARRAY(SELECT element_offset FROM (
        SELECT DISTINCT element_offset
        FROM (SELECT contact_element_
        FROM UNNEST(contact) AS contact_element_ WITH OFFSET AS element_offset),
        UNNEST(contact_element_.relationship) AS relationship_element_ WITH OFFSET AS element_offset,
        UNNEST(relationship_element_.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='urn:test:valueset'
        AND vs.system=codings.system
        AND vs.code=codings.code
        ))) AS element_offset
        ) AS matches
        ON all_.element_offset=matches.element_offset
        ORDER BY all_.element_offset)
        WHERE memberof_ IS NOT NULL)) AS logic_)"""), simple_view)

  def testWhereMemberOfToSql_withValuesFromTable_succeeds(self):
    """Tests memberOf with a valueset url."""
    pat = self._views.view_of('Patient')

    active_patients_view = (
        pat.select({
            'birthDate': pat.birthDate
        }).where(pat.maritalStatus.memberOf('http://a-value.set/id')))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT valueseturi, valuesetversion, system, code FROM vs_project.vs_dataset.vs_table)
        SELECT (SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT memberof_
        FROM (SELECT memberof_
        FROM UNNEST((SELECT IF(maritalStatus IS NULL, [], [
        EXISTS(
        SELECT 1
        FROM UNNEST(maritalStatus.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='http://a-value.set/id'
        AND vs.system=codings.system
        AND vs.code=codings.code
        )]))) AS memberof_)
        WHERE memberof_ IS NOT NULL)) AS logic_)"""), active_patients_view)

  def testWhereMemberOfToSql_withVersionedValueSetUrlAgainstCodesTable_succeeds(
      self):
    """Tests memberOf with a valueset url with versions."""
    pat = self._views.view_of('Patient')

    active_patients_view = (
        pat.select({
            'birthDate': pat.birthDate
        }).where(pat.maritalStatus.memberOf('http://a-value.set/id|1.0')))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT valueseturi, valuesetversion, system, code FROM vs_project.vs_dataset.vs_table)
        SELECT (SELECT PARSE_TIMESTAMP("%Y-%m-%d", birthDate) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT memberof_
        FROM (SELECT memberof_
        FROM UNNEST((SELECT IF(maritalStatus IS NULL, [], [
        EXISTS(
        SELECT 1
        FROM UNNEST(maritalStatus.coding) AS codings
        INNER JOIN `VALUESET_VIEW` vs ON
        vs.valueseturi='http://a-value.set/id'
        AND vs.valuesetversion='1.0'
        AND vs.system=codings.system
        AND vs.code=codings.code
        )]))) AS memberof_)
        WHERE memberof_ IS NOT NULL)) AS logic_)"""), active_patients_view)

  def testWhereMemberOfToSql_withofTypeCall_succeeds(self):
    """Tests memberOf with ofType."""
    meds = self._views.view_of('MedicationRequest')

    statin_meds = (
        meds.select({
            'patient': meds.subject.idFor('Patient'),
            'authoredOn': meds.authoredOn
        }).where(
            meds.medication.ofType('CodeableConcept').memberOf(
                'http://a-value.set/id')))

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    self.mock_bigquery_client.query.return_value = mock_job

    self.runner.create_bigquery_view(statin_meds, 'statin_meds_view')

    # Ensure expected SQL was passed to BigQuery and job was returned.
    expected_sql = (
        textwrap.dedent(
            'CREATE OR REPLACE VIEW '
            '`test_project.test_dataset.statin_meds_view` AS\n'
            f'{self.runner.to_sql(statin_meds, internal_v2=True, include_patient_id_col=False)}'
        ))
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    mock_job.result.assert_called_once()

  def testQueryToJob_forPatient_succeeds(self):
    """Tests sending the query as a job."""
    pat = self._views.view_of('Patient')
    simple_view = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }))

    expected_mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    self.mock_bigquery_client.query.return_value = expected_mock_job

    returned_job = self.runner.run_query(simple_view)
    # Ensure expected SQL was passed to BigQuery and job was returned.
    expected_sql = self.runner.to_sql(simple_view, include_patient_id_col=False)
    self.mock_bigquery_client.query.assert_called_with(expected_sql)
    self.assertEqual(expected_mock_job, returned_job)

    limited_job = self.runner.run_query(simple_view, limit=10)
    # Ensure expected limited SQL was passed to BigQuery and job was returned.
    limited_sql = self.runner.to_sql(
        simple_view, limit=10, include_patient_id_col=False)
    self.mock_bigquery_client.query.assert_called_with(limited_sql)
    self.assertEqual(expected_mock_job, limited_job)

  def testCreateView_forPatient_succeeds(self):
    """Tests creating a view for a Patient."""
    pat = self._views.view_of('Patient')
    simple_view = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }))

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    self.mock_bigquery_client.query.return_value = mock_job

    self.runner.create_bigquery_view(simple_view, 'simple_patient_view')

    # Ensure expected SQL was passed to BigQuery and job was returned.
    expected_sql = (
        f'CREATE OR REPLACE VIEW '
        f'`test_project.test_dataset.simple_patient_view` AS\n'
        f'{self.runner.to_sql(simple_view, include_patient_id_col=False)}')
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    mock_job.result.assert_called_once()

  def testSelectRawSubjectId_forPatient_succeeds(self):
    """Tests selecting id."""
    obs = self._views.view_of('Observation')

    obs_with_raw_patient_id_view = (
        obs.select({
            'id': obs.id,
            'patientId': obs.subject.idFor('Patient'),
            'status': obs.status,
        }))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        SELECT (SELECT id) AS id,(SELECT subject.PatientId AS idFor_) AS patientId,(SELECT status) AS status,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.Observation"""
                       ), obs_with_raw_patient_id_view)

  def testValueOf_forObservationString_succeeds(self):
    """Tests ofType."""
    obs = self._views.view_of('Observation')

    obs_with_value = obs.select({
        'id': obs.id,
        'value': obs.value.ofType('string')
    })

    self.AstAndExpressionTreeTestRunner(
        'SELECT (SELECT id) AS id,'
        '(SELECT value.string AS ofType_) AS value,'
        '(SELECT subject.patientId AS idFor_) AS __patientId__'
        ' FROM `test_project.test_dataset`.Observation', obs_with_value)

  def testNestValueOf_forObservationQuantity_succeeds(self):
    """Tests expressions from an ofType invocation."""
    obs = self._views.view_of('Observation')

    obs_with_value = obs.select({
        'id': obs.id,
        'value': obs.value.ofType('Quantity').value,
        'unit': obs.value.ofType('Quantity').unit
    })

    self.AstAndExpressionTreeTestRunner(
        'SELECT (SELECT id) AS id,(SELECT value.Quantity.value) AS value,'
        '(SELECT value.Quantity.unit) AS unit,'
        '(SELECT subject.patientId AS idFor_) AS __patientId__ '
        'FROM `test_project.test_dataset`.Observation', obs_with_value)

  def testNestValueOf_forExplanationOfBenefit_andCodeableConcept_succeeds(self):
    """Tests complicated CodeableConcept expression."""
    eob = self._views.view_of('ExplanationOfBenefit')

    eob_with_codeableconcept_system = eob.select({
        'id':
            eob.id,
        'system':
            eob.procedure.procedure.ofType('CodeableConcept').coding.system,
    })

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent("""\
        SELECT (SELECT id) AS id,ARRAY(SELECT system
        FROM (SELECT coding_element_.system
        FROM (SELECT procedure_element_.procedure.CodeableConcept AS ofType_
        FROM UNNEST(procedure) AS procedure_element_ WITH OFFSET AS element_offset),
        UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset)
        WHERE system IS NOT NULL) AS system,(SELECT patient.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.ExplanationOfBenefit"""
                       ), eob_with_codeableconcept_system)

  def testSummarizeCodes_forObservation_succeeds(self):
    """Tests summarizing codes."""
    obs = self._views.view_of('Observation')

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    expected_mock_df = mock_job.result.return_value.to_dataframe.return_value
    self.mock_bigquery_client.query.return_value = mock_job

    returned_df = self.runner.summarize_codes(obs, obs.category)
    # Ensure expected SQL was passed to BigQuery and the dataframe was returned
    # up the stack.
    expected_sql = ('WITH c AS (SELECT ARRAY(SELECT category_element_\n'
                    'FROM (SELECT category_element_\nFROM UNNEST(category) AS '
                    'category_element_ WITH OFFSET AS element_offset)\n'
                    'WHERE category_element_ IS NOT NULL) as target FROM '
                    '`test_project.test_dataset`.Observation) SELECT '
                    'codings.system, codings.code, codings.display, COUNT(*) '
                    'count FROM c, UNNEST(c.target) concepts, '
                    'UNNEST(concepts.coding) as codings GROUP BY 1, 2, 3 ORDER '
                    'BY count DESC')
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    self.assertEqual(expected_mock_df, returned_df)

  @mock.patch.object(
      bigquery_runner.value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True)
  def testMaterializeValueSet_withValueSetObject_insertsData(
      self, mock_valueset_codes_insert_statement_for):
    """Tests inserting data through mock call."""
    mock_value_sets = [mock.MagicMock(), mock.MagicMock()]
    mock_insert_statements = [mock.MagicMock(), mock.MagicMock()]
    mock_valueset_codes_insert_statement_for.return_value = mock_insert_statements
    self.mock_bigquery_client.create_table.return_value = _BqValuesetCodesTable(
        'vs_project.vs_dataset.vs_table')

    self.runner.materialize_value_sets(mock_value_sets)

    # Ensure we tried to create the table
    self.mock_bigquery_client.create_table.assert_called_once()

    # Ensure we called query with the rendered SQL for the two mock queries and
    # called .result() on the returned job.
    self.mock_bigquery_client.query.assert_has_calls([
        mock.call(str(mock_insert_statements[0].compile())),
        mock.call().result(),
        mock.call(str(mock_insert_statements[1].compile())),
        mock.call().result(),
    ])

    # Ensure we called valueset_codes_insert_statement_for with the
    # given value sets.
    args, kwargs = mock_valueset_codes_insert_statement_for.call_args_list[0]
    expanded_value_sets, table = args
    self.assertEqual(list(expanded_value_sets), mock_value_sets)

    self.assertEqual(table.name, 'vs_project.vs_dataset.vs_table')
    for col in ('valueseturi', 'valuesetversion', 'system', 'code'):
      self.assertIn(col, table.columns)
    self.assertEqual(kwargs['batch_size'], 500)

  @mock.patch.object(
      bigquery_runner.value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True)
  def testMaterializeValueSetExpansion_withValueSetUrls_performsExpansionsAndInserts(
      self, mock_valueset_codes_insert_statement_for):
    """Tests materialize through mock calls."""
    mock_insert_statements = [mock.MagicMock(), mock.MagicMock()]
    mock_valueset_codes_insert_statement_for.return_value = mock_insert_statements
    mock_expander = mock.MagicMock()
    self.mock_bigquery_client.create_table.return_value = _BqValuesetCodesTable(
        'vs_project.vs_dataset.vs_table')

    self.runner.materialize_value_set_expansion(['url-1', 'url-2'],
                                                mock_expander)

    # Ensure we tried to create the table
    self.mock_bigquery_client.create_table.assert_called_once()

    # Ensure we called query with the rendered SQL for the two mock queries and
    # called .result() on the returned job.
    self.mock_bigquery_client.query.assert_has_calls([
        mock.call(str(mock_insert_statements[0].compile())),
        mock.call().result(),
        mock.call(str(mock_insert_statements[1].compile())),
        mock.call().result(),
    ])
    # Ensure we called valueset_codes_insert_statement_for with the value set
    # expansions for both URLs and with an appropriate table object.
    args, kwargs = mock_valueset_codes_insert_statement_for.call_args_list[0]
    expanded_value_sets, table = args
    self.assertEqual(
        list(expanded_value_sets), [
            mock_expander.expand_value_set_url(),
            mock_expander.expand_value_set_url()
        ])
    self.assertEqual(table.name, 'vs_project.vs_dataset.vs_table')
    for col in ('valueseturi', 'valuesetversion', 'system', 'code'):
      self.assertIn(col, table.columns)
    self.assertEqual(kwargs['batch_size'], 500)

    # Ensure we call expand_value_set_url with the two URLs.
    mock_expander.expand_value_set_url.reset()
    mock_expander.expand_value_set_url.assert_has_calls(
        [mock.call('url-1'), mock.call('url-2')])

  @mock.patch.object(
      bigquery_runner.value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True)
  def testMaterializeValueSetExpansion_withTerminologyServiceUrl_usesGivenTerminologyServiceUrl(
      self, mock_valueset_codes_insert_statement_for):
    """Tests materialze value sets with terminology service url."""
    mock_expander = mock.MagicMock(
        spec=terminology_service_client.TerminologyServiceClient)
    self.mock_bigquery_client.create_table.return_value = _BqValuesetCodesTable(
        'vs_project.vs_dataset.vs_table')

    self.runner.materialize_value_set_expansion(
        ['url-1', 'url-2'],
        mock_expander,
        terminology_service_url='http://my-service.com')

    # Ensure we called valueset_codes_insert_statement_for with the value set
    # expansions for both URLs and with an appropriate table object.
    args, _ = mock_valueset_codes_insert_statement_for.call_args_list[0]
    expanded_value_sets, table = args
    self.assertEqual(
        list(expanded_value_sets), [
            mock_expander.expand_value_set_url_using_service(),
            mock_expander.expand_value_set_url_using_service()
        ])
    self.assertEqual(table.name, 'vs_project.vs_dataset.vs_table')

    # Ensure we call expand_value_set_url_using_service with the right URLs.
    mock_expander.expand_value_set_url.reset()
    mock_expander.expand_value_set_url_using_service.assert_has_calls([
        mock.call('url-1', 'http://my-service.com'),
        mock.call('url-2', 'http://my-service.com')
    ])

  def testMaterializeValueSetExpansion_withTerminologyServiceUrlAndValueSetResolver_raisesError(
      self):
    mock_expander = mock.MagicMock(spec=value_sets.ValueSetResolver)

    with self.assertRaises(TypeError):
      self.runner.materialize_value_set_expansion(
          ['url-1', 'url-2'],
          mock_expander,
          terminology_service_url='http://my-service.com')

  def testCreateValusetCodesTableIfNotExists_callsClientCorrectly(self):
    self.runner._create_valueset_codes_table_if_not_exists()  # pylint: disable=protected-access

    expected_table = _BqValuesetCodesTable('vs_project.vs_dataset.vs_table')
    self.mock_bigquery_client.create_table.assert_called_once_with(
        expected_table, exists_ok=True)


def _BqValuesetCodesTable(name: str) -> bigquery.table.Table:
  """Builds a BigQuery client table representation of a value set codes table.
  """
  schema = [
      bigquery.SchemaField('valueseturi', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('valuesetversion', 'STRING', mode='NULLABLE'),
      bigquery.SchemaField('system', 'STRING', mode='REQUIRED'),
      bigquery.SchemaField('code', 'STRING', mode='REQUIRED'),
  ]
  table = bigquery.Table(name, schema=schema)
  table.clustering_fields = ['valueseturi', 'code']
  return table


if __name__ == '__main__':
  absltest.main()

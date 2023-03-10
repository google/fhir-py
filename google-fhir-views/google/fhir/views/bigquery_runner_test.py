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
from typing import Optional, cast
from unittest import mock

from google.cloud import bigquery
import numpy
import pandas

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.fhir_path import context
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import r4_package
from google.fhir.r4.terminology import terminology_service_client
from google.fhir.r4.terminology import value_sets
from google.fhir.views import bigquery_runner
from google.fhir.views import r4
from google.fhir.views import views


class BigqueryRunnerTest(parameterized.TestCase):
  _fhir_package: fhir_package.FhirPackage

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._fhir_package = r4_package.load_base_r4()

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    self.mock_bigquery_client = mock.create_autospec(
        bigquery.Client, instance=True
    )
    self.mock_bigquery_client.project = 'test_project'
    self.runner = bigquery_runner.BigQueryRunner(
        self.mock_bigquery_client,
        'test_dataset',
        value_set_codes_table='vs_project.vs_dataset.vs_table',
    )
    self._context = context.LocalFhirPathContext(self._fhir_package)
    self._views = r4.from_definitions(self._context)

  def AstAndExpressionTreeTestRunner(
      self,
      expected_output: str,
      view: views.View,
      bq_runner: Optional[bigquery_runner.BigQueryRunner] = None,
      limit: Optional[int] = None,
  ):
    if not bq_runner:
      bq_runner = self.runner
    self.assertMultiLineEqual(
        expected_output, bq_runner.to_sql(view, limit=limit)
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='None_usesDefaultName',
          value_set_codes_table=None,
          expected_table_name=bigquery.table.TableReference.from_string(
              'test_project.test_dataset.value_set_codes'
          ),
      ),
      dict(
          testcase_name='String_succeeds',
          value_set_codes_table='project.dataset.table',
          expected_table_name=bigquery.table.TableReference.from_string(
              'project.dataset.table',
          ),
      ),
      dict(
          testcase_name='StringWithNoProject_succeeds',
          value_set_codes_table='dataset.table',
          expected_table_name=bigquery.table.TableReference.from_string(
              'test_project.dataset.table',
          ),
      ),
      dict(
          testcase_name='TableReference_succeeds',
          value_set_codes_table=bigquery.table.TableReference.from_string(
              'project.dataset.table'
          ),
          expected_table_name=bigquery.table.TableReference.from_string(
              'project.dataset.table'
          ),
      ),
      dict(
          testcase_name='Table_succeeds',
          value_set_codes_table=bigquery.table.Table(
              bigquery.table.TableReference.from_string('project.dataset.table')
          ),
          expected_table_name=bigquery.table.TableReference.from_string(
              'project.dataset.table'
          ),
      ),
  )
  def testInit_withValueSetTableAs(
      self, value_set_codes_table, expected_table_name
  ):
    runner = bigquery_runner.BigQueryRunner(
        self.mock_bigquery_client,
        'test_dataset',
        value_set_codes_table=value_set_codes_table,
    )
    self.assertEqual(runner._value_set_codes_table, expected_table_name)

  def testNestedSingleFieldInNestedArray_forPatient_returnsArray(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select({
        'family_names': pat.name.family,
    })

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
          SELECT ARRAY(SELECT family
          FROM (SELECT name_element_.family
          FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset)
          WHERE family IS NOT NULL) AS family_names,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient"""
        ),
        simple_view,
    )

  def testNoSelectToSql_forPatient_succeeds(self):
    """Tests that a view with no select fields succeeds."""
    pat = self._views.view_of('Patient')
    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
          SELECT *,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient"""
        ),
        pat,
    )

  def testSimpleSelectToSql_forPatient_succeeds(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    )

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient"""
        ),
        simple_view,
    )

  def testSnakeCaseTableName_forPatient_succeeds(self):
    snake_case_runner = bigquery_runner.BigQueryRunner(
        self.mock_bigquery_client,
        'test_dataset',
        value_set_codes_table='vs_project.vs_dataset.vs_table',
        snake_case_resource_tables=True,
    )

    pat = self._views.view_of('Patient')
    simple_view = pat.select({'birthDate': pat.birthDate})
    self.AstAndExpressionTreeTestRunner(
        expected_output=textwrap.dedent(
            """\
          SELECT PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.patient"""
        ),
        view=simple_view,
        bq_runner=snake_case_runner,
    )

    med_rec = self._views.view_of('MedicationRequest')
    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
          SELECT *,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.medication_request"""
        ),
        view=med_rec,
        bq_runner=snake_case_runner,
    )

  def testSimpleSelectAndWhereToSql_forPatient_succeeds(self):
    pat = self._views.view_of('Patient')
    active_patients_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.active)

    # TODO(b/208900793): Remove array offsets when the SQL generator can
    # return single values.
    expected_sql = textwrap.dedent(
        """\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT active
        FROM (SELECT active)
        WHERE active IS NOT NULL)) AS logic_)"""
    )
    self.AstAndExpressionTreeTestRunner(expected_sql, active_patients_view)
    self.AstAndExpressionTreeTestRunner(
        expected_sql + ' LIMIT 5', active_patients_view, limit=5
    )

  def testInvalidLimit_forPatient_fails(self):
    pat = self._views.view_of('Patient')
    patient_names = pat.select({
        'name': pat.name.given,
    })
    with self.assertRaises(ValueError):
      self.runner.to_dataframe(patient_names, limit=-1)

  def testSimpleSelectAndWhereWithDateFilterToSql_forPatient_succeeds(self):
    pat = self._views.view_of('Patient')
    born_before_1960 = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.birthDate < datetime.date(1960, 1, 1))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT comparison_
        FROM (SELECT (birthDate < '1960-01-01') AS comparison_)
        WHERE comparison_ IS NOT NULL)) AS logic_)"""
        ),
        born_before_1960,
    )

  def testQueryToDataFrame_forPatient_succeeds(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    )

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    expected_mock_df = mock_job.result.return_value.to_dataframe.return_value
    self.mock_bigquery_client.query.return_value = mock_job

    returned_df = self.runner.to_dataframe(simple_view)
    # Ensure expected SQL was passed to BigQuery and the dataframe was returned
    # up the stack.
    expected_sql = self.runner.to_sql(simple_view, include_patient_id_col=False)
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    self.assertEqual(expected_mock_df, returned_df)

  def testQueryToDataFrame_TrimsStructWithSelect_succeeds(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select({
        'name': pat.name.given,
        'address': pat.address,
        'maritalStatus': pat.maritalStatus,
    })

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    fake_df = pandas.DataFrame.from_dict({
        'name': ['Bob'],
        'address': [numpy.empty(shape=0)],
        'maritalStatus': [{
            'coding': numpy.array([{
                'system': 'urn:examplesystem',
                'code': 'S',
                'display': None,
            }]),
            'text': None,
        }],
    })
    mock_job.result.return_value.to_dataframe.return_value = fake_df
    self.mock_bigquery_client.query.return_value = mock_job

    returned_df = self.runner.to_dataframe(simple_view)
    self.mock_bigquery_client.query.assert_called_once()

    # Assert simple fields are unchanged.
    self.assertEqual(['Bob'], returned_df['name'].values)

    # Empty arrays should be converted to no values (so users can easily use
    # dropna() and other pandas features).
    self.assertEqual([None], returned_df['address'].values)

    # 'None' values should be trimmed from nested structures.
    self.assertEqual(
        [{'coding': [{'system': 'urn:examplesystem', 'code': 'S'}]}],
        returned_df['maritalStatus'].values,
    )

  def testQueryToDataFrame_TrimsStructNoSelect_succeeds(self):
    # Use base query directly to ensure trimming logic works
    # without explicit select columns.
    pat = self._views.view_of('Patient')

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    fake_df = pandas.DataFrame.from_dict({
        'name': ['Bob'],
        'address': [numpy.empty(shape=0)],
        'maritalStatus': [{
            'coding': numpy.array([{
                'system': 'urn:examplesystem',
                'code': 'S',
                'display': None,
            }]),
            'text': None,
        }],
    })
    mock_job.result.return_value.to_dataframe.return_value = fake_df
    self.mock_bigquery_client.query.return_value = mock_job

    returned_df = self.runner.to_dataframe(pat)
    self.mock_bigquery_client.query.assert_called_once()

    # Assert simple fields are unchanged.
    self.assertEqual(['Bob'], returned_df['name'].values)

    # Empty arrays should be converted to no values (so users can easily use
    # dropna() and other pandas features).
    self.assertEqual([None], returned_df['address'].values)

    # 'None' values should be trimmed from nested structures.
    self.assertEqual(
        [{'coding': [{'system': 'urn:examplesystem', 'code': 'S'}]}],
        returned_df['maritalStatus'].values,
    )

  def testWhereMemberOfToSql_withValuesFromContext_succeeds(self):
    pat = self._views.view_of('Patient')
    unmarried_value_set = (
        r4.value_set('urn:test:valueset')
        .with_codes('http://hl7.org/fhir/v3/MaritalStatus', ['U', 'S'])
        .build()
    )

    # Test loading code values from context, which could be loaded from
    # an external service in future implementations.
    self._context.add_local_value_set(unmarried_value_set)
    active_patients_view = pat.select({'birthDate': pat.birthDate}).where(
        pat.maritalStatus.memberOf(
            cast(value_set_pb2.ValueSet, unmarried_value_set).url.value
        )
    )

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "S" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "U" as code)
        SELECT PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
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
        WHERE memberof_ IS NOT NULL)) AS logic_)"""
        ),
        active_patients_view,
    )

  def testWhereMemberOfToSql_withVersionedValuesFromContext_succeeds(self):
    pat = self._views.view_of('Patient')
    unmarried_value_set = (
        r4.value_set('urn:test:valueset')
        .with_codes('http://hl7.org/fhir/v3/MaritalStatus', ['U', 'S'])
        .with_version('1.0')
        .build()
    )

    # Test loading code values from context, which could be loaded from
    # an external service in future implementations.
    self._context.add_local_value_set(unmarried_value_set)
    active_patients_view = pat.select({'birthDate': pat.birthDate}).where(
        pat.maritalStatus.memberOf(
            f'{cast(value_set_pb2.ValueSet, unmarried_value_set).url.value}'
        )
    )

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "S" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "U" as code)
        SELECT PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
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
        WHERE memberof_ IS NOT NULL)) AS logic_)"""
        ),
        active_patients_view,
    )

  def testWhereMemberOfToSql_withValuesSetInConstraintOperand_succeeds(self):
    pat = self._views.view_of('Patient')
    unmarried_value_set = (
        r4.value_set('urn:test:valueset')
        .with_codes('http://hl7.org/fhir/v3/MaritalStatus', ['U', 'S'])
        .with_version('1.0')
        .build()
    )

    # Ensure we still find the value set within the memberOf call when the
    # memberOf is itself an operand.
    self._context.add_local_value_set(unmarried_value_set)
    active_patients_view = pat.select({'birthDate': pat.birthDate}).where(
        # pylint: disable=g-explicit-bool-comparison singleton-comparison
        pat.maritalStatus.memberOf(
            f'{cast(value_set_pb2.ValueSet, unmarried_value_set).url.value}'
        )
        == True
    )
    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "S" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://hl7.org/fhir/v3/MaritalStatus" as system, "U" as code)
        SELECT PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
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
        WHERE eq_ IS NOT NULL)) AS logic_)"""
        ),
        active_patients_view,
    )

  def testWhereMemberOfToSql_withLiteralValues_succeeds(self):
    obs = self._views.view_of('Observation')

    # Use a value set proto in the expression, so they are not loaded from
    # context.
    hba1c_value_set = (
        r4.value_set('urn:test:valueset')
        .with_codes('http://loinc.org', ['10346-5', '10486-9'])
        .build()
    )
    hba1c_obs_view = obs.select({
        'id': obs.id,
        'status': obs.status,
        'time': obs.issued,
    }).where(obs.code.memberOf(hba1c_value_set))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://loinc.org" as system, "10346-5" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, NULL as valuesetversion, "http://loinc.org" as system, "10486-9" as code)
        SELECT (SELECT id) AS id,(SELECT status) AS status,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez", (SELECT issued)) AS time,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.Observation
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
        WHERE memberof_ IS NOT NULL)) AS logic_)"""
        ),
        hba1c_obs_view,
    )

  def testWhereMemberOfToSql_withVersionedLiteralValues_succeeds(self):
    obs = self._views.view_of('Observation')

    # Use a value set proto in the expression, so they are not loaded from
    # context.
    hba1c_value_set = (
        r4.value_set('urn:test:valueset')
        .with_codes('http://loinc.org', ['10346-5', '10486-9'])
        .with_version('1.0')
        .build()
    )
    hba1c_obs_view = obs.select({
        'id': obs.id,
        'status': obs.status,
        'time': obs.issued,
    }).where(obs.code.memberOf(hba1c_value_set))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        WITH VALUESET_VIEW AS (SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://loinc.org" as system, "10346-5" as code
        UNION ALL SELECT "urn:test:valueset" as valueseturi, "1.0" as valuesetversion, "http://loinc.org" as system, "10486-9" as code)
        SELECT (SELECT id) AS id,(SELECT status) AS status,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez", (SELECT issued)) AS time,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.Observation
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
        WHERE memberof_ IS NOT NULL)) AS logic_)"""
        ),
        hba1c_obs_view,
    )

  def testWhereMemberOf_fromNestedField_succeeds(self):
    next_of_kin_value_set = (
        r4.value_set('urn:test:valueset')
        .with_codes('http://terminology.hl7.org/CodeSystem/v2-0131', ['N'])
        .build()
    )
    pat = self._views.view_of('Patient')
    simple_view = pat.select({
        'name': pat.name.given,
    }).where(pat.contact.relationship.memberOf(next_of_kin_value_set))

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
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
        WHERE memberof_ IS NOT NULL)) AS logic_)"""
        ),
        simple_view,
    )

  def testWhereMemberOfToSql_withValuesFromTable_succeeds(self):
    pat = self._views.view_of('Patient')

    active_patients_view = pat.select({'birthDate': pat.birthDate}).where(
        pat.maritalStatus.memberOf('http://a-value.set/id')
    )

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        WITH VALUESET_VIEW AS (SELECT valueseturi, valuesetversion, system, code FROM vs_project.vs_dataset.vs_table)
        SELECT PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
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
        WHERE memberof_ IS NOT NULL)) AS logic_)"""
        ),
        active_patients_view,
    )

  def testWhereMemberOfToSql_withVersionedValueSetUrlAgainstCodesTable_succeeds(
      self,
  ):
    pat = self._views.view_of('Patient')

    active_patients_view = pat.select({'birthDate': pat.birthDate}).where(
        pat.maritalStatus.memberOf('http://a-value.set/id|1.0')
    )

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        WITH VALUESET_VIEW AS (SELECT valueseturi, valuesetversion, system, code FROM vs_project.vs_dataset.vs_table)
        SELECT PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_project.test_dataset`.Patient
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
        WHERE memberof_ IS NOT NULL)) AS logic_)"""
        ),
        active_patients_view,
    )

  def testWhereMemberOfToSql_withofTypeCall_succeeds(self):
    meds = self._views.view_of('MedicationRequest')

    statin_meds = meds.select({
        'patient': meds.subject.idFor('Patient'),
        'authoredOn': meds.authoredOn,
    }).where(
        meds.medication.ofType('CodeableConcept').memberOf(
            'http://a-value.set/id'
        )
    )

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    self.mock_bigquery_client.query.return_value = mock_job

    self.runner.create_bigquery_view(statin_meds, 'statin_meds_view')

    # Ensure expected SQL was passed to BigQuery and job was returned.
    expected_sql = textwrap.dedent(
        'CREATE OR REPLACE VIEW '
        '`test_project.test_dataset.statin_meds_view` AS\n'
        f'{self.runner.to_sql(statin_meds, include_patient_id_col=False)}'
    )
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    mock_job.result.assert_called_once()

  def testQueryToJob_forPatient_succeeds(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    )

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
        simple_view, limit=10, include_patient_id_col=False
    )
    self.mock_bigquery_client.query.assert_called_with(limited_sql)
    self.assertEqual(expected_mock_job, limited_job)

  def testCreateView_forPatient_succeeds(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    )

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    self.mock_bigquery_client.query.return_value = mock_job

    self.runner.create_bigquery_view(simple_view, 'simple_patient_view')

    # Ensure expected SQL was passed to BigQuery and job was returned.
    expected_sql = (
        'CREATE OR REPLACE VIEW '
        '`test_project.test_dataset.simple_patient_view` AS\n'
        f'{self.runner.to_sql(simple_view, include_patient_id_col=False)}'
    )
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    mock_job.result.assert_called_once()

  def testSelectRawSubjectId_forPatient_succeeds(self):
    obs = self._views.view_of('Observation')

    obs_with_raw_patient_id_view = obs.select({
        'id': obs.id,
        'patientId': obs.subject.idFor('Patient'),
        'status': obs.status,
    })

    self.AstAndExpressionTreeTestRunner(
        textwrap.dedent(
            """\
        SELECT (SELECT id) AS id,(SELECT subject.patientId AS idFor_) AS patientId,(SELECT status) AS status,(SELECT subject.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.Observation"""
        ),
        obs_with_raw_patient_id_view,
    )

  def testValueOf_forObservationString_succeeds(self):
    obs = self._views.view_of('Observation')

    obs_with_value = obs.select(
        {'id': obs.id, 'value': obs.value.ofType('string')}
    )

    self.AstAndExpressionTreeTestRunner(
        (
            'SELECT (SELECT id) AS id,'
            '(SELECT value.string AS ofType_) AS value,'
            '(SELECT subject.patientId AS idFor_) AS __patientId__'
            ' FROM `test_project.test_dataset`.Observation'
        ),
        obs_with_value,
    )

  def testNestValueOf_forObservationQuantity_succeeds(self):
    obs = self._views.view_of('Observation')

    obs_with_value = obs.select({
        'id': obs.id,
        'value': obs.value.ofType('Quantity').value,
        'unit': obs.value.ofType('Quantity').unit,
    })

    self.AstAndExpressionTreeTestRunner(
        (
            'SELECT (SELECT id) AS id,(SELECT value.Quantity.value) AS value,'
            '(SELECT value.Quantity.unit) AS unit,'
            '(SELECT subject.patientId AS idFor_) AS __patientId__ '
            'FROM `test_project.test_dataset`.Observation'
        ),
        obs_with_value,
    )

  def testSummarizeCodes_forObservationCodeable_succeeds(self):
    obs = self._views.view_of('Observation')

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    expected_mock_df = mock_job.result.return_value.to_dataframe.return_value
    self.mock_bigquery_client.query.return_value = mock_job

    returned_df = self.runner.summarize_codes(obs, obs.category)
    # Ensure expected SQL was passed to BigQuery and the dataframe was returned
    # up the stack.
    expected_sql = (
        'WITH c AS (SELECT ARRAY(SELECT category_element_\n'
        'FROM (SELECT category_element_\nFROM UNNEST(category) AS '
        'category_element_ WITH OFFSET AS element_offset)\n'
        'WHERE category_element_ IS NOT NULL) as target FROM '
        '`test_project.test_dataset`.Observation) SELECT '
        'codings.system, codings.code, codings.display, COUNT(*) '
        'count FROM c, UNNEST(c.target) concepts, '
        'UNNEST(concepts.coding) as codings GROUP BY 1, 2, 3 ORDER '
        'BY count DESC'
    )
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    self.assertEqual(expected_mock_df, returned_df)

  def testSummarizeCodes_forObservationStatusCode_succeeds(self):
    obs = self._views.view_of('Observation')

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    expected_mock_df = mock_job.result.return_value.to_dataframe.return_value
    self.mock_bigquery_client.query.return_value = mock_job

    returned_df = self.runner.summarize_codes(obs, obs.status)
    # Ensure expected SQL was passed to BigQuery and the dataframe was returned
    # up the stack.
    expected_sql = (
        'WITH c AS (SELECT ARRAY(SELECT status\n'
        'FROM (SELECT status)\nWHERE status IS NOT NULL) '
        'as target FROM `test_project.test_dataset`.Observation) '
        'SELECT code, COUNT(*) count '
        'FROM c, UNNEST(c.target) as code '
        'GROUP BY 1 ORDER BY count DESC'
    )
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    self.assertEqual(expected_mock_df, returned_df)

  def testSummarizeCodes_forObservationNonCodeField_raisesError(self):
    obs = self._views.view_of('Observation')
    with self.assertRaises(ValueError):
      self.runner.summarize_codes(obs, obs.referenceRange)

  def testSummarizeCodes_forObservationCoding_succeeds(self):
    obs = self._views.view_of('Observation')

    mock_job = mock.create_autospec(bigquery.QueryJob, instance=True)
    expected_mock_df = mock_job.result.return_value.to_dataframe.return_value
    self.mock_bigquery_client.query.return_value = mock_job

    # Standalone coding types are rare but we should support them, so just
    # test the one inside the observation code itself.
    returned_df = self.runner.summarize_codes(obs, obs.code.coding)
    # Ensure expected SQL was passed to BigQuery and the dataframe was returned
    # up the stack.
    expected_sql = (
        'WITH c AS (SELECT ARRAY(SELECT coding_element_\n'
        'FROM (SELECT coding_element_\n'
        'FROM (SELECT code),\n'
        'UNNEST(code.coding) AS coding_element_ '
        'WITH OFFSET AS element_offset)\n'
        'WHERE coding_element_ IS NOT NULL) as target FROM '
        '`test_project.test_dataset`.Observation) '
        'SELECT codings.system, codings.code, codings.display, '
        'COUNT(*) count FROM c, UNNEST(c.target) codings '
        'GROUP BY 1, 2, 3 ORDER BY count DESC'
    )
    self.mock_bigquery_client.query.assert_called_once_with(expected_sql)
    self.assertEqual(expected_mock_df, returned_df)

  @mock.patch.object(
      bigquery_runner.value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True,
  )
  def testMaterializeValueSet_withValueSetObject_insertsData(
      self, mock_valueset_codes_insert_statement_for
  ):
    mock_value_sets = [mock.MagicMock(), mock.MagicMock()]
    mock_insert_statements = [mock.MagicMock(), mock.MagicMock()]
    mock_valueset_codes_insert_statement_for.return_value = (
        mock_insert_statements
    )
    self.mock_bigquery_client.create_table.return_value = _BqValuesetCodesTable(
        'vs_project.vs_dataset.vs_table'
    )

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
      autospec=True,
  )
  def testMaterializeValueSetExpansion_withValueSetUrls_performsExpansionsAndInserts(
      self, mock_valueset_codes_insert_statement_for
  ):
    mock_insert_statements = [mock.MagicMock(), mock.MagicMock()]
    mock_valueset_codes_insert_statement_for.return_value = (
        mock_insert_statements
    )
    mock_expander = mock.MagicMock()
    self.mock_bigquery_client.create_table.return_value = _BqValuesetCodesTable(
        'vs_project.vs_dataset.vs_table'
    )

    self.runner.materialize_value_set_expansion(
        ['url-1', 'url-2'], mock_expander
    )

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
        list(expanded_value_sets),
        [
            mock_expander.expand_value_set_url(),
            mock_expander.expand_value_set_url(),
        ],
    )
    self.assertEqual(table.name, 'vs_project.vs_dataset.vs_table')
    for col in ('valueseturi', 'valuesetversion', 'system', 'code'):
      self.assertIn(col, table.columns)
    self.assertEqual(kwargs['batch_size'], 500)

    # Ensure we call expand_value_set_url with the two URLs.
    mock_expander.expand_value_set_url.reset()
    mock_expander.expand_value_set_url.assert_has_calls(
        [mock.call('url-1'), mock.call('url-2')]
    )

  @mock.patch.object(
      bigquery_runner.value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True,
  )
  def testMaterializeValueSetExpansion_withTerminologyServiceUrl_usesGivenTerminologyServiceUrl(
      self, mock_valueset_codes_insert_statement_for
  ):
    mock_expander = mock.MagicMock(
        spec=terminology_service_client.TerminologyServiceClient
    )
    self.mock_bigquery_client.create_table.return_value = _BqValuesetCodesTable(
        'vs_project.vs_dataset.vs_table'
    )

    self.runner.materialize_value_set_expansion(
        ['url-1', 'url-2'],
        mock_expander,
        terminology_service_url='http://my-service.com',
    )

    # Ensure we called valueset_codes_insert_statement_for with the value set
    # expansions for both URLs and with an appropriate table object.
    args, _ = mock_valueset_codes_insert_statement_for.call_args_list[0]
    expanded_value_sets, table = args
    self.assertEqual(
        list(expanded_value_sets),
        [
            mock_expander.expand_value_set_url_using_service(),
            mock_expander.expand_value_set_url_using_service(),
        ],
    )
    self.assertEqual(table.name, 'vs_project.vs_dataset.vs_table')

    # Ensure we call expand_value_set_url_using_service with the right URLs.
    mock_expander.expand_value_set_url.reset()
    mock_expander.expand_value_set_url_using_service.assert_has_calls([
        mock.call('url-1', 'http://my-service.com'),
        mock.call('url-2', 'http://my-service.com'),
    ])

  def testMaterializeValueSetExpansion_withTerminologyServiceUrlAndValueSetResolver_raisesError(
      self,
  ):
    mock_expander = mock.MagicMock(spec=value_sets.ValueSetResolver)

    with self.assertRaises(TypeError):
      self.runner.materialize_value_set_expansion(
          ['url-1', 'url-2'],
          mock_expander,
          terminology_service_url='http://my-service.com',
      )

  def testCreateValusetCodesTableIfNotExists_callsClientCorrectly(self):
    self.runner._create_valueset_codes_table_if_not_exists()

    expected_table = _BqValuesetCodesTable('vs_project.vs_dataset.vs_table')
    self.mock_bigquery_client.create_table.assert_called_once_with(
        expected_table, exists_ok=True
    )

  # TODO(b/250691318): Fix an issue where the bq_intepreter generates literals
  # with an extra set of quotes.
  def testFHIRViewsExample_explanationOfBenefit_filteredByValueSet_succeeds(
      self,
  ):
    eob = self._views.view_of('ExplanationOfBenefit')

    eob_pde_codes = eob.select({
        'eob_id': eob.id,
        'patient': eob.patient.idFor('Patient'),
        'first_ndc': (
            eob.item.productOrService.coding.where(
                eob.item.productOrService.coding.system
                == 'http://hl7.org/fhir/sid/ndc'
            )
            .first()
            .code
        ),
        'serviced_date': eob.item.first().serviced.ofType('date'),
    }).where(eob.type.memberOf('http://a-value.set/id'))

    self.assertMultiLineEqual(
        """WITH VALUESET_VIEW AS (SELECT valueseturi, valuesetversion, system, code FROM vs_project.vs_dataset.vs_table)
SELECT (SELECT id) AS eob_id,(SELECT patient.patientId AS idFor_) AS patient,(SELECT coding_element_.code
FROM (SELECT item_element_.productOrService
FROM UNNEST(item) AS item_element_ WITH OFFSET AS element_offset),
UNNEST(productOrService.coding) AS coding_element_ WITH OFFSET AS element_offset
WHERE (system = 'http://hl7.org/fhir/sid/ndc')
LIMIT 1) AS first_ndc,PARSE_DATE("%Y-%m-%d", (SELECT item_element_.serviced.date AS ofType_
FROM UNNEST(item) AS item_element_ WITH OFFSET AS element_offset
LIMIT 1)) AS serviced_date,(SELECT patient.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.ExplanationOfBenefit
WHERE (SELECT LOGICAL_AND(logic_)
FROM UNNEST(ARRAY(SELECT memberof_
FROM (SELECT memberof_
FROM UNNEST((SELECT IF(type IS NULL, [], [
EXISTS(
SELECT 1
FROM UNNEST(type.coding) AS codings
INNER JOIN `VALUESET_VIEW` vs ON
vs.valueseturi='http://a-value.set/id'
AND vs.system=codings.system
AND vs.code=codings.code
)]))) AS memberof_)
WHERE memberof_ IS NOT NULL)) AS logic_)""",
        self.runner.to_sql(eob_pde_codes),
    )

  def testFHIRViewsExample_commonOutPatientProcedures_succeeds(self):
    outpatient_claims_valueset = (
        r4.value_set('urn:example:valueset:outpatient_claims')
        .with_codes(
            'https://bluebutton.cms.gov/resources/codesystem/eob-type',
            ['OUTPATIENT'],
        )
        .build()
    )

    coding = self._views.expression_for('Coding')
    eob = self._views.view_of('ExplanationOfBenefit')

    eob_outpatient_proc_codes = eob.select({
        'eob_id': eob.id,
        'patient': eob.patient.idFor('Patient'),
        'first_procedure_code': (
            eob.procedure.first()
            .procedure.ofType('CodeableConcept')
            .coding.where(coding.system == 'http://hl7.org/fhir/sid/icd-10')
            .code
        ),
        'first_procedure_date': eob.procedure.first().date,
    }).where(
        eob.type.memberOf(outpatient_claims_valueset), eob.procedure.exists()
    )

    eob_outpatient_proc_codes_2012 = eob_outpatient_proc_codes.where(
        eob_outpatient_proc_codes.first_procedure_date
        >= datetime.date(2012, 1, 1),
        eob_outpatient_proc_codes.first_procedure_date
        < datetime.date(2013, 1, 1),
    )

    self.assertMultiLineEqual(
        """WITH VALUESET_VIEW AS (SELECT "urn:example:valueset:outpatient_claims" as valueseturi, NULL as valuesetversion, "https://bluebutton.cms.gov/resources/codesystem/eob-type" as system, "OUTPATIENT" as code)
SELECT (SELECT id) AS eob_id,(SELECT patient.patientId AS idFor_) AS patient,(SELECT coding_element_.code
FROM (SELECT procedure_element_.procedure.CodeableConcept AS ofType_
FROM UNNEST(procedure) AS procedure_element_ WITH OFFSET AS element_offset
LIMIT 1),
UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset
WHERE (system = 'http://hl7.org/fhir/sid/icd-10')) AS first_procedure_code,PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%E*S%Ez", (SELECT procedure_element_.date
FROM UNNEST(procedure) AS procedure_element_ WITH OFFSET AS element_offset
LIMIT 1)) AS first_procedure_date,(SELECT patient.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.ExplanationOfBenefit
WHERE (SELECT LOGICAL_AND(logic_)
FROM UNNEST(ARRAY(SELECT memberof_
FROM (SELECT memberof_
FROM UNNEST((SELECT IF(type IS NULL, [], [
EXISTS(
SELECT 1
FROM UNNEST(type.coding) AS codings
INNER JOIN `VALUESET_VIEW` vs ON
vs.valueseturi='urn:example:valueset:outpatient_claims'
AND vs.system=codings.system
AND vs.code=codings.code
)]))) AS memberof_)
WHERE memberof_ IS NOT NULL)) AS logic_) AND (SELECT LOGICAL_AND(logic_)
FROM UNNEST(ARRAY(SELECT exists_
FROM (SELECT EXISTS(
SELECT procedure_element_
FROM (SELECT procedure_element_
FROM UNNEST(procedure) AS procedure_element_ WITH OFFSET AS element_offset)
WHERE procedure_element_ IS NOT NULL) AS exists_)
WHERE exists_ IS NOT NULL)) AS logic_) AND (SELECT LOGICAL_AND(logic_)
FROM UNNEST(ARRAY(SELECT comparison_
FROM (SELECT ((SELECT procedure_element_.date
FROM UNNEST(procedure) AS procedure_element_ WITH OFFSET AS element_offset
LIMIT 1) >= '2012-01-01') AS comparison_)
WHERE comparison_ IS NOT NULL)) AS logic_) AND (SELECT LOGICAL_AND(logic_)
FROM UNNEST(ARRAY(SELECT comparison_
FROM (SELECT ((SELECT procedure_element_.date
FROM UNNEST(procedure) AS procedure_element_ WITH OFFSET AS element_offset
LIMIT 1) < '2013-01-01') AS comparison_)
WHERE comparison_ IS NOT NULL)) AS logic_)""",
        self.runner.to_sql(eob_outpatient_proc_codes_2012),
    )

  def testFHIRViewsExample_outPatientDiagnoses_succeeds(self):
    outpatient_claims_valueset = (
        r4.value_set('urn:example:valueset:outpatient_claims')
        .with_codes(
            'https://bluebutton.cms.gov/resources/codesystem/eob-type',
            ['OUTPATIENT'],
        )
        .build()
    )

    principal_diagnosis_valueset = (
        r4.value_set('urn:example:valueset:principal_diagnosis')
        .with_codes(
            'http://terminology.hl7.org/CodeSystem/ex-diagnosistype',
            ['principal'],
        )
        .build()
    )

    eob = self._views.view_of('ExplanationOfBenefit')
    is_principal_diagnosis = eob.diagnosis.type.memberOf(
        principal_diagnosis_valueset
    ).anyTrue()
    coding = self._views.expression_for('Coding')

    eob_outpatient_codes = eob.select({
        'eob_id': eob.id,
        'patient': eob.patient.idFor('Patient'),
        # Gets the first principal diagnosis's ICD-10 Code
        'principal_diagnosis_icd10': (
            eob.diagnosis.where(is_principal_diagnosis)
            .first()
            .diagnosis.ofType('CodeableConcept')
            .coding.where(coding.system == 'http://hl7.org/fhir/sid/icd-10')
            .code
        ),
    }).where(
        eob.type.memberOf(outpatient_claims_valueset), is_principal_diagnosis
    )

    self.assertMultiLineEqual(
        """WITH VALUESET_VIEW AS (SELECT "urn:example:valueset:principal_diagnosis" as valueseturi, NULL as valuesetversion, "http://terminology.hl7.org/CodeSystem/ex-diagnosistype" as system, "principal" as code
UNION ALL SELECT "urn:example:valueset:outpatient_claims" as valueseturi, NULL as valuesetversion, "https://bluebutton.cms.gov/resources/codesystem/eob-type" as system, "OUTPATIENT" as code
UNION ALL SELECT "urn:example:valueset:principal_diagnosis" as valueseturi, NULL as valuesetversion, "http://terminology.hl7.org/CodeSystem/ex-diagnosistype" as system, "principal" as code)
SELECT (SELECT id) AS eob_id,(SELECT patient.patientId AS idFor_) AS patient,(SELECT coding_element_.code
FROM (SELECT diagnosis_element_.diagnosis.CodeableConcept AS ofType_
FROM UNNEST(diagnosis) AS diagnosis_element_ WITH OFFSET AS element_offset
WHERE (SELECT LOGICAL_OR(
memberof_) AS _anyTrue
FROM (SELECT matches.element_offset IS NOT NULL AS memberof_
FROM (SELECT element_offset
FROM UNNEST(type) AS type_element_ WITH OFFSET AS element_offset) AS all_
LEFT JOIN (SELECT element_offset
FROM UNNEST(ARRAY(SELECT element_offset FROM (
SELECT DISTINCT element_offset
FROM UNNEST(type) AS type_element_ WITH OFFSET AS element_offset,
UNNEST(type_element_.coding) AS codings
INNER JOIN `VALUESET_VIEW` vs ON
vs.valueseturi='urn:example:valueset:principal_diagnosis'
AND vs.system=codings.system
AND vs.code=codings.code
))) AS element_offset
) AS matches
ON all_.element_offset=matches.element_offset
ORDER BY all_.element_offset))
LIMIT 1),
UNNEST(ofType_.coding) AS coding_element_ WITH OFFSET AS element_offset
WHERE (system = 'http://hl7.org/fhir/sid/icd-10')) AS principal_diagnosis_icd10,(SELECT patient.patientId AS idFor_) AS __patientId__ FROM `test_project.test_dataset`.ExplanationOfBenefit
WHERE (SELECT LOGICAL_AND(logic_)
FROM UNNEST(ARRAY(SELECT memberof_
FROM (SELECT memberof_
FROM UNNEST((SELECT IF(type IS NULL, [], [
EXISTS(
SELECT 1
FROM UNNEST(type.coding) AS codings
INNER JOIN `VALUESET_VIEW` vs ON
vs.valueseturi='urn:example:valueset:outpatient_claims'
AND vs.system=codings.system
AND vs.code=codings.code
)]))) AS memberof_)
WHERE memberof_ IS NOT NULL)) AS logic_) AND (SELECT LOGICAL_AND(logic_)
FROM UNNEST(ARRAY(SELECT _anyTrue
FROM (SELECT LOGICAL_OR(
memberof_) AS _anyTrue
FROM (SELECT matches.element_offset IS NOT NULL AS memberof_
FROM (SELECT element_offset
FROM (SELECT diagnosis_element_
FROM UNNEST(diagnosis) AS diagnosis_element_ WITH OFFSET AS element_offset),
UNNEST(diagnosis_element_.type) AS type_element_ WITH OFFSET AS element_offset) AS all_
LEFT JOIN (SELECT element_offset
FROM UNNEST(ARRAY(SELECT element_offset FROM (
SELECT DISTINCT element_offset
FROM (SELECT diagnosis_element_
FROM UNNEST(diagnosis) AS diagnosis_element_ WITH OFFSET AS element_offset),
UNNEST(diagnosis_element_.type) AS type_element_ WITH OFFSET AS element_offset,
UNNEST(type_element_.coding) AS codings
INNER JOIN `VALUESET_VIEW` vs ON
vs.valueseturi='urn:example:valueset:principal_diagnosis'
AND vs.system=codings.system
AND vs.code=codings.code
))) AS element_offset
) AS matches
ON all_.element_offset=matches.element_offset
ORDER BY all_.element_offset))
WHERE _anyTrue IS NOT NULL)) AS logic_)""",
        self.runner.to_sql(eob_outpatient_codes),
    )


def _BqValuesetCodesTable(name: str) -> bigquery.table.Table:
  """Builds a BigQuery client table representation of a value set codes table."""
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

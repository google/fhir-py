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
"""Tests for runner_utils."""

import textwrap

import numpy
import pandas as pd

from absl.testing import absltest
from google.fhir.core.fhir_path import _bigquery_interpreter
from google.fhir.core.fhir_path import _spark_interpreter
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import fhir_path
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import r4_package
from google.fhir.views import r4
from google.fhir.views import runner_utils


class RunnerUtilsTest(absltest.TestCase):
  """Tests runner utils across all interpreters."""

  _fhir_package: fhir_package.FhirPackage

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._fhir_package = r4_package.load_base_r4()

  def setUp(self):
    super().setUp()
    self._context = context.LocalFhirPathContext(self._fhir_package)
    self._views = r4.from_definitions(self._context)

  def test_build_sql_statement_big_query_sql_interpreter(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.active)
    encoder = _bigquery_interpreter.BigQuerySqlInterpreter(
        value_set_codes_table='VALUESET_VIEW'
    )
    sql_statement = runner_utils.RunnerSqlGenerator(
        view=simple_view,
        encoder=encoder,
        dataset='test_dataset',
        table_names={
            'http://hl7.org/fhir/StructureDefinition/Patient': 'Patient'
        },
    ).build_sql_statement(True)
    expected_output = textwrap.dedent(
        """\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,(SELECT SAFE_CAST(birthDate AS TIMESTAMP) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT active
        FROM (SELECT active)
        WHERE active IS NOT NULL)) AS logic_)"""
    )
    self.assertMultiLineEqual(expected_output, sql_statement)

  def test_build_sql_statement_fhir_path_sql_interpreter(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.active)
    url = list(simple_view.get_structdef_urls())[0]
    struct_def = self._context.get_structure_definition(url)
    deps = self._context.get_dependency_definitions(url)
    deps.append(struct_def)

    encoder = fhir_path.FhirPathStandardSqlEncoder(
        deps,
        options=fhir_path.SqlGenerationOptions(
            value_set_codes_table='VALUESET_VIEW'
        ),
    )
    sql_statement = runner_utils.RunnerSqlGenerator(
        view=simple_view,
        encoder=encoder,
        dataset='test_dataset',
        table_names={
            'http://hl7.org/fhir/StructureDefinition/Patient': 'Patient'
        },
    ).build_sql_statement(True)
    expected_output = textwrap.dedent(
        """\
        SELECT ARRAY(SELECT given_element_
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM UNNEST(name) AS name_element_ WITH OFFSET AS element_offset),
        UNNEST(name_element_.given) AS given_element_ WITH OFFSET AS element_offset)
        WHERE given_element_ IS NOT NULL) AS name,PARSE_DATE("%Y-%m-%d", (SELECT birthDate)) AS birthDate,(SELECT id) AS __patientId__ FROM `test_dataset`.Patient
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT active
        FROM (SELECT active)
        WHERE active IS NOT NULL)) AS logic_)"""
    )
    self.assertMultiLineEqual(expected_output, sql_statement)

  def test_build_sql_statement_spark_sql_interpreter(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.active)
    encoder = _spark_interpreter.SparkSqlInterpreter()
    sql_statement = runner_utils.RunnerSqlGenerator(
        view=simple_view,
        encoder=encoder,
        dataset='test_dataset',
        table_names={
            'http://hl7.org/fhir/StructureDefinition/Patient': 'Patient'
        },
    ).build_sql_statement(True)
    expected_output = textwrap.dedent(
        """\
        SELECT (SELECT COLLECT_LIST(given_element_)
        FROM (SELECT given_element_
        FROM (SELECT name_element_
        FROM (SELECT EXPLODE(name_element_) AS name_element_ FROM (SELECT name AS name_element_))) LATERAL VIEW POSEXPLODE(name_element_.given) AS index_given_element_, given_element_)
        WHERE given_element_ IS NOT NULL) AS name,(SELECT CAST(birthDate AS TIMESTAMP) AS birthDate) AS birthDate,(SELECT id) AS __patientId__ FROM `test_dataset`.Patient
        WHERE (SELECT EXISTS(*, x -> x IS true) FROM (SELECT COLLECT_LIST(active)
        FROM (SELECT active)
        WHERE active IS NOT NULL))"""
    )
    self.assertMultiLineEqual(expected_output, sql_statement)

  def test_build_sql_statement_for_mixed_resource_builder(self):
    enc = self._views.view_of('Encounter')
    pat = self._views.view_of('Patient')

    enc_and_pat_view = enc.select(
        {
            'pat': (
                pat.contact.first().relationship.first().text
                == enc.class_.first().display
            )
        }
    ).where(enc.statusHistory.period.start > pat.contact.first().period.start)
    encoder = _bigquery_interpreter.BigQuerySqlInterpreter(
        value_set_codes_table='VALUESET_VIEW'
    )
    sql_statement = runner_utils.RunnerSqlGenerator(
        view=enc_and_pat_view,
        encoder=encoder,
        dataset='test_dataset',
        table_names={
            'http://hl7.org/fhir/StructureDefinition/Patient': 'Patient',
            'http://hl7.org/fhir/StructureDefinition/Encounter': 'Encounter'
        },
    ).build_sql_statement(True)

    expected_output = textwrap.dedent("""\
          SELECT *, (SELECT ((SELECT relationship_element_.text
          FROM (SELECT contact_element_
          FROM (SELECT Patient),
          UNNEST(Patient.contact) AS contact_element_ WITH OFFSET AS element_offset
          LIMIT 1),
          UNNEST(contact_element_.relationship) AS relationship_element_ WITH OFFSET AS element_offset
          LIMIT 1) = Encounter.class.display) AS eq_) AS pat FROM (SELECT * , __patientId__ FROM
          ((SELECT (SELECT subject.patientId AS idFor_) AS __patientId__,Encounter FROM `test_dataset`.Encounter Encounter)
          INNER JOIN
          (SELECT (SELECT id) AS __patientId__,Patient FROM `test_dataset`.Patient Patient)
          USING(__patientId__)))
          WHERE (SELECT LOGICAL_AND(logic_)
          FROM UNNEST(ARRAY(SELECT comparison_
          FROM (SELECT ((SELECT SAFE_CAST(statusHistory_element_.period.start AS TIMESTAMP) AS start
          FROM (SELECT Encounter),
          UNNEST(Encounter.statusHistory) AS statusHistory_element_ WITH OFFSET AS element_offset) > (SELECT SAFE_CAST(contact_element_.period.start AS TIMESTAMP) AS start
          FROM (SELECT Patient),
          UNNEST(Patient.contact) AS contact_element_ WITH OFFSET AS element_offset
          LIMIT 1)) AS comparison_)
          WHERE comparison_ IS NOT NULL)) AS logic_)""")
    self.assertMultiLineEqual(expected_output, sql_statement)

  def test_build_sql_statement_for_mixed_resource_builder_select_base_builder(
      self,
  ):
    enc = self._views.view_of('Encounter')
    pat = self._views.view_of('Patient')

    enc_and_pat_class = enc.where(
        enc.statusHistory.period.start > pat.contact.first().period.start
    )

    encoder = _bigquery_interpreter.BigQuerySqlInterpreter(
        value_set_codes_table='VALUESET_VIEW'
    )
    sql_statement = runner_utils.RunnerSqlGenerator(
        view=enc_and_pat_class,
        encoder=encoder,
        dataset='test_dataset',
        table_names={
            'http://hl7.org/fhir/StructureDefinition/Patient': 'Patient',
            'http://hl7.org/fhir/StructureDefinition/Encounter': 'Encounter'
        },
    ).build_sql_statement(True)

    expected_output = textwrap.dedent("""\
        SELECT * , __patientId__ FROM
        ((SELECT (SELECT subject.patientId AS idFor_) AS __patientId__,Encounter FROM `test_dataset`.Encounter Encounter)
        INNER JOIN
        (SELECT (SELECT id) AS __patientId__,Patient FROM `test_dataset`.Patient Patient)
        USING(__patientId__))
        WHERE (SELECT LOGICAL_AND(logic_)
        FROM UNNEST(ARRAY(SELECT comparison_
        FROM (SELECT ((SELECT SAFE_CAST(statusHistory_element_.period.start AS TIMESTAMP) AS start
        FROM (SELECT Encounter),
        UNNEST(Encounter.statusHistory) AS statusHistory_element_ WITH OFFSET AS element_offset) > (SELECT SAFE_CAST(contact_element_.period.start AS TIMESTAMP) AS start
        FROM (SELECT Patient),
        UNNEST(Patient.contact) AS contact_element_ WITH OFFSET AS element_offset
        LIMIT 1)) AS comparison_)
        WHERE comparison_ IS NOT NULL)) AS logic_)""")
    self.assertMultiLineEqual(expected_output, sql_statement)

  def test_build_valueset_expression(self):
    pat = self._views.view_of('Patient')
    simple_view = pat.select(
        {'name': pat.name.given, 'birthDate': pat.birthDate}
    ).where(pat.maritalStatus.memberOf('http://a-value.set/id'))
    encoder = _bigquery_interpreter.BigQuerySqlInterpreter(
        value_set_codes_table='VALUESET_VIEW'
    )
    sql_statement = runner_utils.RunnerSqlGenerator(
        view=simple_view,
        encoder=encoder,
        dataset='test_dataset',
        table_names={
            'http://hl7.org/fhir/StructureDefinition/Patient': 'Patient'
        },
    ).build_valueset_expression('VALUESET_VIEW')
    expected_output = textwrap.dedent("""\
        WITH VALUESET_VIEW AS (SELECT valueseturi, valuesetversion, system, code FROM VALUESET_VIEW)
        """)
    self.assertMultiLineEqual(expected_output, sql_statement)

  def test_build_select_for_summarize_code_succeeds(self):
    """Tests summarizing codes."""
    observation = self._views.view_of('Observation')
    encoder = _bigquery_interpreter.BigQuerySqlInterpreter(
        value_set_codes_table='VALUESET_VIEW'
    )
    returned_sql = runner_utils.RunnerSqlGenerator(
        view=observation,
        encoder=encoder,
        dataset='test_project.test_dataset',
        table_names={
            'http://hl7.org/fhir/StructureDefinition/Observation': 'Observation'
        },
    ).build_select_for_summarize_code(observation.category)

    # Ensure expected SQL was passed to BigQuery and the dataframe was returned
    # up the stack.
    expected_sql = (
        'SELECT ARRAY(SELECT category_element_\n'
        'FROM (SELECT category_element_\nFROM UNNEST(category) AS '
        'category_element_ WITH OFFSET AS element_offset)\n'
        'WHERE category_element_ IS NOT NULL) as target FROM '
        '`test_project.test_dataset`.Observation'
    )
    self.assertEqual(expected_sql, returned_sql)

  def test_clean_data_frame_for_patient_succeeds(self):
    """Test to_dataframe()."""
    patient = self._views.view_of('Patient')
    simple_view = patient.select(
        {'gender': patient.gender, 'birthDate': patient.birthDate}
    )
    expected_df = pd.DataFrame({
        'gender': ['male', 'male', 'female', 'female', 'male'],
        'birthDate': [
            '2002-01-01',
            '2009-08-02',
            '2016-10-17',
            '2017-05-23',
            '2003-01-17',
        ],
    })

    returned_df = runner_utils.clean_dataframe(
        expected_df, simple_view.get_select_expressions()
    )
    self.assertTrue(expected_df.equals(returned_df))

  def test_clean_data_frame_trims_struct_with_select_succeeds(self):
    """Test structure trimming of explicitly defined columns."""
    patient = self._views.view_of('Patient')
    simple_view = patient.select({
        'name': patient.name.given,
        'address': patient.address,
        'maritalStatus': patient.maritalStatus,
    })
    fake_df = pd.DataFrame.from_dict({
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

    returned_df = runner_utils.clean_dataframe(
        fake_df, simple_view.get_select_expressions()
    )

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

  def test_clean_data_frame_trims_struct_no_select_succeeds(self):
    """Test the base query directly to ensure struct trimming logic works."""
    fake_df = pd.DataFrame.from_dict({
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

    returned_df = runner_utils.clean_dataframe(
        fake_df, self._views.view_of('Patient').get_select_expressions()
    )

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


if __name__ == '__main__':
  absltest.main()

# Copyright 2023 Google LLC
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
"""Tests for spark_value_set_manager."""

from unittest import mock

from sqlalchemy import engine

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import context
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import r4_package
from google.fhir.r4.terminology import terminology_service_client
from google.fhir.r4.terminology import value_set_tables
from google.fhir.r4.terminology import value_sets
from google.fhir.views import spark_value_set_manager


class SparkValueSetManagerTest(parameterized.TestCase):
  _fhir_package: fhir_package.FhirPackage

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._fhir_package = r4_package.load_base_r4()

  def setUp(self):
    super().setUp()
    self.addCleanup(mock.patch.stopall)
    self.mock_spark_engine = mock.create_autospec(engine.Engine, instance=True)
    self.value_set_manager = spark_value_set_manager.SparkValueSetManager(
        self.mock_spark_engine,
        value_set_codes_table='vs_table',
    )
    self._context = context.LocalFhirPathContext(self._fhir_package)

  def test_init_with_value_set_table_as(self):
    manager = spark_value_set_manager.SparkValueSetManager(
        self.mock_spark_engine,
        value_set_codes_table='table',
    )
    self.assertEqual(manager.value_set_codes_table, 'table')

  @mock.patch.object(
      value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True,
  )
  def test_materialize_value_set_with_value_set_object_inserts_data(
      self, mock_valueset_codes_insert_statement_for
  ):
    mock_value_sets = [mock.MagicMock(), mock.MagicMock()]
    mock_insert_statements = [mock.MagicMock(), mock.MagicMock()]
    mock_valueset_codes_insert_statement_for.return_value = (
        mock_insert_statements
    )
    self.mock_spark_engine.execute.return_value = (
        'vs_table'
    )

    self.value_set_manager.materialize_value_sets(mock_value_sets)

    # Ensure we tried to create the table
    self.assertEqual(self.mock_spark_engine.connect.call_count, 3)

    # Ensure we called query with the rendered SQL for the two mock queries
    self.mock_spark_engine.connect.return_value.__enter__.return_value.execute.assert_has_calls([
        mock.call(
            'CREATE TABLE IF NOT EXISTS `default`.`vs_table`'
            ' (valueseturi string, valuesetversion String, system String, code'
            ' String)'
        ),
        mock.call(mock_insert_statements[0]),
        mock.call(mock_insert_statements[1]),
    ])

    # Ensure we called valueset_codes_insert_statement_for with the
    # given value sets.
    args, kwargs = mock_valueset_codes_insert_statement_for.call_args_list[0]
    expanded_value_sets, table = args
    self.assertEqual(list(expanded_value_sets), mock_value_sets)

    self.assertEqual(table.name, 'vs_table')
    for col in ('valueseturi', 'valuesetversion', 'system', 'code'):
      self.assertIn(col, table.columns)
    self.assertEqual(kwargs['batch_size'], 500)

  @mock.patch.object(
      value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True,
  )
  def test_materialize_value_set_expansion_with_value_set_urls_performs_expansions_and_inserts(
      self, mock_valueset_codes_insert_statement_for
  ):
    mock_insert_statements = [mock.MagicMock(), mock.MagicMock()]
    mock_valueset_codes_insert_statement_for.return_value = (
        mock_insert_statements
    )
    mock_expander = mock.MagicMock()

    self.value_set_manager.materialize_value_set_expansion(
        ['url-1', 'url-2'], mock_expander
    )

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
    self.assertEqual(table.name, 'vs_table')
    for col in ('valueseturi', 'valuesetversion', 'system', 'code'):
      self.assertIn(col, table.columns)
    self.assertEqual(kwargs['batch_size'], 500)

    # Ensure we call expand_value_set_url with the two URLs.
    mock_expander.expand_value_set_url.reset()
    mock_expander.expand_value_set_url.assert_has_calls(
        [mock.call('url-1'), mock.call('url-2')]
    )

  @mock.patch.object(
      value_set_tables,
      'valueset_codes_insert_statement_for',
      autospec=True,
  )
  def test_materialize_value_set_expansion_with_terminology_service_url_uses_given_terminology_service_url(
      self, mock_valueset_codes_insert_statement_for
  ):
    mock_expander = mock.MagicMock(
        spec=terminology_service_client.TerminologyServiceClient
    )
    self.mock_spark_engine.execute.return_value = (
        'vs_table'
    )

    self.value_set_manager.materialize_value_set_expansion(
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
    self.assertEqual(table.name, 'vs_table')

    # Ensure we call expand_value_set_url_using_service with the right URLs.
    mock_expander.expand_value_set_url.reset()
    mock_expander.expand_value_set_url_using_service.assert_has_calls([
        mock.call('url-1', 'http://my-service.com'),
        mock.call('url-2', 'http://my-service.com'),
    ])

  def test_materialize_value_set_expansion_with_terminology_service_url_and_value_set_resolver_raises_error(
      self,
  ):
    mock_expander = mock.MagicMock(spec=value_sets.ValueSetResolver)

    with self.assertRaises(TypeError):
      self.value_set_manager.materialize_value_set_expansion(
          ['url-1', 'url-2'],
          mock_expander,
          terminology_service_url='http://my-service.com',
      )

  def test_create_valuset_codes_table_if_not_exists_calls_client_correctly(
      self,
  ):
    self.value_set_manager._create_valueset_codes_table_if_not_exists()

    expected_sql = (
        'CREATE TABLE IF NOT EXISTS `default`.`vs_table`'
        ' (valueseturi string, valuesetversion String, system String, code'
        ' String)'
    )
    self.mock_spark_engine.connect.return_value.__enter__.return_value.execute.assert_called_once_with(
        expected_sql
    )


if __name__ == '__main__':
  absltest.main()

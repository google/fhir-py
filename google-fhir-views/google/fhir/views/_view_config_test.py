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
"""Tests for view_config."""

from absl.testing import absltest
from google.fhir.core.fhir_path import context
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import r4_package
from google.fhir.views import _view_config
from google.fhir.views import r4


class ConfigTest(absltest.TestCase):
  """Tests all attributes in the ColumnExpressionBuilder."""

  _fhir_package: fhir_package.FhirPackage

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._fhir_package = r4_package.load_base_r4()

  def setUp(self):
    super().setUp()
    self._context = context.LocalFhirPathContext(self._fhir_package)
    self._handler = r4.from_definitions(self._context)._handler
    self._patient_view = r4.from_definitions(self._context).view_of('Patient')

  def test_valid_config(self):
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'patient_id', 'path': 'id'},
            {'alias': 'birth_date', 'path': 'birthDate'},
        ],
    }
    config = _view_config.ViewConfig(
        self._context, self._handler, view_definition
    )
    self.assertEqual(config.resource, 'Patient')
    self.assertEqual(
        repr(config.column_builders[0]),
        'ColumnExpressionBuilder("id.alias(patient_id)")',
    )
    self.assertEqual(
        repr(config.column_builders[1]),
        'ColumnExpressionBuilder("birthDate.alias(birth_date)")',
    )

  def test_config_without_resource_raises_error(self):
    view_definition = {
        'select': [
            {'alias': 'patient_id', 'path': 'id'},
            {'alias': 'birth_date', 'path': 'birthDate'},
        ],
    }
    with self.assertRaises(KeyError):
      _view_config.ViewConfig(self._context, self._handler, view_definition)

  def test_config_without_select_raises_error(self):
    view_definition = {
        'resource': 'Patient',
    }
    with self.assertRaises(KeyError):
      _view_config.ViewConfig(self._context, self._handler, view_definition)

  def test_config_with_non_string_select_alias_raises_error(self):
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 100, 'path': 'id'},
        ],
    }
    with self.assertRaises(ValueError):
      _view_config.ViewConfig(self._context, self._handler, view_definition)

  def test_config_with_non_string_select_path_raises_error(self):
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'patient_id', 'path': 100},
        ],
    }
    with self.assertRaises(ValueError):
      _view_config.ViewConfig(self._context, self._handler, view_definition)

  def test_config_with_not_alias_path_select_raises_error(self):
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'from': 'patient_id', 'select': 'id'},
        ],
    }
    with self.assertRaises(NotImplementedError):
      _view_config.ViewConfig(self._context, self._handler, view_definition)

  def test_valid_config_with_where(self):
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'patient_id', 'path': 'id'},
            {'alias': 'birth_date', 'path': 'birthDate'},
        ],
        'where': [{'path': 'birthDate < @1990-01-01'}],
    }
    config = _view_config.ViewConfig(
        self._context, self._handler, view_definition
    )
    self.assertLen(config.constraint_builders, 1)
    self.assertEqual(
        repr(config.constraint_builders[0]),
        'ColumnExpressionBuilder("birthDate < @1990-01-01")',
    )

  def test_config_with_where_without_path_raises_error(self):
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'patient_id', 'path': 'id'},
            {'alias': 'birth_date', 'path': 'birthDate'},
        ],
        'where': [{'description': 'birthDate < @1990-01-01'}],
    }
    with self.assertRaises(KeyError):
      _view_config.ViewConfig(self._context, self._handler, view_definition)

  def test_config_with_non_string_where_path_raises_error(self):
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'patient_id', 'path': 'id'},
            {'alias': 'birth_date', 'path': 'birthDate'},
        ],
        'where': [{'path': False}],
    }
    with self.assertRaises(ValueError):
      _view_config.ViewConfig(self._context, self._handler, view_definition)


if __name__ == '__main__':
  absltest.main()

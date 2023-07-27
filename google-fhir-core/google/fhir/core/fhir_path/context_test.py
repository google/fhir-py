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
"""Tests for FHIRPath context."""

from typing import BinaryIO

import requests
import requests_mock

from absl.testing import absltest
# TODO(b/229908551): Eliminate R4-specific tests from this package.
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import primitive_handler
from google.fhir.r4 import r4_package

_PATIENT_STRUCTDEF_URL = 'http://hl7.org/fhir/StructureDefinition/Patient'
_PRIMITIVE_HANDLER = primitive_handler.PrimitiveHandler()


class FhirPathContextTest(absltest.TestCase):

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._package = r4_package.load_base_r4()
    us_core = []
    cls._package_manager = fhir_package.FhirPackageManager(
        [cls._package, us_core]
    )

  def test_struct_def_load_from_fhir_package_succeeds(self):
    test_context = context.LocalFhirPathContext(self._package)
    from_unqualified = test_context.get_structure_definition('Patient')
    self.assertEqual(from_unqualified.url.value, _PATIENT_STRUCTDEF_URL)
    from_qualified = test_context.get_structure_definition(
        _PATIENT_STRUCTDEF_URL
    )
    self.assertEqual(from_qualified.url.value, _PATIENT_STRUCTDEF_URL)

  def test_struct_def_load_from_fhir_package_manager_succeeds(self):
    test_context = context.LocalFhirPathContext(self._package_manager)
    from_unqualified = test_context.get_structure_definition('Patient')
    self.assertEqual(from_unqualified.url.value, _PATIENT_STRUCTDEF_URL)
    from_qualified = test_context.get_structure_definition(
        _PATIENT_STRUCTDEF_URL
    )
    self.assertEqual(from_qualified.url.value, _PATIENT_STRUCTDEF_URL)

  def test_stuct_def_load_missing_resource_from_fhir_package_fails(self):
    test_context = context.LocalFhirPathContext(self._package)

    with self.assertRaisesRegex(
        context.UnableToLoadResourceError, '.*BogusResource.*'
    ):
      test_context.get_structure_definition('BogusResource')

  def test_struct_def_load_dependencies_as_expected(self):
    test_context = context.LocalFhirPathContext(self._package)
    dependencies = test_context.get_dependency_definitions('Observation')
    dependency_urls = set([dep.url.value for dep in dependencies])

    # Ensure direct and transitive dependencies are present.
    self.assertContainsSubset(
        {
            'http://hl7.org/fhir/StructureDefinition/Range',
            'http://hl7.org/fhir/StructureDefinition/CodeableConcept',
            'http://hl7.org/fhir/StructureDefinition/Coding',
        },
        dependency_urls,
    )

  def test_get_fhir_type_of_reference_element_succeeds(self):
    test_context = context.LocalFhirPathContext(self._package)
    observation = test_context.get_structure_definition(
        'http://hl7.org/fhir/StructureDefinition/Observation',
    )
    obs_type = _fhir_path_data_types.StructureDataType.from_proto(observation)

    # Ensure component.referenceRange follows the reference to the actual
    # referenceRange backbone element.
    component = test_context.get_child_data_type(obs_type, 'component')
    ref_range = test_context.get_child_data_type(component, 'referenceRange')
    self.assertIsNotNone(ref_range)
    self.assertEqual(
        'http://hl7.org/fhir/StructureDefinition/Observation.referenceRange',
        ref_range.url,
    )

  def test_get_fhir_type_from_string_with_reference_succeeds(self):
    test_context = context.LocalFhirPathContext(self._package)

    # Grab the element definition for Observation.subject, a reference field.
    observation = test_context.get_structure_definition(
        'http://hl7.org/fhir/StructureDefinition/Observation'
    )
    subject = next(
        element
        for element in observation.snapshot.element
        if element.id.value == 'Observation.subject'
    )

    return_type = test_context.get_fhir_type_from_string(
        element_definition=subject,
        profile='http://hl7.org/fhir/StructureDefinition/Reference',
        type_code=None,
    )
    self.assertIsInstance(
        return_type, _fhir_path_data_types.ReferenceStructureDataType
    )
    self.assertCountEqual(
        return_type.target_profiles,
        [
            'http://hl7.org/fhir/StructureDefinition/Patient',
            'http://hl7.org/fhir/StructureDefinition/Group',
            'http://hl7.org/fhir/StructureDefinition/Device',
            'http://hl7.org/fhir/StructureDefinition/Location',
        ],
    )


class ServerFhirPathContextTest(absltest.TestCase):

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._mock_server_address = 'https://mockserver.com'
    cls._test_context = context.ServerFhirPathContext(
        cls._mock_server_address,
        structure_definition_pb2.StructureDefinition,
        _PRIMITIVE_HANDLER,
    )

  def test_struct_def_load_from_fhir_server_succeeds(self):
    patient_url = f'{self._mock_server_address}/StructureDefinition?_id={requests.utils.quote(_PATIENT_STRUCTDEF_URL)}'
    with requests_mock.Mocker() as m:
      m.get(
          patient_url,
          # Bundle with placeholder patient for testing.
          json={
              'resourceType': 'Bundle',
              'id': 'resources',
              'type': 'collection',
              'entry': [
                  {
                      'resource': {
                          'resourceType': 'StructureDefinition',
                          'id': 'Patient',
                          'url': _PATIENT_STRUCTDEF_URL,
                          'name': 'Patient',
                      }
                  }
              ],
          },
      )

      patient_structdef = self._test_context.get_structure_definition('Patient')
      self.assertEqual(patient_structdef.url.value, _PATIENT_STRUCTDEF_URL)

      # Reload the resource to ensure it uses the cached copy.
      m.get(patient_url, status_code=404)
      patient_structdef = self._test_context.get_structure_definition('Patient')
      self.assertEqual(patient_structdef.url.value, _PATIENT_STRUCTDEF_URL)

  def test_struct_def_load_from_fhir_server_not_found(self):
    bad_resource_id = 'http://hl7.org/fhir/StructureDefinition/NoSuchResource'
    no_resource_url = (
        f'{self._mock_server_address}/StructureDefinition?_id={bad_resource_id}'
    )
    with requests_mock.Mocker() as m:
      m.get(no_resource_url, status_code=404)

      with self.assertRaisesRegex(
          context.UnableToLoadResourceError, '.*NoSuchResource.*404.*'
      ):
        self._test_context.get_structure_definition('NoSuchResource')


if __name__ == '__main__':
  absltest.main()

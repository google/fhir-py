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

import requests
import requests_mock

from absl.testing import absltest

# TODO: Eliminate R4-specific tests from this package.
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.core.fhir_path import context
from google.fhir.r4 import primitive_handler
from google.fhir.r4 import r4_package

_PATIENT_STRUCTDEF_URL = 'http://hl7.org/fhir/StructureDefinition/Patient'
_PRIMITIVE_HANDLER = primitive_handler.PrimitiveHandler()


class FhirPathContextTest(absltest.TestCase):

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._package = r4_package.load_base_r4()

  def testStructDefLoad_FromFhirPackage_succeeds(self):
    test_context = context.LocalFhirPathContext.from_resources(
        self._package.structure_definitions)
    from_unqualified = test_context.get_structure_definition('Patient')
    self.assertEqual(from_unqualified.url.value, _PATIENT_STRUCTDEF_URL)
    from_qualified = test_context.get_structure_definition(
        _PATIENT_STRUCTDEF_URL)
    self.assertEqual(from_qualified.url.value, _PATIENT_STRUCTDEF_URL)

  def testStuctDefLoadMissingResource_FromFhirPackage_fails(self):
    test_context = context.LocalFhirPathContext.from_resources(
        self._package.structure_definitions)

    with self.assertRaisesRegex(context.UnableToLoadResourceError,
                                '.*BogusResource.*'):
      test_context.get_structure_definition('BogusResource')

  def testStructDef_LoadDependencies_asExpected(self):
    test_context = context.LocalFhirPathContext.from_resources(
        self._package.structure_definitions)
    dependencies = test_context.get_dependency_definitions('Observation')
    dependency_urls = set([dep.url.value for dep in dependencies])

    # Ensure direct and transitive dependencies are present.
    self.assertContainsSubset(
        {
            'http://hl7.org/fhir/StructureDefinition/Range',
            'http://hl7.org/fhir/StructureDefinition/CodeableConcept',
            'http://hl7.org/fhir/StructureDefinition/Coding'
        }, dependency_urls)


class ServerFhirPathContextTest(absltest.TestCase):

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._mock_server_address = 'https://mockserver.com'
    cls._test_context = context.ServerFhirPathContext(
        cls._mock_server_address, structure_definition_pb2.StructureDefinition,
        _PRIMITIVE_HANDLER)

  def testStructDefLoad_FromFhirServer_succeeds(self):

    patient_url = f'{self._mock_server_address}/StructureDefinition?_id={requests.utils.quote(_PATIENT_STRUCTDEF_URL)}'
    with requests_mock.Mocker() as m:
      m.get(
          patient_url,
          # Bundle with placeholder patient for testing.
          json={
              'resourceType':
                  'Bundle',
              'id':
                  'resources',
              'type':
                  'collection',
              'entry': [{
                  'resource': {
                      'resourceType': 'StructureDefinition',
                      'id': 'Patient',
                      'url': _PATIENT_STRUCTDEF_URL,
                      'name': 'Patient',
                  }
              }]
          })

      patient_structdef = self._test_context.get_structure_definition('Patient')
      self.assertEqual(patient_structdef.url.value, _PATIENT_STRUCTDEF_URL)

      # Reload the resource to ensure it uses the cached copy.
      m.get(patient_url, status_code=404)
      patient_structdef = self._test_context.get_structure_definition('Patient')
      self.assertEqual(patient_structdef.url.value, _PATIENT_STRUCTDEF_URL)

  def testStructDefLoad_FromFhirServer_notFound(self):

    bad_resource_id = 'http://hl7.org/fhir/StructureDefinition/NoSuchResource'
    no_resource_url = f'{self._mock_server_address}/StructureDefinition?_id={bad_resource_id}'
    with requests_mock.Mocker() as m:
      m.get(no_resource_url, status_code=404)

      with self.assertRaisesRegex(context.UnableToLoadResourceError,
                                  '.*NoSuchResource.*404.*'):
        self._test_context.get_structure_definition('NoSuchResource')


if __name__ == '__main__':
  absltest.main()

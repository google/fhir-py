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

"""Testing functionality for the fhir_client."""

import unittest.mock

from absl.testing import absltest
from google.fhir.r4.proto.core import codes_pb2
from google.fhir.r4.proto.core import datatypes_pb2
from google.fhir.r4.proto.core.resources import bundle_and_contained_resource_pb2
from google.fhir.r4.fhir_client import _fhir_client
from google.fhir.r4.fhir_client import fhir_client


class ClientTest(absltest.TestCase):

  @unittest.mock.patch.object(_fhir_client.requests, 'Session', autospec=True)
  def testSearch_withSimpleQuery_returnsExpectedProto(self, mock_session_get):
    mock_session_get().headers = {}
    fhir_resource_response = {
        'resourceType': 'Bundle',
        'id': '12345',
        'type': 'transaction',
    }
    mock_session_get().get('url').json.return_value = fhir_resource_response

    client = fhir_client.Client(
        base_url='http://base.url.org',
        basic_auth=('user', 'pwd'),
    )
    result = client.search('Patient?_count=10')

    type_code = bundle_and_contained_resource_pb2.Bundle.TypeCode(
        value=codes_pb2.BundleTypeCode.Value.TRANSACTION
    )
    bundle = bundle_and_contained_resource_pb2.Bundle(
        id=datatypes_pb2.Id(value='12345'), type=type_code
    )
    self.assertEqual(result, bundle)

  @unittest.mock.patch.object(_fhir_client.requests, 'Session', autospec=True)
  def testSearch_withFloatValuesInResponse_doesNotThrowError(
      self, mock_session_get
  ):
    mock_session_get().headers = {}
    fhir_resource_response = {
        'resourceType': 'Bundle',
        'id': '12345',
        'type': 'transaction',
        'entry': [{
            'resource': {
                'resourceType': 'Observation',
                'status': 'final',
                'code': {},
                'valueQuantity': {
                    'value': 66.0,
                },
            }
        }],
    }
    mock_session_get().get('url').json.return_value = fhir_resource_response

    client = fhir_client.Client(
        base_url='http://base.url.org',
        basic_auth=('user', 'pwd'),
    )
    result = client.search('Observation?_count=1')

    self.assertEqual(
        result.entry[0].resource.observation.value.quantity.value.value, '66.0'
    )


if __name__ == '__main__':
  absltest.main()

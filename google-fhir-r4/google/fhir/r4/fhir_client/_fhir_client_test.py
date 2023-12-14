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
from google.fhir.r4.fhir_client import _fhir_client


class FhirClientTest(absltest.TestCase):

  @unittest.mock.patch.object(_fhir_client.requests, 'Session', autospec=True)
  def testSearch_withSimpleQuery_returnsExpectedJson(self, mock_session_get):
    fhir_resource_response = {'resourceType': 'Bundle', 'id': '12345'}
    mock_session_get().headers = {}
    mock_session_get().get('url').json.return_value = fhir_resource_response

    client = _fhir_client.FhirClient(
        base_url='http://base.url.org', basic_auth=('user', 'pwd')
    )
    result = client.search('Patient?_count=10')

    self.assertEqual(result, fhir_resource_response)

  @unittest.mock.patch.object(_fhir_client.requests, 'Session', autospec=True)
  def testSearch_withAuth_setsAuthFieldsInSession(self, mock_session_get):
    mock_session_get().headers = {}
    mock_session_get().get('url').json.return_value = {}

    client = _fhir_client.FhirClient(
        base_url='http://base.url.org', basic_auth=('user', 'pwd')
    )
    client.search('Patient?_count=10')

    self.assertEqual(mock_session_get().headers['Accept'], 'application/json')
    self.assertEqual(mock_session_get().auth, ('user', 'pwd'))

  @unittest.mock.patch.object(_fhir_client.requests, 'Session', autospec=True)
  def testSearch_withoutAuth_doesNotsetAuthAttr(self, mock_session_get):
    mock_session_get().headers = {}
    mock_session_get().get('url').json.return_value = {}

    client = _fhir_client.FhirClient(
        base_url='http://base.url.org', basic_auth=None
    )
    client.search('Patient?_count=10')

    self.assertEqual(mock_session_get().headers['Accept'], 'application/json')
    self.assertEqual(hasattr(mock_session_get(), 'auth'), False)

  @unittest.mock.patch.object(_fhir_client, 'requests', autospec=True)
  def testSessionWithBackoff_withRequests_AddsAdapter(self, mock_requests):
    _fhir_client.FhirClient('url', None).create_session()

    mock_requests.adapters.HTTPAdapter.assert_called_once_with(
        max_retries=mock_requests.packages.urllib3.util.Retry()
    )
    mock_requests.Session().mount.assert_has_calls([
        unittest.mock.call('http://', mock_requests.adapters.HTTPAdapter()),
        unittest.mock.call('https://', mock_requests.adapters.HTTPAdapter()),
    ])


if __name__ == '__main__':
  absltest.main()

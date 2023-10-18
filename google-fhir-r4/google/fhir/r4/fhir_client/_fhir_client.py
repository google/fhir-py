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
"""Provides a client for interacting with FHIR servers."""

import requests


class FhirClient:
  """FhirClient for interacting with FHIR servers.

  Attributes:
    base_url: The base url of the rest service for the FHIR server. The service
      should implement the FHIR search API following HL7 documentation
      https://www.hl7.org/fhir/search.html.
  """

  def __init__(self, base_url):
    # TODO(b/302104967): Handle auth with FHIR server
    self._base_url = base_url

  def search(self, search_query):
    """Make a search request to the FHIR server."""
    url = f'{self._base_url}/{search_query}'
    resp = self.create_session().get(url)
    return resp.json()

  @classmethod
  def create_session(cls) -> requests.Session:
    """Builds a request session with exponential back-off retries."""
    session = requests.Session()
    retry_policy = requests.packages.urllib3.util.Retry(backoff_factor=2)
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_policy)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

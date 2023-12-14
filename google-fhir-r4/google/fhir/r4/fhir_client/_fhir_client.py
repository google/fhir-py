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
from typing import Tuple, Optional

import requests


class FhirClient:
  """FhirClient for interacting with FHIR servers.

  Attributes:
    base_url: The base url of the rest service for the FHIR server. The service
      should implement the FHIR search API following HL7 documentation
      https://www.hl7.org/fhir/search.html.
    basic_auth: A tuple of (user_name, password) to use when performing basic
      auth with the FHIR service or None if no authentication is required.
  """

  def __init__(self, base_url, basic_auth: Optional[Tuple[str, str]]):
    self._basic_auth = basic_auth
    self._base_url = base_url

  def search(self, search_query):
    """Make a search request to the FHIR server."""
    url = f'{self._base_url}/{search_query}'

    session = self.create_session()
    session.headers.update({'Accept': 'application/json'})
    if self._basic_auth is not None:
      session.auth = self._basic_auth

    resp = session.get(url)
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

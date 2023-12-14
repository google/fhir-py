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
"""Provides a client for interacting with FHIR servers returning the protobuf representations of FHIR."""


from google.fhir.r4.proto.core.resources import bundle_and_contained_resource_pb2
from google.fhir.r4 import json_format
from google.fhir.r4.fhir_client import _fhir_client


class Client(_fhir_client.FhirClient):
  """Client for interacting with FHIR servers.

  Attributes:
    base_url: The base url of the rest service for the FHIR server. The service
      should implement the FHIR search API following HL7 documentation
      https://www.hl7.org/fhir/search.html.
    basic_auth: A tuple of (user_name, password) to use when performing basic
      auth with the FHIR service or None if no authentication is required.
  """

  def search(self, search_query):
    """Make a search request to the FHIR server and convert response to protobuf."""
    resp_json = super().search(search_query)
    return json_format.json_fhir_object_to_proto(
        resp_json, bundle_and_contained_resource_pb2.Bundle
    )

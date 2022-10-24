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
"""Support for creating views of FHIR R4 resources.

See the views module for details on use.
"""

from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.r4 import primitive_handler
from google.fhir.r4 import r4_package
from google.fhir.views import views

_PRIMITIVE_HANDLER = primitive_handler.PrimitiveHandler()

# FhirPathContext using FHIR R4 resources.
R4FhirPathContext = context.FhirPathContext[
    structure_definition_pb2.StructureDefinition, value_set_pb2.ValueSet]


def value_set(uri: str) -> expressions.ValueSetBuilder:
  """Returns a builder to easily build FHIR value sets."""
  return expressions.ValueSetBuilder(uri, value_set_pb2.ValueSet())


def base_r4() -> views.Views:
  """Returns a Views instance using the base FHIR R4 structure definitions."""
  package = r4_package.load_base_r4()
  fhir_context = context.LocalFhirPathContext(package)
  return from_definitions(fhir_context)


def from_definitions(fhir_context: R4FhirPathContext) -> views.Views:
  """Returns a Views instance that loads resources from the given context."""
  return views.Views(fhir_context, _PRIMITIVE_HANDLER)


def from_fhir_package(package_path: str) -> views.Views:
  """Returns a Views instance that loads resources from the given on-disk package.
  """
  package = r4_package.load(package_path)
  fhir_context: R4FhirPathContext = context.LocalFhirPathContext(package)
  return from_definitions(fhir_context)


def from_fhir_server(server_base_url: str) -> views.Views:
  """Returns a Views instance that loads resources from the given FHIR server.
  """
  fhir_context: R4FhirPathContext = context.ServerFhirPathContext(
      server_base_url, structure_definition_pb2.StructureDefinition,
      _PRIMITIVE_HANDLER)
  return from_definitions(fhir_context)

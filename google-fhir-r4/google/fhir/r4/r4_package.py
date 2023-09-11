#
# Copyright 2022 Google LLC
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
"""Loader for retrieving FHIR packages based on FHIR R4."""

from typing import BinaryIO, Iterable

from google.fhir.r4.proto.core.resources import code_system_pb2
from google.fhir.r4.proto.core.resources import search_parameter_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import primitive_handler

# pyformat: disable
import io
import os

def _base_core_factory() -> BinaryIO:
  path = os.path.join(
      os.path.abspath(os.path.dirname(__file__)), 'data/hl7.fhir.r4.core.tgz')
  return io.open(path, 'rb')
# pyformat: enable

_PRIMITIVE_HANDLER = primitive_handler.PrimitiveHandler()


def load_base_r4() -> fhir_package.FhirPackage:
  """Returns a `FhirPackage` containing the base R4 profiles."""
  return load(_base_core_factory)


def load(
    archive_file: fhir_package.PackageSource,
) -> fhir_package.FhirPackage[
    structure_definition_pb2.StructureDefinition,
    search_parameter_pb2.SearchParameter,
    code_system_pb2.CodeSystem,
    value_set_pb2.ValueSet,
]:
  """Instantiates and returns a new `FhirPackage` for FHIR R4.

  Args:
    archive_file: The zip or tar file path or a function returning a file-like
      containing resources represented by this collection.

  Returns:
    An instance of `FhirPackage`.

  Raises:
    ValueError: In the event that the file or contents are invalid.
  """
  return fhir_package.FhirPackage.load(
      archive_file,
      _PRIMITIVE_HANDLER,
      structure_definition_pb2.StructureDefinition,
      search_parameter_pb2.SearchParameter,
      code_system_pb2.CodeSystem,
      value_set_pb2.ValueSet,
  )


def from_iterables(
    ig_info: fhir_package.IgInfo,
    structure_definitions: Iterable[
        structure_definition_pb2.StructureDefinition
    ],
    search_parameters: Iterable[search_parameter_pb2.SearchParameter],
    code_systems: Iterable[code_system_pb2.CodeSystem],
    value_sets: Iterable[value_set_pb2.ValueSet],
    resource_time_zone: str = 'Z',
) -> fhir_package.FhirPackage[
    structure_definition_pb2.StructureDefinition,
    search_parameter_pb2.SearchParameter,
    code_system_pb2.CodeSystem,
    value_set_pb2.ValueSet,
]:
  """Builds a FHIR R4 `FhirPackage` containing the given resources.

  Args:
    ig_info: The metadata to associate with the `FhirPackage`.
    structure_definitions: The structure definitions to include in the
      `FhirPackage`.
    search_parameters: The search parameters to include in the `FhirPackage`.
    code_systems: The code systems to include in the `FhirPackage`.
    value_sets: The value sets to include in the `FhirPackage`.
    resource_time_zone: If additional JSON resources are added to the
      `FhirPackage`, the time zone code to parse resource dates into when adding
      those JSON resources.

  Returns:
    A `FhirPackage` instance with the requested resources.
  """
  return fhir_package.FhirPackage(
      ig_info=ig_info,
      structure_definitions=fhir_package.ResourceCollection.from_iterable(
          structure_definitions,
          structure_definition_pb2.StructureDefinition,
          _PRIMITIVE_HANDLER,
          resource_time_zone,
      ),
      search_parameters=fhir_package.ResourceCollection.from_iterable(
          search_parameters,
          search_parameter_pb2.SearchParameter,
          _PRIMITIVE_HANDLER,
          resource_time_zone,
      ),
      code_systems=fhir_package.ResourceCollection.from_iterable(
          code_systems,
          code_system_pb2.CodeSystem,
          _PRIMITIVE_HANDLER,
          resource_time_zone,
      ),
      value_sets=fhir_package.ResourceCollection.from_iterable(
          value_sets,
          value_set_pb2.ValueSet,
          _PRIMITIVE_HANDLER,
          resource_time_zone,
      ),
  )

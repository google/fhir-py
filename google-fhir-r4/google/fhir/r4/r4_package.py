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

from typing import BinaryIO

from google.fhir.r4.proto.core.resources import code_system_pb2
from google.fhir.r4.proto.core.resources import search_parameter_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import primitive_handler

import io
import os

def _base_core_factory() -> BinaryIO:
  path = os.path.join(
      os.path.abspath(os.path.dirname(__file__)), 'data/hl7.fhir.r4.core.tgz')
  return io.open(path, 'rb')

_PRIMITIVE_HANDLER = primitive_handler.PrimitiveHandler()


def load_base_r4() -> fhir_package.FhirPackage:
  """Returns a `FhirPackage` containing the base R4 profiles."""
  return load(_base_core_factory)


def load(archive_file: fhir_package.PackageSource) -> fhir_package.FhirPackage:
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
      archive_file, _PRIMITIVE_HANDLER,
      structure_definition_pb2.StructureDefinition,
      search_parameter_pb2.SearchParameter, code_system_pb2.CodeSystem,
      value_set_pb2.ValueSet)

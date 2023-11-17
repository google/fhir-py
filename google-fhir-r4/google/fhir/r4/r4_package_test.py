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
"""Tests for retrieving FHIR packages based on FHIR R4."""

from typing import Iterable

from absl.testing import absltest
from google.fhir.r4.proto.core.resources import code_system_pb2
from google.fhir.r4.proto.core.resources import search_parameter_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.internal.json_format import _json_parser
from google.fhir.core.utils import fhir_package
from google.fhir.core.utils import fhir_package_test_base
from google.fhir.r4 import primitive_handler
from google.fhir.r4 import r4_package

_R4_DEFINITIONS_COUNT = 655
_R4_CODESYSTEMS_COUNT = 1062
_R4_VALUESETS_COUNT = 1316
_R4_SEARCH_PARAMETERS_COUNT = 1400


class R4FhirPackageTest(fhir_package_test_base.FhirPackageTest):

  @property
  def _primitive_handler(self):
    return primitive_handler.PrimitiveHandler()

  @property
  def _structure_definition_cls(self):
    return structure_definition_pb2.StructureDefinition

  @property
  def _search_parameter_cls(self):
    return search_parameter_pb2.SearchParameter

  @property
  def _code_system_cls(self):
    return code_system_pb2.CodeSystem

  @property
  def _valueset_cls(self):
    return value_set_pb2.ValueSet

  def _load_package(
      self, package_source: fhir_package.PackageSource
  ) -> fhir_package.FhirPackage[
      structure_definition_pb2.StructureDefinition,
      search_parameter_pb2.SearchParameter,
      code_system_pb2.CodeSystem,
      value_set_pb2.ValueSet,
  ]:
    return r4_package.load(package_source)

  def _package_from_iterables(
      self,
      ig_info: fhir_package.IgInfo,
      structure_definitions: Iterable[
          structure_definition_pb2.StructureDefinition
      ],
      search_parameters: Iterable[search_parameter_pb2.SearchParameter],
      code_systems: Iterable[code_system_pb2.CodeSystem],
      value_sets: Iterable[value_set_pb2.ValueSet],
  ) -> fhir_package.FhirPackage[
      structure_definition_pb2.StructureDefinition,
      search_parameter_pb2.SearchParameter,
      code_system_pb2.CodeSystem,
      value_set_pb2.ValueSet,
  ]:
    return r4_package.from_iterables(
        ig_info=ig_info,
        structure_definitions=structure_definitions,
        search_parameters=search_parameters,
        code_systems=code_systems,
        value_sets=value_sets,
    )

  def test_fhir_package_load_with_valid_fhir_package_succeeds(self):
    package = r4_package.load_base_r4()
    self.assertLen(package.structure_definitions, _R4_DEFINITIONS_COUNT)
    self.assertLen(package.code_systems, _R4_CODESYSTEMS_COUNT)
    self.assertLen(package.value_sets, _R4_VALUESETS_COUNT)
    self.assertLen(package.search_parameters, _R4_SEARCH_PARAMETERS_COUNT)


class R4ResourceCollectionTest(fhir_package_test_base.ResourceCollectionTest):

  @property
  def _primitive_handler(self):
    return primitive_handler.PrimitiveHandler()

  @property
  def _parser(self):
    return _json_parser.JsonParser(self._primitive_handler, 'Z')

  @property
  def _valueset_cls(self):
    return value_set_pb2.ValueSet


class FhirPackageManagerTest(fhir_package_test_base.FhirPackageManagerTest):

  @property
  def _structure_definition_cls(self):
    return structure_definition_pb2.StructureDefinition

  @property
  def _search_parameter_cls(self):
    return search_parameter_pb2.SearchParameter

  @property
  def _code_system_cls(self):
    return code_system_pb2.CodeSystem

  @property
  def _valueset_cls(self):
    return value_set_pb2.ValueSet


if __name__ == '__main__':
  absltest.main()

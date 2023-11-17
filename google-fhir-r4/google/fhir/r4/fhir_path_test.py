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
"""Tests for fhir_path."""

from typing import Any

from google.protobuf import descriptor
from absl.testing import absltest
from google.fhir.r4.proto.core.resources import observation_pb2
from google.fhir.r4.proto.core.resources import patient_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.fhir_path import _interpreter_test_base
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.core.fhir_path import python_compiled_expressions
from google.fhir.r4 import fhir_path
from google.fhir.r4 import r4_package


class FhirPathTest(_interpreter_test_base.FhirPathExpressionsTest):
  """FHIR expressions test for FHIR R4. See parent class for common tests."""

  _context: context.LocalFhirPathContext[Any, Any]

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._package = r4_package.load_base_r4()
    cls._context = context.LocalFhirPathContext(cls._package)

  def compile_expression(
      self, structdef_url: str, fhir_path_expression: str
  ) -> python_compiled_expressions.PythonCompiledExpression:
    return fhir_path.compile_expression(
        structdef_url, self._context, fhir_path_expression
    )

  def builder(self, structdef_url: str) -> expressions.Builder:
    return fhir_path.builder(structdef_url, self._context)

  def patient_descriptor(self) -> descriptor.Descriptor:
    return patient_pb2.Patient.DESCRIPTOR

  def observation_descriptor(self) -> descriptor.Descriptor:
    return observation_pb2.Observation.DESCRIPTOR

  def value_set_builder(self, url: str) -> expressions.ValueSetBuilder:
    return expressions.ValueSetBuilder(url, value_set_pb2.ValueSet())

  def context(self) -> context.LocalFhirPathContext[Any, Any]:
    return self._context


if __name__ == '__main__':
  absltest.main()

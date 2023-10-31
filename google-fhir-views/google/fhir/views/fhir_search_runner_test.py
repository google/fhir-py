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
"""Tests for fhir_search_runner."""

from unittest import mock

import pandas as pd

from google.protobuf import symbol_database
from absl.testing import absltest
from google.fhir.r4.proto.core.resources import bundle_and_contained_resource_pb2
from google.fhir.r4.proto.core.resources import observation_pb2
from google.fhir.r4.proto.core.resources import patient_pb2
from google.fhir.core.fhir_path import context
from google.fhir.r4 import r4_package
from google.fhir.r4.fhir_client import fhir_client
from google.fhir.views import fhir_search_runner
from google.fhir.views import r4


class FhirSearchRunnerTest(absltest.TestCase):

  def _new_patient(self):
    return symbol_database.Default().GetPrototype(
        patient_pb2.Patient.DESCRIPTOR
    )()

  def _new_observation(self):
    return symbol_database.Default().GetPrototype(
        observation_pb2.Observation.DESCRIPTOR
    )()

  def _new_bundle(self):
    return symbol_database.Default().GetPrototype(
        bundle_and_contained_resource_pb2.Bundle.DESCRIPTOR
    )()

  def _build_observation_bundle(
      self, observation
  ) -> bundle_and_contained_resource_pb2.Bundle:
    obs = self._new_observation()
    obs.value.quantity.value.value = observation["value"]
    obs.value.quantity.unit.value = observation["unit"]
    obs.code.coding.add().display.value = observation["display"]

    bundle = self._new_bundle()
    bundle.entry.add().resource.observation.CopyFrom(obs)
    return bundle

  def _build_patient_bundle(
      self, patients
  ) -> bundle_and_contained_resource_pb2.Bundle:
    bundle = self._new_bundle()

    for patient_data in patients:
      patient = self._new_patient()
      patient.active.value = patient_data["active"]
      patient_name = patient.name.add()
      for name in patient_data["names"]:
        patient_name.given.add().value = name
      bundle.entry.add().resource.patient.CopyFrom(patient)

    return bundle

  def setUp(self):
    super().setUp()
    self.mock_fhir_search_client = mock.create_autospec(
        fhir_client.Client, instance=True
    )
    self.runner = fhir_search_runner.FhirSearchRunner(
        self.mock_fhir_search_client, "Patient?_count=1"
    )
    self._context = context.LocalFhirPathContext(r4_package.load_base_r4())
    self._views = r4.from_definitions(self._context)

  def testToDataframe_withSimpleView_returnsExpectedDataframe(self):
    self.mock_fhir_search_client.search.return_value = (
        self._build_patient_bundle([
            {"names": ["Beyonce"], "active": True},
            {"names": ["Bob", "Smith"], "active": False},
            {"names": ["John", "Adedayo", "Bamidele"], "active": True},
        ])
    )
    pat = self._views.view_of("Patient")
    patient_names = pat.select({
        "name": pat.name.given,
    })

    result = self.runner.to_dataframe(patient_names)

    expected = pd.DataFrame(
        {"name": ["Beyonce", ["Bob", "Smith"], ["John", "Adedayo", "Bamidele"]]}
    )
    pd.testing.assert_frame_equal(expected, result)

  def testToDataframe_withSimpleObsView_returnsExpectedDataframe(self):
    self.mock_fhir_search_client.search.return_value = (
        self._build_observation_bundle(
            {"value": "153.058", "unit": "mg/dL", "display": "Cholesterol"}
        )
    )
    obs = self._views.view_of("Observation")
    observations = obs.select({
        "value": obs.valueQuantity.value,
        "unit": obs.valueQuantity.unit,
        "test": obs.code.coding.display.first(),
    })

    result = self.runner.to_dataframe(observations)

    expected = pd.DataFrame(
        {"value": ["153.058"], "unit": ["mg/dL"], "test": ["Cholesterol"]}
    )
    pd.testing.assert_frame_equal(expected, result)

  def testToDataframe_withConstraints_removesConstraintFromView(self):
    self.mock_fhir_search_client.search.return_value = (
        self._build_patient_bundle([{"names": ["Bob"], "active": False}])
    )
    pat = self._views.view_of("Patient")
    patient_names = pat.select({
        "name": pat.name.given,
    }).where(pat.active)

    result = self.runner.to_dataframe(patient_names)

    expected = pd.DataFrame()
    pd.testing.assert_frame_equal(expected, result)

  def testToDataframe_whenDataDoesNotExist_setsValueToNone(self):
    self.mock_fhir_search_client.search.return_value = (
        self._build_patient_bundle([{"names": ["Oprah"], "active": False}])
    )
    pat = self._views.view_of("Patient")
    patient_gender = pat.select({
        "gender": pat.gender,
    })

    result = self.runner.to_dataframe(patient_gender)

    expected = pd.DataFrame({"gender": [None]})
    pd.testing.assert_frame_equal(expected, result)


if __name__ == "__main__":
  absltest.main()

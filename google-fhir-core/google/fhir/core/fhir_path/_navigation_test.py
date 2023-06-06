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
"""Tests Python FHIRPath graph navigation functionality."""
from typing import Any, cast

from google.fhir.core.fhir_path import _navigation
from google.fhir.core.fhir_path import _structure_definitions as sdefs
from absl.testing import absltest
from absl.testing import parameterized


class FhirStructureDefinitionWalkerTest(parameterized.TestCase):
  """Tests the FhirStructureDefinitionWalker over a FHIR resource graph.

  The suite stands-up a list of synthetic resources for profiling and
  validation. The resources have the following structure:
  ```
  Foo {
    Bar bar
  }
  Bar {
    string baz
  }
  string {}
  ```
  """

  @classmethod
  def setUpClass(cls) -> None:
    super().setUpClass()

    bar_element_definition = sdefs.build_element_definition(
        id_='Foo.bar',
        type_codes=['Bar'],
        cardinality=sdefs.Cardinality(min=0, max='1'))
    foo = sdefs.build_resource_definition(
        id_='Foo',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Foo',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1')),
            bar_element_definition
        ])

    bar = sdefs.build_resource_definition(
        id_='Bar',
        element_definitions=[
            sdefs.build_element_definition(
                id_='Bar',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1')),
            sdefs.build_element_definition(
                id_='Bar.baz',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(0, '1')),
        ])

    # string datatype.
    string_datatype = sdefs.build_resource_definition(
        id_='string',
        element_definitions=[
            sdefs.build_element_definition(
                id_='string',
                type_codes=None,
                cardinality=sdefs.Cardinality(min=0, max='1'))
        ])

    structure_definitions = [
        foo,
        bar,
        string_datatype,
    ]

    cls.foo = foo

    cls.bar = bar
    cls.bar_non_root_element_def = bar_element_definition

    cls.env = _navigation._Environment(structure_definitions)

  def test_step_called_with_valid_identifier_starting_from_foo_succeeds(self):
    walker = _navigation.FhirStructureDefinitionWalker(self.env, self.foo)
    identifier = 'bar'
    expected_new_state_id = 'Foo.bar'

    walker.step(identifier)

    actual_element_id = cast(Any, walker.element).id.value
    self.assertEqual(actual_element_id, expected_new_state_id)

    actual_containing_type_id = cast(Any, walker.containing_type).id.value
    self.assertEqual(actual_containing_type_id, 'Foo')

  def test_step_called_with_valid_identifier_starting_from_bar_succeeds(self):
    walker = _navigation.FhirStructureDefinitionWalker(self.env, self.bar)
    identifier = 'baz'
    expected_new_state_id = 'Bar.baz'

    walker.step(identifier)

    actual_element_id = cast(Any, walker.element).id.value
    self.assertEqual(actual_element_id, expected_new_state_id)

    actual_containing_type_id = cast(Any, walker.containing_type).id.value
    self.assertEqual(actual_containing_type_id, 'Bar')

  def test_step_called_with_valid_identifier_starting_from_non_root_element_succeeds(
      self,
  ):
    walker = _navigation.FhirStructureDefinitionWalker(
        self.env, self.foo, self.bar_non_root_element_def)
    identifier = 'baz'
    expected_new_state_id = 'Bar.baz'

    walker.step(identifier)

    actual_element_id = cast(Any, walker.element).id.value
    self.assertEqual(actual_element_id, expected_new_state_id)

    actual_containing_type_id = cast(Any, walker.containing_type).id.value
    self.assertEqual(actual_containing_type_id, 'Bar')

  def test_step_called_with_global_walker_multiple_times_starting_from_foo_succeeds(
      self,
  ):
    walker = _navigation.FhirStructureDefinitionWalker(self.env, self.foo)

    identifier = 'bar'
    expected_new_state_id = 'Foo.bar'
    expected_containing_element = 'Foo'

    walker.step(identifier)

    actual_element_id = cast(Any, walker.element).id.value
    self.assertEqual(actual_element_id, expected_new_state_id)

    actual_containing_type_id = cast(Any, walker.containing_type).id.value
    self.assertEqual(actual_containing_type_id, expected_containing_element)

    # Walker will have moved from Foo to Bar after the previous call allowing us
    # to call baz this time.
    identifier = 'baz'
    expected_new_state_id = 'Bar.baz'
    expected_containing_element = 'Bar'

    walker.step(identifier)

    actual_element_id = cast(Any, walker.element).id.value
    self.assertEqual(actual_element_id, expected_new_state_id)

    actual_containing_type_id = cast(Any, walker.containing_type).id.value
    self.assertEqual(actual_containing_type_id, expected_containing_element)


class FhirStructureDefinitionWalkerErrorsTest(parameterized.TestCase):
  """Tests FhirStructureDefinitionWalker errors after running on resource graph.

  The suite stands-up a list of invalid synthetic resources for profiling and
  validation. The resources have the following structure:
  ```
  Fig {
    DoesNotExists invalid_typecode
    InvalidStruct invalid_struct
  }

  InvalidStruct {}
  ```
  """

  @classmethod
  def setUpClass(cls) -> None:
    super().setUpClass()

    # This resource is invalid because it has no root element definiton.
    invalid_struct = sdefs.build_resource_definition(
        id_='InvalidStruct', element_definitions=[])

    fig_root_element_definition = sdefs.build_element_definition(
        id_='Fig',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'))
    cls.invalid_typecode = sdefs.build_element_definition(
        id_='Fig.invalid_typecode',
        type_codes=['DoesNotExists'],
        cardinality=sdefs.Cardinality(min=0, max='1'))
    cls.invalid_struct = sdefs.build_element_definition(
        id_='Fig.invalid_struct',
        type_codes=['InvalidStruct'],
        cardinality=sdefs.Cardinality(min=0, max='1'))
    fig = sdefs.build_resource_definition(
        id_='Fig',
        element_definitions=[
            fig_root_element_definition,
            cls.invalid_typecode,
            cls.invalid_struct,
        ])

    structure_definitions = [
        fig,
        invalid_struct,
    ]

    cls.fig = fig
    cls.fig_root = fig_root_element_definition

    cls.env = _navigation._Environment(structure_definitions)

  def test_step_called_with_in_valid_identifier_raises_value_error(self):
    walker = _navigation.FhirStructureDefinitionWalker(self.env, self.fig)
    with self.assertRaisesRegex(ValueError,
                                'Unable to find child under containing_type'):
      _ = walker.step('unknownIdentifier')

  def test_step_called_with_in_valid_type_code_raises_value_error(self):
    walker = _navigation.FhirStructureDefinitionWalker(self.env, self.fig,
                                                       self.invalid_typecode)
    with self.assertRaisesRegex(ValueError,
                                'Unable to find `StructureDefinition` for'):
      _ = walker.step('anyIdentifier')

  def test_step_called_with_in_valid_struct_raises_value_error(self):
    walker = _navigation.FhirStructureDefinitionWalker(self.env, self.fig,
                                                       self.invalid_struct)
    with self.assertRaisesRegex(ValueError,
                                'Unable to find root `ElementDefinition` for'):
      _ = walker.step('anyIdentifier')


if __name__ == '__main__':
  absltest.main()

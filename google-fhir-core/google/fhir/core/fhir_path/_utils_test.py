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
"""Tests for _utils."""

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import _structure_definitions as sdefs
from google.fhir.core.fhir_path import _utils


class ElementTypeCodeTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(testcase_name='_withSingleTypeCode', type_codes=['Bar']),
      dict(
          testcase_name='_withMultipleTypeCodes', type_codes=['string', 'Bar']),
  )
  def testElementTypeCodes_succeeds(self, type_codes):
    element = sdefs.build_element_definition(
        id_='Foo.bar',
        type_codes=type_codes,
        cardinality=sdefs.Cardinality(min=0, max='1'))
    actual = _utils.element_type_codes(element)
    expected = type_codes
    self.assertEqual(actual, expected)

  def testElementTypeCode_withSingleTypeCode_succeeds(self):
    type_codes = ['Bar']
    element = sdefs.build_element_definition(
        id_='Foo.bar',
        type_codes=type_codes,
        cardinality=sdefs.Cardinality(min=0, max='1'))
    actual = _utils.element_type_code(element)
    expected = type_codes[0]
    self.assertEqual(actual, expected)

  def testElementTypeCode_withMultipleTypeCodes_raisesValueError(self):
    type_codes = ['string', 'Bar']
    with self.assertRaisesRegex(ValueError,
                                'Add support for more than one type.'):
      element = sdefs.build_element_definition(
          id_='Foo.bar',
          type_codes=type_codes,
          cardinality=sdefs.Cardinality(min=0, max='1'))
      _ = _utils.element_type_code(element)

  @parameterized.named_parameters(
      dict(
          testcase_name='_withRootElement',
          element_path='Foo',
          is_root=True,
      ),
      dict(
          testcase_name='_withNoneRootElement',
          element_path='Foo.bar',
          is_root=False,
      ),
  )
  def testIsRootElement_succeeds(self, element_path, is_root):
    element = sdefs.build_element_definition(
        id_=element_path,
        path=element_path,
        type_codes=['Foo'],
        cardinality=sdefs.Cardinality(min=0, max='1'))
    actual = _utils.is_root_element(element)
    expected = is_root
    self.assertEqual(actual, expected)


class IsSliceElementTest(absltest.TestCase):

  def testIsSliceElement_returnsTrue_withSliceElement(self):
    element = sdefs.build_element_definition(
        id_='slice:',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'))
    self.assertTrue(_utils.is_slice_element(element))

  def testIsSliceElement_returnsFalse_withNonSliceElement(self):
    element = sdefs.build_element_definition(
        id_='not_slice',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'))
    self.assertFalse(_utils.is_slice_element(element))


class IsSliceOnExtensionElementTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='_withSliceOnExtensionElement',
          element=sdefs.build_element_definition(
              id_='Foo.extension:slice',
              type_codes=['Extension'],
              cardinality=sdefs.Cardinality(min=0, max='1'),
          ),
          expected=True),
      dict(
          testcase_name='_withNonSlice',
          element=sdefs.build_element_definition(
              id_='Foo.slice',
              type_codes=['Extension'],
              cardinality=sdefs.Cardinality(min=0, max='1')),
          expected=False),
      dict(
          testcase_name='_withSliceOnNestedExtensionElement',
          element=sdefs.build_element_definition(
              id_='Foo.bar.extension:slice',
              type_codes=['Extension'],
              cardinality=sdefs.Cardinality(min=0, max='1'),
          ),
          expected=True),
      dict(
          testcase_name='_withSliceOnNonExtensionElement',
          element=sdefs.build_element_definition(
              id_='Observation.code:loinc',
              type_codes=['Code'],
              cardinality=sdefs.Cardinality(min=0, max='1')),
          expected=False),
  )
  def testIsSliceOnExtensionElement_succeeds(self, element, expected):
    self.assertEqual(_utils.is_slice_on_extension(element), expected)


class IsRecursiveElementTest(absltest.TestCase):

  def testRecursiveElement_returnsTrue_withRecursiveElement(self):
    element = sdefs.build_element_definition(
        id_='recursive_elem',
        path='foo.bar.baz',
        content_reference='#foo.bar',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'))
    self.assertTrue(_utils.is_recursive_element(element))

  def testIsRecursiveElement_returnsFalse_withNonRecursiveElement(self):
    element = sdefs.build_element_definition(
        id_='not_recursive_elem',
        path='foo.bar.baz',
        content_reference='#some.other.path',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'))
    self.assertFalse(_utils.is_recursive_element(element))

  def testIsRecursiveElement_returnsFalse_withNonContentRefElement(self):
    element = sdefs.build_element_definition(
        id_='not_recursive_elem',
        path='foo.bar.baz',
        type_codes=['HumanName'],
        cardinality=sdefs.Cardinality(min=0, max='1'))
    self.assertFalse(_utils.is_recursive_element(element))


class FhirPathUtilitiesTest(parameterized.TestCase):
  """Unit tests for module-level utility functions in `fhir_path.py`."""

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls.patient_root = sdefs.build_element_definition(
        id_='Patient', type_codes=None, cardinality=sdefs.Cardinality(0, '1'))
    patient_name = sdefs.build_element_definition(
        id_='Patient.name',
        type_codes=['HumanName'],
        cardinality=sdefs.Cardinality(0, '1'))
    patient_addresses = sdefs.build_element_definition(
        id_='Patient.addresses',
        type_codes=['Address'],
        cardinality=sdefs.Cardinality(0, '*'))
    patient_contact = sdefs.build_element_definition(
        id_='Patient.contact',
        type_codes=['BackboneElement'],
        cardinality=sdefs.Cardinality(0, '*'))
    patient_contact_name = sdefs.build_element_definition(
        id_='Patient.contact.name',
        type_codes=['HumanName'],
        cardinality=sdefs.Cardinality(0, '1'))
    patient_deceased = sdefs.build_element_definition(
        id_='Patient.deceased[x]',
        type_codes=['boolean', 'dateTime'],
        cardinality=sdefs.Cardinality(0, '*'))
    cls._patient_structdef = sdefs.build_resource_definition(
        id_='Patient',
        element_definitions=[
            cls.patient_root,
            patient_name,
            patient_addresses,
            patient_contact,
            patient_contact_name,
            patient_deceased,
        ])

  @parameterized.named_parameters(
      dict(
          testcase_name='_withRelativeIdentifier',
          root='Patient',
          identifier='name',
          expected='Patient.name'),
      dict(
          testcase_name='_withAbsoluteIdentifier',
          root='',
          identifier='Patient.name',
          expected='Patient.name'),
  )
  def testGetAbsoluteIdentifier_succeeds(self, root: str, identifier: str,
                                         expected: str):
    actual: str = _utils.get_absolute_identifier(root, identifier)
    self.assertEqual(actual, expected)

  @parameterized.named_parameters(
      dict(
          testcase_name='_withRelativeUri',
          uri='Patient',
          expected='http://hl7.org/fhir/StructureDefinition/Patient'),
      dict(
          testcase_name='_withAsboluteUri',
          uri='http://hl7.org/fhir/StructureDefinition/Patient',
          expected='http://hl7.org/fhir/StructureDefinition/Patient'),
  )
  def testGetAbsoluteUri_succeeds(self, uri: str, expected: str):
    actual: str = _utils.get_absolute_uri_for_structure(uri)
    self.assertEqual(actual, expected)

  @parameterized.named_parameters(
      dict(
          testcase_name='_withRepeadedElement',
          element=sdefs.build_element_definition(
              id_='value',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='*')),
          expected=True),
      dict(
          testcase_name='_withNonRepeadedElement',
          element=sdefs.build_element_definition(
              id_='value',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='1')),
          expected=False),
      dict(
          testcase_name='_withNonRepeadedElement_maxOfZero',
          element=sdefs.build_element_definition(
              id_='value',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='0')),
          expected=False),
  )
  def testIsRepeatedElement_succeeds(self, element, expected):
    actual = _utils.is_repeated_element(element)
    self.assertEqual(actual, expected)

  def testListBackboneElementFields_Succeeds(self):
    # Include normal and choice type field to ensure proper conversion.
    self.assertEqual(['name', 'addresses', 'contact', 'deceased'],
                     _utils.get_backbone_element_fields(self._patient_structdef,
                                                        ''))
    self.assertEqual(['name'],
                     _utils.get_backbone_element_fields(self._patient_structdef,
                                                        'contact'))

  def testIsBackboneElement_Succeeds(self):
    self.assertTrue(
        _utils.is_backbone_element(
            _utils.get_element(self._patient_structdef, 'contact')))
    self.assertFalse(
        _utils.is_backbone_element(
            _utils.get_element(self._patient_structdef, 'addresses')))

  def testGetRootElementDefinition_succeeds(self):
    root_element_def = _utils.get_root_element_definition(
        self._patient_structdef)
    self.assertEqual(root_element_def.id, self.patient_root.id)

  def testGetRootElementDefinition_withMultipleRoots_fails(self):

    with self.assertRaisesRegex(ValueError,
                                'Expected a single root ElementDefinition'):
      sdef_with_two_roots = sdefs.build_resource_definition(
          id_='Patient',
          element_definitions=[self.patient_root, self.patient_root],
      )
      _ = _utils.get_root_element_definition(sdef_with_two_roots)


if __name__ == '__main__':
  absltest.main()

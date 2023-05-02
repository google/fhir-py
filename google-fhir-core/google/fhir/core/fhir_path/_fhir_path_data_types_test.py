#
# Copyright 2023 Google LLC
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

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _structure_definitions as sdefs


class StructureDataTypeTest(absltest.TestCase):

  def testInit_withStructureDefinition_allowsAccessToElementDefinitions(self):
    element_definitions = [
        sdefs.build_element_definition(
            id_='Test',
            type_codes=None,
            cardinality=sdefs.Cardinality(min=1, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.field',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.field.deeper',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.collection',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
        ),
        sdefs.build_element_definition(
            id_='Test.collection:Slice',
            path='Test.collection',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
            slice_name='Slice',
        ),
        sdefs.build_element_definition(
            id_='Test.collection:Slice.field',
            path='Test.collection.field',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.collection:Slice.another_field',
            path='Test.collection.another_field',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.collection:Slice.another_collection:SliceOnSlice',
            path='Test.collection.another_collection',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
            slice_name='SliceOnSlice',
        ),
        sdefs.build_element_definition(
            id_='Test.collection:Slice.another_collection:SliceOnSlice.yet_another_field',
            path='Test.collection.another_collection.yet_another_field',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.extension:extension_field',
            path='Test.extension',
            type_codes=['Extension'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.extension:extension_field.collection',
            path='Test.extension.collection',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
        ),
        sdefs.build_element_definition(
            id_='Test.extension:extension_field.collection:AnotherSlice',
            path='Test.extension.collection',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
            slice_name='AnotherSlice',
        ),
        sdefs.build_element_definition(
            id_='Test.extension:extension_field.collection:AnotherSlice.field',
            path='Test.extension.collection.field',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.extension:extension_field.extension:another_extension.deeper.extension:yet_another_extension.even_deeper',
            path='Test.extension.extension.deeper.extension.even_deeper',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
    ]
    element_definitions_by_id = {
        elem.id.value: elem for elem in element_definitions
    }
    test_resource = sdefs.build_resource_definition(
        id_='Test', element_definitions=element_definitions
    )
    structure_definition = _fhir_path_data_types.StructureDataType(
        test_resource
    )

    self.assertCountEqual(
        list(structure_definition.iter_children()),
        [
            ('field', element_definitions_by_id['Test.field']),
            ('collection', element_definitions_by_id['Test.collection']),
            (
                'extension_field',
                element_definitions_by_id['Test.extension:extension_field'],
            ),
        ],
    )
    self.assertCountEqual(
        list(structure_definition.iter_all_descendants()),
        [
            ('field', element_definitions_by_id['Test.field']),
            ('field.deeper', element_definitions_by_id['Test.field.deeper']),
            ('collection', element_definitions_by_id['Test.collection']),
            (
                'extension_field',
                element_definitions_by_id['Test.extension:extension_field'],
            ),
            (
                'extension_field.collection',
                element_definitions_by_id[
                    'Test.extension:extension_field.collection'
                ],
            ),
            (
                'extension_field.another_extension.deeper.yet_another_extension.even_deeper',
                element_definitions_by_id[
                    'Test.extension:extension_field.extension:another_extension.deeper.extension:yet_another_extension.even_deeper'
                ],
            ),
        ],
    )
    self.assertCountEqual(
        list(structure_definition.iter_slices()),
        [
            _fhir_path_data_types.Slice(
                element_definitions_by_id['Test.collection:Slice'],
                'collection',
                [
                    (
                        'collection.field',
                        element_definitions_by_id[
                            'Test.collection:Slice.field'
                        ],
                    ),
                    (
                        'collection.another_field',
                        element_definitions_by_id[
                            'Test.collection:Slice.another_field'
                        ],
                    ),
                ],
            ),
            _fhir_path_data_types.Slice(
                element_definitions_by_id[
                    'Test.collection:Slice.another_collection:SliceOnSlice'
                ],
                'collection.another_collection',
                [
                    (
                        'collection.another_collection.yet_another_field',
                        element_definitions_by_id[
                            'Test.collection:Slice.another_collection:SliceOnSlice.yet_another_field'
                        ],
                    ),
                ],
            ),
            _fhir_path_data_types.Slice(
                element_definitions_by_id[
                    'Test.extension:extension_field.collection:AnotherSlice'
                ],
                'extension_field.collection',
                [
                    (
                        'extension_field.collection.field',
                        element_definitions_by_id[
                            'Test.extension:extension_field.collection:AnotherSlice.field'
                        ],
                    ),
                ],
            ),
        ],
    )


class FhirPathDataTypeTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='_withTypeCode_returnsCorrectFieldName',
          type_code='boolean',
          expected_field_name='boolean',
      ),
      dict(
          testcase_name='_withSpecialCasedTypeCode_returnsCorrectFieldName',
          type_code='string',
          expected_field_name='string_value',
      ),
      dict(
          testcase_name='_withUrl_returnsCorrectFieldName',
          type_code='http://hl7.org/fhirpath/System.String',
          expected_field_name='string_value',
      ),
      dict(
          testcase_name='_withNumberInTypeName_returnsCorrectFieldName',
          type_code='base64Binary',
          expected_field_name='base64_binary',
      ),
      dict(
          testcase_name='_withNonPrimitive_returnsCorrectFieldName',
          type_code='Address',
          expected_field_name='address',
      ),
      dict(
          testcase_name='_withSnakeCasedNonPrimitive_returnsCorrectFieldName',
          type_code='CodeableConcept',
          expected_field_name='codeable_concept',
      ),
  )
  def testFixedFieldForTypeCode(self, type_code, expected_field_name):
    self.assertEqual(
        _fhir_path_data_types.fixed_field_for_type_code(type_code),
        expected_field_name,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='_withPrimitive_returnsTrue',
          type_code='boolean',
          expected_result=True,
      ),
      dict(
          testcase_name='_withNonPrimitive_returnsFalse',
          type_code='Observation',
          expected_result=False,
      ),
      dict(
          testcase_name='_withUrlPrimitive_returnsTrue',
          type_code='http://hl7.org/fhirpath/System.String',
          expected_result=True,
      ),
      dict(
          testcase_name='_withUrlNonPrimitive_returnsFalse',
          type_code='http://hl7.org/fhirpath/Observation',
          expected_result=False,
      ),
  )
  def testIsTypeCodePrimitive(self, type_code, expected_result):
    self.assertEqual(
        _fhir_path_data_types.is_type_code_primitive(type_code),
        expected_result,
    )


if __name__ == '__main__':
  absltest.main()

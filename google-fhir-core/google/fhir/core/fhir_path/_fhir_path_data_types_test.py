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

  def test_init_with_structure_definition_allows_access_to_element_definitions(
      self,
  ):
    element_definitions = [
        sdefs.build_element_definition(
            id_='Test',
            type_codes=None,
            cardinality=sdefs.Cardinality(min=1, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.id',
            path='Test.id',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.choice',
            path='Test.choice',
            type_codes=['string', 'boolean'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
        ),
        sdefs.build_element_definition(
            id_='Test.field-with-hyphen',
            path='Test.field-with-hyphen',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='1'),
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
        sdefs.build_element_definition(
            id_='Test:rootSlice',
            path='Test',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
            slice_name='rootSlice',
        ),
        sdefs.build_element_definition(
            id_='Test:rootSlice.id',
            path='Test.id',
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

    # Add an override from a parent setting a different cardinality.
    parent_definitions = _fhir_path_data_types.ChildDefinitions()
    parent_definitions.add_definition(
        'field',
        sdefs.build_element_definition(
            id_='Test.field',
            type_codes=['string'],
            cardinality=sdefs.Cardinality(min=1, max='*'),
        ),
    )

    structure_definition = _fhir_path_data_types.StructureDataType.from_proto(
        test_resource, parent_definitions=parent_definitions
    )

    self.assertCountEqual(
        list(structure_definition.iter_children()),
        [
            ('id', element_definitions_by_id['Test.id']),
            ('choice', element_definitions_by_id['Test.choice']),
            (
                'field-with-hyphen',
                element_definitions_by_id['Test.field-with-hyphen'],
            ),
            ('field', parent_definitions['field']),
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
            ('id', element_definitions_by_id['Test.id']),
            ('choice', element_definitions_by_id['Test.choice']),
            (
                'field-with-hyphen',
                element_definitions_by_id['Test.field-with-hyphen'],
            ),
            ('field', parent_definitions['field']),
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
            _fhir_path_data_types.Slice(
                element_definitions_by_id['Test:rootSlice'],
                '',
                [
                    (
                        'id',
                        element_definitions_by_id['Test:rootSlice.id'],
                    ),
                ],
            ),
        ],
    )


class FhirPathDataTypeTest(parameterized.TestCase):

  @parameterized.named_parameters(
      dict(
          testcase_name='with_type_code_returns_correct_field_name',
          type_code='boolean',
          expected_field_name='boolean',
      ),
      dict(
          testcase_name=(
              'with_special_cased_type_code_returns_correct_field_name'
          ),
          type_code='string',
          expected_field_name='string_value',
      ),
      dict(
          testcase_name='with_url_returns_correct_field_name',
          type_code='http://hl7.org/fhirpath/System.String',
          expected_field_name='string_value',
      ),
      dict(
          testcase_name='with_number_in_type_name_returns_correct_field_name',
          type_code='base64Binary',
          expected_field_name='base64_binary',
      ),
      dict(
          testcase_name='with_non_primitive_returns_correct_field_name',
          type_code='Address',
          expected_field_name='address',
      ),
      dict(
          testcase_name=(
              'with_snake_cased_non_primitive_returns_correct_field_name'
          ),
          type_code='CodeableConcept',
          expected_field_name='codeable_concept',
      ),
  )
  def test_fixed_field_for_type_code(self, type_code, expected_field_name):
    self.assertEqual(
        _fhir_path_data_types.fixed_field_for_type_code(type_code),
        expected_field_name,
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_primitive_returns_true',
          type_code='boolean',
          expected_result=True,
      ),
      dict(
          testcase_name='with_non_primitive_returns_false',
          type_code='Observation',
          expected_result=False,
      ),
      dict(
          testcase_name='with_url_primitive_returns_true',
          type_code='http://hl7.org/fhirpath/System.String',
          expected_result=True,
      ),
      dict(
          testcase_name='with_url_non_primitive_returns_false',
          type_code='http://hl7.org/fhirpath/Observation',
          expected_result=False,
      ),
  )
  def test_is_type_code_primitive(self, type_code, expected_result):
    self.assertEqual(
        _fhir_path_data_types.is_type_code_primitive(type_code),
        expected_result,
    )

  def test_same_hash(self):
    class FakeDataType(_fhir_path_data_types.FhirPathDataType):

      @property
      def supported_coercion(self):
        return []

      @property
      def url(self):
        return _fhir_path_data_types.String.url

      def comparable(self):
        return False

    set_test = {
        FakeDataType(),
        FakeDataType(),
        _fhir_path_data_types.String,
        _fhir_path_data_types.String,
    }

    self.assertLen(set_test, 2)

  def test_equality_and_hash(self):
    collection_type = _fhir_path_data_types.Collection(
        types=set([_fhir_path_data_types.String, _fhir_path_data_types.Date])
    )
    collection_type_2 = _fhir_path_data_types.Collection(
        types=set([_fhir_path_data_types.Date, _fhir_path_data_types.String])
    )

    self.assertEqual(collection_type, collection_type_2)
    set_test = {collection_type, collection_type_2}
    # Since the two collection types hash to the same value, there should only
    # be one element in the set.
    self.assertLen(set_test, 1)
    self.assertEqual(set_test, {collection_type})

    collection_type_3 = _fhir_path_data_types.Collection(
        types=set(
            [_fhir_path_data_types.String, _fhir_path_data_types.DateTime]
        )
    )
    self.assertNotEqual(collection_type, collection_type_3)

    poly_type = _fhir_path_data_types.PolymorphicDataType(
        types={
            'collection': collection_type,
            'string': _fhir_path_data_types.String,
            'bool': _fhir_path_data_types.Boolean,
        }
    )

    poly_type_2 = _fhir_path_data_types.PolymorphicDataType(
        types={
            'string': _fhir_path_data_types.String,
            'bool': _fhir_path_data_types.Boolean,
            'collection': collection_type_2,
        }
    )
    self.assertEqual(poly_type, poly_type_2)
    set_test = {poly_type, poly_type_2}
    self.assertLen(set_test, 1)
    self.assertEqual(set_test, {poly_type})

    poly_type_3 = _fhir_path_data_types.PolymorphicDataType(
        types={
            'string': _fhir_path_data_types.String,
            'bool': _fhir_path_data_types.Boolean,
            'collection': collection_type_3,
        }
    )

    self.assertNotEqual(poly_type_2, poly_type_3)


class ChildDefinitionsTest(absltest.TestCase):

  def test_getters_retrieve_element_definition(self):
    element_definition_a = sdefs.build_element_definition(
        id_='a',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_b = sdefs.build_element_definition(
        id_='b',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    definitions = _fhir_path_data_types.ChildDefinitions()
    definitions.add_definition('a', element_definition_a)
    definitions.add_definition('b', element_definition_b)

    self.assertIn('a', definitions)
    self.assertEqual(definitions.get('a'), element_definition_a)
    self.assertEqual(definitions['a'], element_definition_a)

  def test_getters_indicate_missing_item(self):
    definitions = _fhir_path_data_types.ChildDefinitions()

    self.assertNotIn('a', definitions)
    self.assertIsNone(definitions.get('a'))
    with self.assertRaises(KeyError):
      _ = definitions['a']

  def test_getters_indicate_missing_for_edges(self):
    element_definition_a_b = sdefs.build_element_definition(
        id_='a.b',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )

    definitions = _fhir_path_data_types.ChildDefinitions()
    # a is present in the tree, but only as an edge.
    definitions.add_definition('a.b', element_definition_a_b)

    self.assertNotIn('a', definitions)
    self.assertIsNone(definitions.get('a'))
    with self.assertRaises(KeyError):
      _ = definitions['a']

  def test_get_child_definitions_returns_empty_containers_when_missing(self):
    definitions = _fhir_path_data_types.ChildDefinitions()
    self.assertIsNone(definitions.get_child_definitions('a').get('b'))

  def test_add_nested_definition_inserts_definition(self):
    definitions = _fhir_path_data_types.ChildDefinitions()

    element_definition = sdefs.build_element_definition(
        id_='a.b.c',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    definitions.add_definition('a.b.c', element_definition)

    self.assertEqual(
        definitions.get_child_definitions('a')
        .get_child_definitions('b')
        .get('c'),
        element_definition,
    )

  def test_iterators_yield_appropriate_definitions(self):
    definitions = _fhir_path_data_types.ChildDefinitions()

    element_definition_a = sdefs.build_element_definition(
        id_='a',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_a_b = sdefs.build_element_definition(
        id_='a.b',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_a_b_c = sdefs.build_element_definition(
        id_='a.b.c',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_a_c = sdefs.build_element_definition(
        id_='a.c',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_b_c = sdefs.build_element_definition(
        id_='b.c',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )

    definitions.add_definition('a', element_definition_a)
    definitions.add_definition('a.b', element_definition_a_b)
    definitions.add_definition('a.b.c', element_definition_a_b_c)
    definitions.add_definition('a.c', element_definition_a_c)
    definitions.add_definition('b.c', element_definition_b_c)

    self.assertCountEqual(
        definitions.iter_all_definitions(),
        [
            ('a', element_definition_a),
            ('a.b', element_definition_a_b),
            ('a.b.c', element_definition_a_b_c),
            ('a.c', element_definition_a_c),
            ('b.c', element_definition_b_c),
        ],
    )

    self.assertCountEqual(
        definitions.iter_child_definitions(),
        [('a', element_definition_a)],
    )

    self.assertCountEqual(
        definitions.keys(),
        ['a'],
    )

  def test_update_merges_all_child_definitions(self):
    definitions_a = _fhir_path_data_types.ChildDefinitions()
    definitions_b = _fhir_path_data_types.ChildDefinitions()

    element_definition_a_old = sdefs.build_element_definition(
        id_='a_old',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_a_new = sdefs.build_element_definition(
        id_='a_new',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_a_b = sdefs.build_element_definition(
        id_='a.b',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_a_b_c = sdefs.build_element_definition(
        id_='a.b.c',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_a_c = sdefs.build_element_definition(
        id_='a.c',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    element_definition_b = sdefs.build_element_definition(
        id_='b',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )

    definitions_a.add_definition('a', element_definition_a_old)
    definitions_a.add_definition('b', element_definition_b)
    definitions_a.add_definition('a.b', element_definition_a_b)

    definitions_b.add_definition('a', element_definition_a_new)
    definitions_b.add_definition('a.b.c', element_definition_a_b_c)
    definitions_b.add_definition('a.c', element_definition_a_c)

    definitions_a.update(definitions_b)
    self.assertCountEqual(
        definitions_a.iter_all_definitions(),
        [
            ('a', element_definition_a_new),
            ('a.b', element_definition_a_b),
            ('a.b.c', element_definition_a_b_c),
            ('a.c', element_definition_a_c),
            ('b', element_definition_b),
        ],
    )


if __name__ == '__main__':
  absltest.main()

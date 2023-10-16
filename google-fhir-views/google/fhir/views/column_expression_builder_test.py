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
"""Tests for column_expression_builder."""

import textwrap
from absl.testing import absltest
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import r4_package
from google.fhir.views import column_expression_builder
from google.fhir.views import r4


class ColumnExpressionBuilderTest(absltest.TestCase):

  """Tests all attributes in the ColumnExpressionBuilder."""

  _fhir_package: fhir_package.FhirPackage

  @classmethod
  def setUpClass(cls):
    super().setUpClass()
    cls._fhir_package = r4_package.load_base_r4()

  def setUp(self):
    super().setUp()
    self._context = context.LocalFhirPathContext(self._fhir_package)
    self._view = r4.from_definitions(self._context).view_of('Patient')

  def test_basic_builder(self):
    builder = self._view.name

    self.assertIsInstance(builder.builder, expressions.Builder)
    self.assertIsNone(builder.column_name)
    self.assertEmpty(builder.children)
    self.assertFalse(builder.needs_unnest)
    self.assertFalse(builder.sealed)
    self.assertEqual(str(builder), 'name')
    self.assertEqual(
        repr(builder),
        'ColumnExpressionBuilder("name")',
    )

  def test_alias(self):
    column_name = 'patient_name'
    builder_with_alias = self._view.name.alias(column_name)

    self.assertEqual(builder_with_alias.column_name, column_name)
    self.assertEmpty(builder_with_alias.children)
    self.assertFalse(builder_with_alias.needs_unnest)
    self.assertTrue(builder_with_alias.sealed)
    self.assertEqual(str(builder_with_alias), 'name.alias(patient_name)')

  def test_alias_after_foreach(self):
    column_name = 'patient_name'
    builder_with_foreach_alias = self._view.name.forEach().alias(column_name)

    self.assertEqual(builder_with_foreach_alias.column_name, column_name)
    self.assertEmpty(builder_with_foreach_alias.children)
    self.assertTrue(builder_with_foreach_alias.needs_unnest)
    self.assertTrue(builder_with_foreach_alias.sealed)
    self.assertEqual(
        str(builder_with_foreach_alias), 'name.forEach().alias(patient_name)'
    )

  def test_alias_after_select_raises_error(self):
    with self.assertRaises(AttributeError):
      name = self._view.name
      name.select([
          name.family.alias('family_name'),
          name.given.first().alias('given_name'),
      ]).alias('patient_name')

  def test_foreach(self):
    builder_with_foreach = self._view.name.forEach()

    self.assertIsNone(builder_with_foreach.column_name)
    self.assertEmpty(builder_with_foreach.children)
    self.assertTrue(builder_with_foreach.needs_unnest)
    self.assertTrue(builder_with_foreach.sealed)
    self.assertEqual(str(builder_with_foreach), 'name.forEach()')

  def test_foreach_on_non_collection(self):
    builder_with_foreach = self._view.name.first().forEach()

    self.assertIsNone(builder_with_foreach.column_name)
    self.assertEmpty(builder_with_foreach.children)
    self.assertTrue(builder_with_foreach.needs_unnest)
    self.assertTrue(builder_with_foreach.sealed)
    self.assertEqual(str(builder_with_foreach), 'name.first().forEach()')

  def test_foreach_select(self):
    name = self._view.name.where(self._view.name.exists())
    builder_with_foreach_select = name.forEach().select([
        name.family.alias('family_name'),
        name.given.first().alias('given_name'),
    ])

    self.assertIsNone(builder_with_foreach_select.column_name)
    self.assertLen(builder_with_foreach_select.children, 2)
    family_name_builder, given_name_builder = (
        builder_with_foreach_select.children
    )
    self.assertIsInstance(
        family_name_builder.node.parent_node, _evaluation.ReferenceNode
    )
    self.assertFalse(family_name_builder.return_type.returns_collection())
    self.assertIsInstance(
        given_name_builder.node.parent_node.parent_node,
        _evaluation.ReferenceNode,
    )
    self.assertFalse(given_name_builder.return_type.returns_collection())
    self.assertTrue(builder_with_foreach_select.needs_unnest)
    self.assertTrue(builder_with_foreach_select.sealed)
    self.assertMultiLineEqual(
        str(builder_with_foreach_select),
        textwrap.dedent("""\
        name.where($this.exists()).forEach().select([
          family.alias(family_name),
          given.first().alias(given_name)
        ])"""),
    )

  def test_select(self):
    name = self._view.name.first()
    builder_with_select = name.select([
        name.family.alias('family_name'),
        name.given.first().alias('given_name'),
    ])

    self.assertIsNone(builder_with_select.column_name)
    self.assertLen(builder_with_select.children, 2)
    family_name_builder, given_name_builder = builder_with_select.children
    self.assertIsInstance(
        family_name_builder.node.parent_node, _evaluation.ReferenceNode
    )
    self.assertIsInstance(
        given_name_builder.node.parent_node.parent_node,
        _evaluation.ReferenceNode,
    )
    self.assertFalse(builder_with_select.needs_unnest)
    self.assertTrue(builder_with_select.sealed)
    self.assertMultiLineEqual(
        str(builder_with_select),
        textwrap.dedent("""\
        name.first().select([
          family.alias(family_name),
          given.first().alias(given_name)
        ])"""),
    )

  def test_select_nested_select(self):
    name = self._view.name.first()
    period = name.period.where(name.period.start.exists()).first()
    builder_with_nested_select = name.select(
        [
            period.select([
                period.start.alias('period_start'),
                period.end.alias('period_end'),
            ])
        ]
    )

    self.assertIsNone(builder_with_nested_select.column_name)
    self.assertLen(builder_with_nested_select.children, 1)
    self.assertLen(builder_with_nested_select.children[0].children, 2)
    self.assertFalse(builder_with_nested_select.needs_unnest)
    self.assertTrue(builder_with_nested_select.sealed)
    self.assertMultiLineEqual(
        str(builder_with_nested_select),
        textwrap.dedent("""\
        name.first().select([
          period.where(start.exists()).first().select([
            start.alias(period_start),
            end.alias(period_end)
          ])
        ])"""),
    )

  def test_select_children_without_alias_or_children_raises_error(self):
    with self.assertRaises(AttributeError):
      name = self._view.name
      name.first().select([name.family])

  def test_select_after_alias_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.alias('patient_name').select([])

  def test_select_on_collection_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.select([])

  def test_select_on_non_structure_node_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.given.select([])

  def test_keep_building_fhir_path(self):
    builder = self._view.name.first()

    self.assertEqual(builder.fhir_path, 'name.first()')

  def test_keep_building_fhir_path_after_alias_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.alias('patient_name').first()

  def test_keep_building_fhir_path_after_foreach_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.forEach().first()

  def test_keep_building_fhir_path_after_select_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.select([]).first()

  def test_get_non_builder_attribute(self):
    node = self._view.name.node

    self.assertIsInstance(node, _evaluation.ExpressionNode)

  def test_get_non_builder_attribute_after_alias(self):
    node = self._view.name.alias('a').node

    self.assertIsInstance(node, _evaluation.ExpressionNode)

  def test_get_a_list_of_builders(self):
    resource_builders = self._view.name.get_resource_builders()

    for builder in resource_builders:
      self.assertIsInstance(
          builder, column_expression_builder.ColumnExpressionBuilder
      )

  def test_get_a_dict_of_builders(self):
    resource_builders = self._view.name._choice_fields()

    for _, builder in resource_builders:
      self.assertIsInstance(
          builder, column_expression_builder.ColumnExpressionBuilder
      )

  def test_get_non_existent_attribute_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.non_existent_attribute  # pylint: disable=pointless-statement

  def test_get_item(self):
    builder = self._view.name[0]

    self.assertEqual(builder.fhir_path, 'name[0]')

  def test_get_item_after_alias_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.alias('a')[0]  # pylint: disable=expression-not-assigned

  def test_get_non_existent_item_raises_error(self):
    with self.assertRaises(TypeError):
      self._view.name['a']  # pylint: disable=pointless-statement

  def test_eq_operation(self):
    eq_operation = self._view.name.count() == 1

    self.assertEqual(
        repr(eq_operation),
        'ColumnExpressionBuilder("name.count() = 1")',
    )

  def test_ne_operation(self):
    ne_operation = self._view.name.count() != 1

    self.assertEqual(
        repr(ne_operation),
        'ColumnExpressionBuilder("name.count() != 1")',
    )

  def test_or_operation(self):
    or_operation = (self._view.name.count() == 1) | (
        self._view.name.count() == 2
    )

    self.assertEqual(
        repr(or_operation),
        'ColumnExpressionBuilder("name.count() = 1 or name.count() = 2")',
    )

  def test_and_operation(self):
    and_operation = (self._view.name.count() == 1) & self._view.name[0]

    self.assertEqual(
        repr(and_operation),
        'ColumnExpressionBuilder("name.count() = 1 and name[0]")',
    )

  def test_xor_operation(self):
    xor_operation = self._view.name ^ self._view.address

    self.assertEqual(
        repr(xor_operation),
        'ColumnExpressionBuilder("name xor address")',
    )

  def test_lt_operation(self):
    lt_operation = self._view.name.count() < 1

    self.assertEqual(
        repr(lt_operation),
        'ColumnExpressionBuilder("name.count() < 1")',
    )

  def test_gt_operation(self):
    gt_operation = self._view.name.count() > 1

    self.assertEqual(
        repr(gt_operation),
        'ColumnExpressionBuilder("name.count() > 1")',
    )

  def test_le_operation(self):
    le_operation = self._view.name.count() <= 1

    self.assertEqual(
        repr(le_operation),
        'ColumnExpressionBuilder("name.count() <= 1")',
    )

  def test_ge_operation(self):
    ge_operation = self._view.name.count() >= 1

    self.assertEqual(
        repr(ge_operation),
        'ColumnExpressionBuilder("name.count() >= 1")',
    )

  def test_add_operation(self):
    add_operation = self._view.name.count() + 1

    self.assertEqual(
        repr(add_operation),
        'ColumnExpressionBuilder("name.count() + 1")',
    )

  def test_mul_operation(self):
    mul_operation = self._view.name.count() * 10

    self.assertEqual(
        repr(mul_operation),
        'ColumnExpressionBuilder("name.count() * 10")',
    )

  def test_sub_operation(self):
    sub_operation = self._view.name.count() - 1

    self.assertEqual(
        repr(sub_operation),
        'ColumnExpressionBuilder("name.count() - 1")',
    )

  def test_truediv_operation(self):
    truediv_operation = self._view.name.count() / 2

    self.assertEqual(
        repr(truediv_operation),
        'ColumnExpressionBuilder("name.count() / 2")',
    )

  def test_floordiv_operation(self):
    floordiv_operation = self._view.name.count() // 2

    self.assertEqual(
        repr(floordiv_operation),
        'ColumnExpressionBuilder("name.count() div 2")',
    )

  def test_mod_operation(self):
    mod_operation = self._view.name.count() % 2

    self.assertEqual(
        repr(mod_operation),
        'ColumnExpressionBuilder("name.count() mod 2")',
    )

  def test_call_operation_after_alias_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.count().alias('a') + 1  # pylint: disable=expression-not-assigned


if __name__ == '__main__':
  absltest.main()

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
    self.assertEqual(str(builder), 'name')
    self.assertEqual(
        repr(builder),
        'ColumnExpressionBuilder("name")',
    )

  def test_alias(self):
    column_name = 'patient_name'
    builder_with_alias = self._view.name.alias(column_name)

    self.assertEqual(builder_with_alias.column_name, column_name)
    self.assertEqual(str(builder_with_alias), 'name.alias(patient_name)')
    self.assertEqual(
        repr(builder_with_alias),
        'ColumnExpressionBuilder("name.alias(patient_name)")',
    )

  def test_keep_building_fhir_path(self):
    builder = self._view.name.first()

    self.assertEqual(builder.fhir_path, 'name.first()')

  def test_keep_building_fhir_path_after_alias_raises_error(self):
    with self.assertRaises(AttributeError):
      self._view.name.alias('a').first()

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

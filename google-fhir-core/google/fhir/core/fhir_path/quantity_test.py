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
import decimal
from absl.testing import absltest
from google.fhir.r4.proto.core import datatypes_pb2
from google.fhir.core.fhir_path import quantity


class QuantityTest(absltest.TestCase):
  ONE_GRAM = quantity.Quantity(value=decimal.Decimal(1), unit='g')
  TWO_GRAMS = quantity.Quantity(value=decimal.Decimal(2), unit='g')
  ZERO_GRAMS = quantity.Quantity(value=decimal.Decimal(0), unit='g')
  ONE_KILOGRAM = quantity.Quantity(value=decimal.Decimal(1), unit='kg')

  def test_eq_ne(self):
    self.assertEqual(
        self.ONE_GRAM, quantity.Quantity(value=decimal.Decimal(1), unit='g')
    )
    self.assertNotEqual(self.ONE_GRAM, self.TWO_GRAMS)
    self.assertNotEqual(self.ONE_GRAM, self.ZERO_GRAMS)
    with self.assertRaisesRegex(NotImplementedError, 'Units must be the same'):
      self.ONE_GRAM.__eq__(self.ONE_KILOGRAM)
    self.assertNotEqual(self.ONE_GRAM, 1)

  def test_lt(self):
    self.assertLess(self.ONE_GRAM, self.TWO_GRAMS)
    # pylint: disable=g-generic-assert
    self.assertFalse(
        self.ONE_GRAM < quantity.Quantity(value=decimal.Decimal(1), unit='g')
    )
    self.assertFalse(self.ONE_GRAM < self.ZERO_GRAMS)
    with self.assertRaisesRegex(NotImplementedError, 'Units must be the same'):
      self.ONE_GRAM.__lt__(self.ONE_KILOGRAM)
    self.assertEqual(self.ONE_GRAM.__lt__(1), NotImplemented)  # pytype: disable=unsupported-operands

  def test_gt(self):
    self.assertGreater(self.ONE_GRAM, self.ZERO_GRAMS)
    # pylint: disable=g-generic-assert
    self.assertFalse(
        self.ONE_GRAM > quantity.Quantity(value=decimal.Decimal(1), unit='g')
    )
    self.assertFalse(self.ONE_GRAM > self.TWO_GRAMS)
    with self.assertRaisesRegex(NotImplementedError, 'Units must be the same'):
      self.ONE_GRAM.__gt__(self.ONE_KILOGRAM)
    self.assertEqual(self.ONE_GRAM.__gt__(1), NotImplemented)  # pytype: disable=unsupported-operands

  def test_le(self):
    self.assertLessEqual(self.ONE_GRAM, self.TWO_GRAMS)
    self.assertLessEqual(
        self.ONE_GRAM, quantity.Quantity(value=decimal.Decimal(1), unit='g')
    )
    # pylint: disable=g-generic-assert
    self.assertFalse(self.ONE_GRAM <= self.ZERO_GRAMS)
    with self.assertRaisesRegex(NotImplementedError, 'Units must be the same'):
      self.ONE_GRAM.__le__(self.ONE_KILOGRAM)
    self.assertEqual(self.ONE_GRAM.__le__(1), NotImplemented)  # pytype: disable=unsupported-operands

  def test_ge(self):
    self.assertGreaterEqual(self.ONE_GRAM, self.ZERO_GRAMS)
    self.assertGreaterEqual(
        self.ONE_GRAM, quantity.Quantity(value=decimal.Decimal(1), unit='g')
    )
    # pylint: disable=g-generic-assert
    self.assertFalse(self.ONE_GRAM >= self.TWO_GRAMS)
    with self.assertRaisesRegex(NotImplementedError, 'Units must be the same'):
      self.ONE_GRAM.__ge__(self.ONE_KILOGRAM)
    self.assertEqual(self.ONE_GRAM.__ge__(1), NotImplemented)  # pytype: disable=unsupported-operands

  def test_str(self):
    self.assertEqual(
        str(quantity.Quantity(value=decimal.Decimal(1), unit='g')), "1 'g'"
    )

  def test_quantity_from_proto_with_missing_value(self):
    with self.assertRaisesRegex(ValueError, r".*'value' and 'unit'.*"):
      quantity.quantity_from_proto(datatypes_pb2.Annotation())

  def test_quantity_from_proto_with_missing_unit(self):
    with self.assertRaisesRegex(ValueError, r".*'value' and 'unit'.*"):
      quantity.quantity_from_proto(datatypes_pb2.String(value='1.0'))

  def test_quantity_from_proto_with_valid_resource(self):
    self.assertEqual(
        quantity.quantity_from_proto(
            datatypes_pb2.Quantity(
                value=datatypes_pb2.Decimal(value='1.0'),
                unit=datatypes_pb2.String(value='g'),
            )
        ),
        quantity.Quantity(value=decimal.Decimal(1), unit='g'),
    )


if __name__ == '__main__':
  absltest.main()

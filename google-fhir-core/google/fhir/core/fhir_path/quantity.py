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
"""Support for Quantity throughout the FHIR Path library."""

import decimal
from typing import Any, cast
from google.protobuf import message


# TODO(b/226133941): Validate units as UCUM and support unit conversion for
# comparisons.
class Quantity:
  """A representation of a Quantity that can be used with a FHIRPath expression builder.

  This class can be used for comparing quantities, for example:

    builder = fhir_path.compile_expression(
      'Observation', context, "value.quantity < 2 'g'") < Quantity(2, 'g')

  Unit conversion is unsupported. Attempts to compare with different units will
  raise `NotImplementedError`-s.
  """

  def __init__(self, value: decimal.Decimal, unit: str):
    self.value = value
    self.unit = unit

  def _validate_units(self, other: 'Quantity') -> None:
    if self.unit != other.unit:
      raise NotImplementedError(
          'Unit conversion is not supported. Units must be the same.')

  def __eq__(self, other: 'Quantity') -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    self._validate_units(other)
    return self.value == other.value

  def __ne__(self, other: 'Quantity') -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    self._validate_units(other)
    return not self.__eq__(other)

  def __lt__(self, other: 'Quantity') -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    self._validate_units(other)
    return self.value < other.value

  def __gt__(self, other: 'Quantity') -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    self._validate_units(other)
    return self.value > other.value

  def __le__(self, other: 'Quantity') -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    self._validate_units(other)
    return self.value <= other.value

  def __ge__(self, other: 'Quantity') -> bool:
    if not isinstance(other, type(self)):
      return NotImplemented
    self._validate_units(other)
    return self.value >= other.value

  def __str__(self) -> str:
    return f"{self.value} '{self.unit}'"

  # Python requires that if an object is hashable, any object with which that
  # object compares equal must hash to the same value. If this is needed in the
  # future, implement an explicit __hash__ function.
  __hash__ = None


def quantity_from_proto(
    quantity_normative_resource: message.Message) -> Quantity:
  """Creates a `Quantity` from the normative quantity resource message.

  Args:
    quantity_normative_resource: The Quantity FHIR resource message
      (https://build.fhir.org/datatypes.html#quantity). Expected to specify the
      'value' and 'unit'.

  Returns:
    A `Quantity` representing the supplied `quantity_normative_resource`.

  Raises:
    ValueError: The supplied `quantity_normative_resource` did not specify the
    'value' and 'unit'.
  """

  resource = cast(Any, quantity_normative_resource)
  if not hasattr(resource, 'value') or not hasattr(resource, 'unit'):
    raise ValueError("A quantity message must provide the 'value' and 'unit'. "
                     f'Received: {quantity_normative_resource}.')
  value = decimal.Decimal(resource.value.value)
  unit = resource.unit.value

  return Quantity(value, unit)

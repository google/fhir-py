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
"""Base class of tests for FHIRPath evaluation."""

import abc
import datetime
import decimal
import keyword
import math
import textwrap
from typing import List, Union, cast
from google.protobuf import descriptor
from google.protobuf import message
from google.protobuf import symbol_database
from absl.testing import parameterized
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.core.fhir_path import python_compiled_expressions
from google.fhir.core.fhir_path import quantity
from google.fhir.core.utils import proto_utils

_UNIX_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class _ParameterizedABCMetaclass(
    parameterized.TestGeneratorMetaclass, abc.ABCMeta
):
  """A workaround class to resolve metaclass conflicts."""


class FhirPathExpressionsTest(
    parameterized.TestCase, metaclass=_ParameterizedABCMetaclass
):
  """A suite of tests to ensure proper validation for FHIRPath evaluation."""

  @abc.abstractmethod
  def compile_expression(
      self, structdef_url: str, fhir_path_expression: str
  ) -> python_compiled_expressions.PythonCompiledExpression:
    pass

  @abc.abstractmethod
  def builder(self, structdef_url: str) -> expressions.Builder:
    pass

  @abc.abstractmethod
  def context(self) -> context.LocalFhirPathContext:
    pass

  @abc.abstractmethod
  def patient_descriptor(self) -> descriptor.Descriptor:
    pass

  @abc.abstractmethod
  def observation_descriptor(self) -> descriptor.Descriptor:
    pass

  def _new_patient(self):
    return symbol_database.Default().GetPrototype(self.patient_descriptor())()

  def _new_observation(self):
    return symbol_database.Default().GetPrototype(
        self.observation_descriptor()
    )()

  @abc.abstractmethod
  def value_set_builder(self, url: str):
    pass

  def assert_expression_result(
      self,
      parsed_expression: python_compiled_expressions.PythonCompiledExpression,
      builder: expressions.Builder,
      resource: message.Message,
      expected_result: Union[bool, str, float, int],
  ):
    """Checks correctness of a builder against the equivalent parsed version."""
    # $this syntax isn't supported in the builder so skip the fhir path
    # expression check.
    if '$this' not in parsed_expression.fhir_path:
      # Confirm the expressions themselves match.
      self.assertEqual(parsed_expression.fhir_path, builder.fhir_path)

    # Evaluate both the built and parsed expressions and ensure they
    # produce the same result.
    parsed_result = parsed_expression.evaluate(resource)
    built_result = (
        python_compiled_expressions.PythonCompiledExpression.from_builder(
            builder
        ).evaluate(resource)
    )

    if expected_result is None:
      self.assertFalse(parsed_result.has_value())
      self.assertFalse(built_result.has_value())
      return

    expected_type = _fhir_path_data_types.Empty
    if isinstance(expected_result, bool):
      self.assertIs(expected_result, parsed_result.as_bool())
      self.assertIs(expected_result, built_result.as_bool())
      expected_type = _fhir_path_data_types.Boolean
    elif isinstance(expected_result, float) or isinstance(expected_result, int):
      self.assertTrue(math.isclose(expected_result, parsed_result.as_decimal()))
      self.assertTrue(math.isclose(expected_result, built_result.as_decimal()))
      if isinstance(expected_result, float):
        expected_type = _fhir_path_data_types.Decimal
      else:
        expected_type = _fhir_path_data_types.Integer
    else:
      self.assertEqual(expected_result, parsed_result.as_string())
      self.assertEqual(expected_result, built_result.as_string())
      expected_type = _fhir_path_data_types.String

    builder_type = builder.node.return_type
    if isinstance(builder_type, _fhir_path_data_types.PolymorphicDataType):
      self.assertIn(
          expected_type,
          cast(
              _fhir_path_data_types.PolymorphicDataType, builder_type
          ).types.values(),
      )
    else:
      if _fhir_path_data_types.is_numeric(expected_type):
        self.assertTrue(_fhir_path_data_types.is_numeric(builder_type))
      else:
        self.assertEqual(expected_type, builder_type)

  # Exclude parameter type info to work with multiple FHIR versions.
  def _set_fhir_enum_by_name(self, enum_wrapper, enum_value_name) -> None:
    """Helper method to set an enum value by string."""
    enum_field = next(
        field
        for field in enum_wrapper.DESCRIPTOR.fields
        if field.name == 'value'
    )
    if not enum_field:
      raise ValueError(
          f'{enum_wrapper.DESCRIPTOR.name} is not a valid FHIR enum. '
      )
    value_descrip = enum_field.enum_type.values_by_name[enum_value_name]
    if not value_descrip:
      raise ValueError(f'No such enum value {enum_value_name}.')
    enum_wrapper.value = value_descrip.index

  def test_simple_root_field_for_resource_has_expected_value(self):
    patient = self._new_patient()
    patient.active.value = True

    self.assert_expression_result(
        self.compile_expression('Patient', 'active'),
        self.builder('Patient').active,
        patient,
        True,
    )

  def test_missing_field_raises_error(self):
    pat = self.builder('Patient')
    with self.assertRaises(AttributeError):
      pat.noSuchField  # pylint: disable=pointless-statement

    self.assertIsNotNone(pat.address)
    with self.assertRaises(AttributeError):
      pat.address.noSuchField  # pylint: disable=pointless-statement

  def test_expressions_with_wrong_type_raises_error(self):
    pat = self.builder('Patient')
    patient = self._new_patient()
    patient.address.add().city.value = 'Seattle'
    patient.telecom.add().rank.value = 5

    with self.assertRaises(ValueError):
      pat.address.city + pat.telecom.rank  # pylint: disable=pointless-statement

  def test_nested_field_for_resource_has_expected_value(self):
    patient = self._new_patient()
    patient.address.add().city.value = 'Seattle'

    self.assert_expression_result(
        self.compile_expression('Patient', 'address.city'),
        self.builder('Patient').address.city,
        patient,
        'Seattle',
    )

  def test_exists_function_for_resource_succeeds(self):
    """Tests the behavior of the exists() function."""
    patient = self._new_patient()

    # Check builder and expression return correct results when the field does
    # not exist.
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.exists()'),
        self.builder('Patient').active.exists(),
        patient,
        False,
    )

    # Check builder and expression return correct results when the field exists.
    patient.active.value = True
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.exists()'),
        self.builder('Patient').active.exists(),
        patient,
        True,
    )

    # Tests edge case that function at the root node should still work.
    self.assert_expression_result(
        self.compile_expression('Patient', 'exists()'),
        self.builder('Patient').exists(),
        patient,
        True,
    )

  def test_first_function_for_resource_succeeds(self):
    """Tests the behavior of the first() function."""
    # Note: the reason why we are using an expression with
    # multiple first() calls is because we need the result to ultimately
    # evalutate to something that's not a proto message (like a String) in this
    # case, because EvaluationResult does not yet support evaluating to proto
    # messages.
    # TODO(b/208900793): Consider revisiting when EvaluationResult supports
    # other types and messages.
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'name.first().given.first()'),
        self.builder('Patient').name.first().given.first(),
        patient,
        None,
    )

    first_name = patient.name.add()
    first_name.given.add().value = 'Bob'  # First given name
    first_name.given.add().value = 'Smith'  # Second given name

    self.assert_expression_result(
        self.compile_expression('Patient', 'name.first().given.first()'),
        self.builder('Patient').name.first().given.first(),
        patient,
        'Bob',
    )

    # When called on a non-repeated field, first() is a no-op.
    patient.active.value = True
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.first()'),
        self.builder('Patient').active.first(),
        patient,
        True,
    )

  def test_any_true_function_for_resource_succeeds(self):
    """Tests the behavior of the anyTrue() function."""
    observation = self._new_observation()
    category_1 = observation.category.add()
    category_1_coding = category_1.coding.add()
    category_1_coding.system.value = 'mysystem'
    category_1_coding.code.value = 'category_1'

    category_2 = observation.category.add()
    category_2_coding = category_2.coding.add()
    category_2_coding.system.value = 'mysystem'
    category_2_coding.code.value = 'category_2'

    # Create a valueset and add it to the context so it is resolved
    # in memberOf evaluation.
    category_1_valueset = 'url:test:valueset'
    value_set = (
        self.value_set_builder(category_1_valueset)
        .with_codes('mysystem', ['category_1'])
        .build()
    )

    self.context().add_local_value_set(value_set)

    parsed_expr = self.compile_expression(
        'Observation', f"category.memberOf('{category_1_valueset}').anyTrue()"
    )
    built_expr = (
        self.builder('Observation')
        .category.memberOf(category_1_valueset)
        .anyTrue()
    )

    self.assert_expression_result(parsed_expr, built_expr, observation, True)
    category_1_coding.code.value = 'something_else'
    self.assert_expression_result(parsed_expr, built_expr, observation, False)

  def test_indexer_for_resource_succeeds(self):
    """Tests the behavior of the Indexer."""
    # Note: the reason why we are using an expression with
    # multiple indexer calls is because we need the result to ultimately
    # evalutate to something that's not a proto message (like a String) in this
    # case, because EvaluationResult does not yet support evaluating to proto
    # messages.
    # TODO(b/208900793): Consider revisiting when EvaluationResult supports
    # other types and messages.
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'name[0].given[0]'),
        self.builder('Patient').name[0].given[0],
        patient,
        None,
    )

    first_name = patient.name.add()
    first_name.given.add().value = 'Bob'  # First given name
    first_name.given.add().value = 'Smith'  # Second given name

    self.assert_expression_result(
        self.compile_expression('Patient', 'name[0].given[1]'),
        self.builder('Patient').name[0].given[1],
        patient,
        'Smith',
    )

    # Index out of bounds should result in an empty array.
    self.assert_expression_result(
        self.compile_expression('Patient', 'name[0].given[2]'),
        self.builder('Patient').name[0].given[2],
        patient,
        None,
    )

    # When called on a non-repeated field with 0 index, indexer is a no-op.
    patient.active.value = True
    self.assert_expression_result(
        self.compile_expression('Patient', 'active[0]'),
        self.builder('Patient').active[0],
        patient,
        True,
    )

  def test_count_function_for_resource_succeeds(self):
    """Tests the behavior of the count() function."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'name.first().given.count()'),
        self.builder('Patient').name.first().given.count(),
        patient,
        0,
    )

    first_name = patient.name.add()
    first_name.given.add().value = 'Bob'  # First given name
    first_name.given.add().value = 'Smith'  # Second given name

    self.assert_expression_result(
        self.compile_expression('Patient', 'name.first().given.count()'),
        self.builder('Patient').name.first().given.count(),
        patient,
        2,
    )

    # When called on a non-repeated field, count() returns 1.
    patient.active.value = True
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.count()'),
        self.builder('Patient').active.count(),
        patient,
        1,
    )

  def test_builder_has_all_functions(self):
    """Ensures the builder has all functions visible to the FHIRPath parser."""
    # pylint: disable=protected-access
    expected_fhirpath_functions: List[str] = []
    for function_name in _evaluation._FUNCTION_NODE_MAP.keys():
      if keyword.iskeyword(function_name):
        expected_fhirpath_functions.append(function_name + '_')
      else:
        expected_fhirpath_functions.append(function_name)
    self.assertContainsSubset(
        expected_fhirpath_functions, dir(self.builder('Patient'))
    )
    # pylint: enable=protected-access

  def test_empty_function_for_resource_succeeds(self):
    """Tests the behavior of the empty() function."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'name.first().given.empty()'),
        self.builder('Patient').name.first().given.empty(),
        patient,
        True,
    )

    first_name = patient.name.add()
    first_name.given.add().value = 'Bob'  # First given name
    first_name.given.add().value = 'Smith'  # Second given name

    self.assert_expression_result(
        self.compile_expression('Patient', 'name.first().given.empty()'),
        self.builder('Patient').name.first().given.empty(),
        patient,
        False,
    )

    # When called on a non-repeated field, empty() returns False.
    patient.active.value = True
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.empty()'),
        self.builder('Patient').active.empty(),
        patient,
        False,
    )

  def test_matches_function_for_resource_succeeds(self):
    """Tests the behavior of the matches() function."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', "name.first().given.matches('B')"),
        self.builder('Patient').name.first().given.matches('B'),
        patient,
        None,
    )

    first_name = patient.name.add()
    first_name.given.add().value = 'Bob'

    self.assert_expression_result(
        self.compile_expression('Patient', "name.first().given.matches('B')"),
        self.builder('Patient').name.first().given.matches('B'),
        patient,
        True,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', "name.first().given.matches('F')"),
        self.builder('Patient').name.first().given.matches('F'),
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression(
            'Patient', "name.first().given.matches('^[a-zA-Z0-9_]*$')"
        ),
        self.builder('Patient').name.first().given.matches('^[a-zA-Z0-9_]*$'),
        patient,
        True,
    )

  def test_has_value_function_for_resource_succeeds(self):
    """Tests the behavior of the has_value() function."""
    patient = self._new_patient()

    # hasValue is false when there is no value
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.hasValue()'),
        self.builder('Patient').active.hasValue(),
        patient,
        False,
    )

    # hasValue is true when there is exactly one primitive.
    patient.active.value = True
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.hasValue()'),
        self.builder('Patient').active.hasValue(),
        patient,
        True,
    )

    # hasValue is false for struct values.
    patient.address.add().city.value = 'Seattle'
    self.assert_expression_result(
        self.compile_expression('Patient', 'address.hasValue()'),
        self.builder('Patient').address.hasValue(),
        patient,
        False,
    )

  def test_value_from_primitive_succeeds(self):
    """Tests getting values from primitives."""
    # Placeholder resource to call expression valuation.
    patient = self._new_patient()

    def eval_literal(
        literal: str,
    ) -> python_compiled_expressions.EvaluationResult:
      expression = self.compile_expression('Patient', literal)
      self.assertEqual(literal, expression.fhir_path)
      return expression.evaluate(patient)

    self.assertEqual('foo', eval_literal("'foo'").as_string())
    self.assertEqual(
        'Complex string!?!@', eval_literal("'Complex string!?!@'").as_string()
    )

    self.assertTrue(eval_literal('true').as_bool())
    self.assertFalse(eval_literal('false').as_bool())

    self.assertEqual(42, eval_literal('42').as_int())
    self.assertEqual(-9999, eval_literal('-9999').as_int())
    self.assertEqual(decimal.Decimal('3.14'), eval_literal('3.14').as_decimal())
    self.assertEqual(
        decimal.Decimal('-1.2345'), eval_literal('-1.2345').as_decimal()
    )

  @parameterized.named_parameters(
      dict(testcase_name='equal', left='Seattle', right='Seattle', result=True),
      dict(
          testcase_name='not_equal',
          left='Seattle',
          right='Vancouver',
          result=False,
      ),
      dict(
          testcase_name='left_none', left=None, right='Vancouver', result=None
      ),
      dict(testcase_name='right_none', left='Seattle', right=None, result=None),
      dict(testcase_name='both_none', left=None, right=None, result=None),
  )
  def test_builder_equality_for_resource_succeeds(
      self, left: str, right: str, result: bool
  ):
    """Tests equality operator with two builder operands."""
    patient = self._new_patient()
    builder = self.builder('Patient')
    equality_expr = self.compile_expression(
        'Patient', 'address.city = contact.address.city'
    )
    equality_builder = builder.address.city == builder.contact.address.city

    if left is not None:
      patient.address.add().city.value = left
    if right is not None:
      patient.contact.add().address.city.value = right
    self.assert_expression_result(
        equality_expr, equality_builder, patient, result
    )

  def test_primitive_equality_for_resource_succeeds(self):
    """Tests FHIRPath and builder '=' operator on primitives."""
    patient = self._new_patient()
    patient.active.value = True
    patient.address.add().city.value = 'Seattle'
    patient.telecom.add().rank.value = 1

    # String equality.
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city = 'Seattle'"),
        self.builder('Patient').address.city == 'Seattle',
        patient,
        True,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city = 'Vancouver'"),
        self.builder('Patient').address.city == 'Vancouver',
        patient,
        False,
    )

    # Boolean equality. Disable some pylint checks because the builder triggers
    # some false positives for extraneous comparisons in this test case, since
    # we artificially are testing a boolean comparison flow that isn't
    # idiomatic
    # pylint:disable=g-explicit-bool-comparison, singleton-comparison
    self.assert_expression_result(
        self.compile_expression('Patient', 'active = true'),
        self.builder('Patient').active == True,
        patient,
        True,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'active = false'),
        self.builder('Patient').active == False,
        patient,
        False,
    )
    # pylint:enable=g-explicit-bool-comparison, singleton-comparison

    # Integer equality.
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank = 1'),
        self.builder('Patient').telecom.rank == 1,
        patient,
        True,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank = 2'),
        self.builder('Patient').telecom.rank == 2,
        patient,
        False,
    )

  def test_primitive_equality_with_empty_returns_empty(self):
    """Tests FHIRPath and builder '=' operator on null."""
    patient = self._new_patient()
    patient.active.value = True

    expr = self.compile_expression('Patient', 'active = {}')
    self.assertEqual(expr.fhir_path, 'active = {}')
    self.assertFalse(expr.evaluate(patient).has_value())

  def test_primitive_inequality_for_resource_succeeds(self):
    """Tests FHIRPath and builder '!=' operator on primitives."""
    patient = self._new_patient()
    patient.active.value = True
    patient.address.add().city.value = 'Seattle'
    patient.telecom.add().rank.value = 1

    # String inequality.
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city != 'Seattle'"),
        self.builder('Patient').address.city != 'Seattle',
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city != 'Vancouver'"),
        self.builder('Patient').address.city != 'Vancouver',
        patient,
        True,
    )

    # Boolean inequality. Disable some pylint checks because this triggers
    # some false positives for extraneous comparisons in this test case, since
    # we artificially are testing a boolean comparison flow that isn't
    # idiomatic.
    # pylint:disable=g-explicit-bool-comparison, singleton-comparison
    self.assert_expression_result(
        self.compile_expression('Patient', 'active != true'),
        self.builder('Patient').active != True,
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'active != false'),
        self.builder('Patient').active != False,
        patient,
        True,
    )
    # pylint:enable=g-explicit-bool-comparison, singleton-comparison

    # Integer inequality.
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank != 1'),
        self.builder('Patient').telecom.rank != 1,
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank != 2'),
        self.builder('Patient').telecom.rank != 2,
        patient,
        True,
    )

  def test_primitive_inequality_with_empty_returns_empty(self):
    """Tests FHIRPath and builder '!=' operator on null."""
    patient = self._new_patient()
    patient.active.value = True

    expr = self.compile_expression('Patient', 'active != {}')
    self.assertEqual(expr.fhir_path, 'active != {}')
    self.assertFalse(expr.evaluate(patient).has_value())

  @parameterized.named_parameters(
      dict(testcase_name='left_larger', left=3, right=2),
      dict(testcase_name='equal', left=3, right=3),
      dict(testcase_name='right_larger', left=2, right=3),
      dict(testcase_name='left_none', left=None, right=3),
      dict(testcase_name='right_none', left=2, right=None),
      dict(testcase_name='both_none', left=None, right=None),
  )
  def test_builder_comparison_for_resource_succeeds(
      self, left: int, right: int
  ):
    """Tests comparison operators with two builder operands."""
    patient = self._new_patient()
    builder = self.builder('Patient')
    gt_expr = self.compile_expression(
        'Patient', 'telecom.rank > contact.telecom.rank'
    )
    gt_builder = builder.telecom.rank > builder.contact.telecom.rank
    lt_expr = self.compile_expression(
        'Patient', 'telecom.rank < contact.telecom.rank'
    )
    lt_builder = builder.telecom.rank < builder.contact.telecom.rank

    ge_expr = self.compile_expression(
        'Patient', 'telecom.rank >= contact.telecom.rank'
    )
    ge_builder = builder.telecom.rank >= builder.contact.telecom.rank
    le_expr = self.compile_expression(
        'Patient', 'telecom.rank <= contact.telecom.rank'
    )
    le_builder = builder.telecom.rank <= builder.contact.telecom.rank

    if left is not None:
      patient.telecom.add().rank.value = left
    if right is not None:
      patient.contact.add().telecom.add().rank.value = right

    if left is None or right is None:
      self.assert_expression_result(gt_expr, gt_builder, patient, None)
      self.assert_expression_result(lt_expr, lt_builder, patient, None)
      self.assert_expression_result(ge_expr, ge_builder, patient, None)
      self.assert_expression_result(le_expr, le_builder, patient, None)

    else:
      self.assert_expression_result(gt_expr, gt_builder, patient, left > right)
      self.assert_expression_result(lt_expr, lt_builder, patient, left < right)
      self.assert_expression_result(ge_expr, ge_builder, patient, left >= right)
      self.assert_expression_result(le_expr, le_builder, patient, left <= right)

  def test_integer_comparison_for_resource_succeeds(self):
    """Tests comparison operator builders."""
    patient = self._new_patient()
    patient.telecom.add().rank.value = 2

    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank > 1'),
        self.builder('Patient').telecom.rank > 1,
        patient,
        True,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank > 3'),
        self.builder('Patient').telecom.rank > 3,
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank > 10'),
        self.builder('Patient').telecom.rank > 10,
        patient,
        False,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank < 1'),
        self.builder('Patient').telecom.rank < 1,
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank < 3'),
        self.builder('Patient').telecom.rank < 3,
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank >= 1'),
        self.builder('Patient').telecom.rank >= 1,
        patient,
        True,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank >= 3'),
        self.builder('Patient').telecom.rank >= 3,
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank >= 2'),
        self.builder('Patient').telecom.rank >= 2,
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank <= 1'),
        self.builder('Patient').telecom.rank <= 1,
        patient,
        False,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank <= 3'),
        self.builder('Patient').telecom.rank <= 3,
        patient,
        True,
    )

  @parameterized.named_parameters(
      dict(testcase_name='normal', left=3, right=2),
      dict(testcase_name='right_zero', left=3, right=0),
      dict(testcase_name='left_none', left=None, right=3),
      dict(testcase_name='right_none', left=2, right=None),
      dict(testcase_name='both_none', left=None, right=None),
  )
  def test_builder_arithmetic_for_resource_succeeds(
      self, left: int, right: int
  ):
    """Tests arithmetic operators with two builder operands."""
    patient = self._new_patient()
    builder = self.builder('Patient')

    add_expr = self.compile_expression(
        'Patient', 'telecom.rank + contact.telecom.rank'
    )
    add_builder = builder.telecom.rank + builder.contact.telecom.rank
    sub_expr = self.compile_expression(
        'Patient', 'telecom.rank - contact.telecom.rank'
    )
    sub_builder = builder.telecom.rank - builder.contact.telecom.rank
    mult_expr = self.compile_expression(
        'Patient', 'telecom.rank * contact.telecom.rank'
    )
    mult_builder = builder.telecom.rank * builder.contact.telecom.rank
    div_expr = self.compile_expression(
        'Patient', 'telecom.rank / contact.telecom.rank'
    )
    div_builder = builder.telecom.rank / builder.contact.telecom.rank
    trunc_div_expr = self.compile_expression(
        'Patient', 'telecom.rank div contact.telecom.rank'
    )
    trunc_div_builder = builder.telecom.rank // builder.contact.telecom.rank
    mod_expr = self.compile_expression(
        'Patient', 'telecom.rank mod contact.telecom.rank'
    )
    mod_builder = builder.telecom.rank % builder.contact.telecom.rank

    if left is not None:
      patient.telecom.add().rank.value = left
    if right is not None:
      patient.contact.add().telecom.add().rank.value = right

    if left is None or right is None:
      self.assert_expression_result(add_expr, add_builder, patient, None)
      self.assert_expression_result(sub_expr, sub_builder, patient, None)
      self.assert_expression_result(mult_expr, mult_builder, patient, None)
      self.assert_expression_result(div_expr, div_builder, patient, None)
      self.assert_expression_result(mod_expr, mod_builder, patient, None)
      self.assert_expression_result(
          trunc_div_expr, trunc_div_builder, patient, None
      )
      return

    if right == 0:
      self.assert_expression_result(div_expr, div_builder, patient, None)
      self.assert_expression_result(mod_expr, mod_builder, patient, None)
      self.assert_expression_result(
          trunc_div_expr, trunc_div_builder, patient, None
      )
    else:
      self.assert_expression_result(
          div_expr, div_builder, patient, float(left / right)
      )
      self.assert_expression_result(
          mod_expr, mod_builder, patient, float(left % right)
      )
      self.assert_expression_result(
          trunc_div_expr, trunc_div_builder, patient, float(left // right)
      )

    self.assert_expression_result(
        add_expr, add_builder, patient, float(left + right)
    )
    self.assert_expression_result(
        sub_expr, sub_builder, patient, float(left - right)
    )
    self.assert_expression_result(
        mult_expr, mult_builder, patient, float(left * right)
    )

  def test_builder_with_none_handles_empty_collection(self):
    """Ensures builders can use None to represent FHIRPath {}."""
    patient = self._new_patient()
    patient.active.value = True

    expr = self.builder('Patient').active == None  # pylint: disable=singleton-comparison
    self.assertEqual(expr.fhir_path, 'active = {}')  # pytype: disable=attribute-error
    compiled = (
        python_compiled_expressions.PythonCompiledExpression.from_builder(expr)  # pytype: disable=wrong-arg-types
    )
    self.assertFalse(compiled.evaluate(patient).has_value())  # pytype: disable=attribute-error

  def test_numeric_addition_arithmetic(self):
    """Tests addition logic for numeric values defined at https://hl7.org/fhirpath/#math-2."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank + 1'),
        self.builder('Patient').telecom.rank + 1,
        patient,
        None,
    )
    patient.telecom.add().rank.value = 2
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank + 1'),
        self.builder('Patient').telecom.rank + 1,
        patient,
        3.0,
    )

  def test_numeric_subtraction_arithmetic(self):
    """Tests subtraction logic for numeric values defined at https://hl7.org/fhirpath/#math-2."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank - 1'),
        self.builder('Patient').telecom.rank - 1,
        patient,
        None,
    )
    patient.telecom.add().rank.value = 2
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank - 1'),
        self.builder('Patient').telecom.rank - 1,
        patient,
        1.0,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank - 4'),
        self.builder('Patient').telecom.rank - 4,
        patient,
        -2.0,
    )

  def test_numeric_division_arithmetic(self):
    """Tests division logic on numeric values defined at https://hl7.org/fhirpath/#math-2."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank / 1'),
        self.builder('Patient').telecom.rank / 1,
        patient,
        None,
    )
    patient.telecom.add().rank.value = 26
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank / 8'),
        self.builder('Patient').telecom.rank / 8,
        patient,
        3.25,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank / 0'),
        self.builder('Patient').telecom.rank / 0,
        patient,
        None,
    )

  def test_numeric_modular_arithmetic(self):
    """Tests modulo logic on numeric values defined at https://hl7.org/fhirpath/#math-2."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank mod 1'),
        self.builder('Patient').telecom.rank % 1,
        patient,
        None,
    )
    patient.telecom.add().rank.value = 23
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank mod 8'),
        self.builder('Patient').telecom.rank % 8,
        patient,
        7.0,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank mod 0'),
        self.builder('Patient').telecom.rank % 0,
        patient,
        None,
    )

  def test_numeric_trunc_div_arithmetic(self):
    """Tests truncated division logic defined at https://hl7.org/fhirpath/#math-2."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank div 1'),
        self.builder('Patient').telecom.rank // 1,
        patient,
        None,
    )
    patient.telecom.add().rank.value = 23
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank div 8'),
        self.builder('Patient').telecom.rank // 8,
        patient,
        2.0,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank div 0'),
        self.builder('Patient').telecom.rank // 0,
        patient,
        None,
    )

  def test_numeric_multiplication_arithmetic(self):
    """Tests multiplication logic on numeric values defined at https://hl7.org/fhirpath/#math-2."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank * 1'),
        self.builder('Patient').telecom.rank * 1,
        patient,
        None,
    )
    patient.telecom.add().rank.value = 8
    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank * 2.4'),
        self.builder('Patient').telecom.rank * 2.4,
        patient,
        19.2,
    )

  def test_non_literal_negative_polarity(self):
    """Tests FHIRPath expressions with negative polarity on non-literals."""
    patient = self._new_patient()
    expr = self.compile_expression(
        'Patient', "-multipleBirth.ofType('Integer')"
    )
    self.assertEqual(expr.fhir_path, "-multipleBirth.ofType('Integer')")
    self.assertFalse(expr.evaluate(patient).has_value())
    patient.multiple_birth.integer.value = 2
    self.assertEqual(expr.evaluate(patient).as_decimal(), -2)
    patient.multiple_birth.integer.value = -47
    self.assertEqual(expr.evaluate(patient).as_decimal(), 47)

  def test_non_literal_positive_polarity(self):
    """Tests FHIRPath expressions with positive polarity on non-literals."""
    patient = self._new_patient()
    expr = self.compile_expression(
        'Patient', "+multipleBirth.ofType('Integer')"
    )
    self.assertEqual(expr.fhir_path, "+multipleBirth.ofType('Integer')")
    self.assertFalse(expr.evaluate(patient).has_value())
    patient.multiple_birth.integer.value = 2
    self.assertEqual(expr.evaluate(patient).as_decimal(), 2)
    patient.multiple_birth.integer.value = -47
    self.assertEqual(expr.evaluate(patient).as_decimal(), -47)

  def test_string_arithmetic(self):
    """Tests string addition and concatenation logic defined at https://hl7.org/fhirpath/#math-2."""
    patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city + '1'"),
        self.builder('Patient').address.city + '1',
        patient,
        None,
    )
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city & '1'"),
        self.builder('Patient').address.city & '1',
        patient,
        '1',
    )

    patient.address.add().city.value = 'Seattle'
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city + '1'"),
        self.builder('Patient').address.city + '1',
        patient,
        'Seattle1',
    )
    self.assert_expression_result(
        self.compile_expression('Patient', "address.city & '1'"),
        self.builder('Patient').address.city & '1',
        patient,
        'Seattle1',
    )

  @parameterized.named_parameters(
      dict(testcase_name='true_or_true', left=True, right=True, result=True),
      dict(testcase_name='true_or_false', left=True, right=False, result=True),
      dict(testcase_name='false_or_true', left=False, right=True, result=True),
      dict(
          testcase_name='false_or_false', left=False, right=False, result=False
      ),
      dict(testcase_name='true_or_none', left=True, right=None, result=True),
      dict(testcase_name='none_or_true', left=None, right=True, result=True),
      dict(testcase_name='none_or_none', left=None, right=None, result=None),
  )
  def test_boolean_or(self, left: bool, right: bool, result: bool):
    """Tests logic defined at https://hl7.org/fhirpath/#boolean-logic."""
    patient = self._new_patient()
    builder = self.builder('Patient')
    active_or_deceased_expr = self.compile_expression(
        'Patient', "active or deceased.ofType('FHIR.boolean')"
    )
    active_or_deceased_builder = builder.active | builder.deceased.ofType(
        'FHIR.boolean'
    )

    if left is not None:
      patient.active.value = left
    if right is not None:
      patient.deceased.boolean.value = right
    self.assert_expression_result(
        active_or_deceased_expr, active_or_deceased_builder, patient, result
    )

  @parameterized.named_parameters(
      dict(testcase_name='true_and_true', left=True, right=True, result=True),
      dict(
          testcase_name='true_and_false', left=True, right=False, result=False
      ),
      dict(
          testcase_name='false_and_true', left=False, right=True, result=False
      ),
      dict(
          testcase_name='false_and_false', left=False, right=False, result=False
      ),
      dict(testcase_name='true_and_none', left=True, right=None, result=None),
      dict(testcase_name='none_and_true', left=None, right=True, result=None),
      dict(
          testcase_name='none_and_false', left=None, right=False, result=False
      ),
      dict(testcase_name='none_and_none', left=None, right=None, result=None),
  )
  def test_boolean_and(self, left: bool, right: bool, result: bool):
    """Tests logic defined at https://hl7.org/fhirpath/#boolean-logic."""
    patient = self._new_patient()
    builder = self.builder('Patient')
    active_or_deceased_expr = self.compile_expression(
        'Patient', "active and deceased.ofType('FHIR.boolean')"
    )
    active_or_deceased_builder = builder.active & builder.deceased.ofType(
        'FHIR.boolean'
    )

    if left is not None:
      patient.active.value = left
    if right is not None:
      patient.deceased.boolean.value = right
    self.assert_expression_result(
        active_or_deceased_expr, active_or_deceased_builder, patient, result
    )

  @parameterized.named_parameters(
      dict(testcase_name='true_xor_true', left=True, right=True, result=False),
      dict(testcase_name='true_xor_false', left=True, right=False, result=True),
      dict(testcase_name='false_xor_true', left=False, right=True, result=True),
      dict(
          testcase_name='false_xor_false', left=False, right=False, result=False
      ),
      dict(testcase_name='true_xor_none', left=True, right=None, result=None),
      dict(testcase_name='none_xor_true', left=None, right=True, result=None),
      dict(testcase_name='none_xor_none', left=None, right=None, result=None),
  )
  def test_boolean_xor(self, left: bool, right: bool, result: bool):
    """Tests logic defined at https://hl7.org/fhirpath/#boolean-logic."""
    patient = self._new_patient()
    builder = self.builder('Patient')
    active_or_deceased_expr = self.compile_expression(
        'Patient', "active xor deceased.ofType('FHIR.boolean')"
    )
    active_or_deceased_builder = builder.active ^ builder.deceased.ofType(
        'FHIR.boolean'
    )

    if left is not None:
      patient.active.value = left
    if right is not None:
      patient.deceased.boolean.value = right
    self.assert_expression_result(
        active_or_deceased_expr, active_or_deceased_builder, patient, result
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='true_implies_true', left=True, right=True, result=True
      ),
      dict(
          testcase_name='true_implies_false',
          left=True,
          right=False,
          result=False,
      ),
      dict(
          testcase_name='false_implies_true',
          left=False,
          right=True,
          result=True,
      ),
      dict(
          testcase_name='false_implies_false',
          left=False,
          right=False,
          result=True,
      ),
      dict(
          testcase_name='true_implies_none', left=True, right=None, result=None
      ),
      dict(
          testcase_name='none_implies_true', left=None, right=True, result=True
      ),
      dict(
          testcase_name='none_implies_none', left=None, right=None, result=None
      ),
      dict(
          testcase_name='none_implies_false',
          left=None,
          right=False,
          result=None,
      ),
  )
  def test_boolean_implies(self, left: bool, right: bool, result: bool):
    """Tests logic defined at https://hl7.org/fhirpath/#boolean-logic."""
    patient = self._new_patient()
    builder = self.builder('Patient')
    active_or_deceased_expr = self.compile_expression(
        'Patient', "active implies deceased.ofType('FHIR.boolean')"
    )
    active_or_deceased_builder = builder.active.implies(
        builder.deceased.ofType('FHIR.boolean')
    )

    if left is not None:
      patient.active.value = left
    if right is not None:
      patient.deceased.boolean.value = right
    self.assert_expression_result(
        active_or_deceased_expr, active_or_deceased_builder, patient, result
    )

  def test_string_comparison_for_resource_succeeds(self):
    """Tests string comparison builders."""
    patient = self._new_patient()
    patient.address.add().state.value = 'NY'

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state > 'CA'"),
        self.builder('Patient').address.state > 'CA',
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state > 'WA'"),
        self.builder('Patient').address.state > 'WA',
        patient,
        False,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state < 'CA'"),
        self.builder('Patient').address.state < 'CA',
        patient,
        False,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state < 'WA'"),
        self.builder('Patient').address.state < 'WA',
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state >= 'CA'"),
        self.builder('Patient').address.state >= 'CA',
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state >= 'NY'"),
        self.builder('Patient').address.state >= 'NY',
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state >= 'WA'"),
        self.builder('Patient').address.state >= 'WA',
        patient,
        False,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state <= 'CA'"),
        self.builder('Patient').address.state <= 'CA',
        patient,
        False,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state <= 'WA'"),
        self.builder('Patient').address.state <= 'WA',
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.state <= 'NY'"),
        self.builder('Patient').address.state <= 'NY',
        patient,
        True,
    )

  def _to_value_us(self, dt: datetime.datetime) -> int:
    delta = dt - _UNIX_EPOCH
    return int(delta.total_seconds() * 1e6)

  def test_date_comparison_for_resource_succeeds(self):
    """Tests date comparison builders."""
    patient = self._new_patient()
    patient.birth_date.value_us = self._to_value_us(
        datetime.datetime(1949, 5, 27, tzinfo=datetime.timezone.utc)
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'birthDate < @1950-01-01'),
        self.builder('Patient').birthDate < datetime.date(1950, 1, 1),
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'birthDate < @1940-01-01'),
        self.builder('Patient').birthDate < datetime.date(1940, 1, 1),
        patient,
        False,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'birthDate > @1950-01-01'),
        self.builder('Patient').birthDate > datetime.date(1950, 1, 1),
        patient,
        False,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'birthDate > @1940-01-01'),
        self.builder('Patient').birthDate > datetime.date(1940, 1, 1),
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', 'birthDate < @1950-01-01T00:00:00'),
        self.builder('Patient').birthDate < datetime.datetime(1950, 1, 1),
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Patient', 'birthDate < @1950-01-01T00:00:00+00:00'
        ),
        self.builder('Patient').birthDate
        < datetime.datetime(1950, 1, 1, tzinfo=datetime.timezone.utc),
        patient,
        True,
    )

  def test_quantity_comparison_for_resource_succeeds(self):
    """Tests quantity comparisons against a resource."""
    observation = self._new_observation()
    observation.value.quantity.value.value = '1.0'
    observation.value.quantity.unit.value = 'g'

    observation_quantity = self.builder('Observation').value.ofType('Quantity')

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') < 0 'g'"
        ),
        observation_quantity
        < quantity.Quantity(value=decimal.Decimal(0), unit='g'),
        observation,
        False,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') < 2 'g'"
        ),
        observation_quantity
        < quantity.Quantity(value=decimal.Decimal(2), unit='g'),
        observation,
        True,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') <= 2 'g'"
        ),
        observation_quantity
        <= quantity.Quantity(value=decimal.Decimal(2), unit='g'),
        observation,
        True,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') <= 0 'g'"
        ),
        observation_quantity
        <= quantity.Quantity(value=decimal.Decimal(0), unit='g'),
        observation,
        False,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') > 0 'g'"
        ),
        observation_quantity
        > quantity.Quantity(value=decimal.Decimal(0), unit='g'),
        observation,
        True,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') > 2 'g'"
        ),
        observation_quantity
        > quantity.Quantity(value=decimal.Decimal(2), unit='g'),
        observation,
        False,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') >= 2 'g'"
        ),
        observation_quantity
        >= quantity.Quantity(value=decimal.Decimal(2), unit='g'),
        observation,
        False,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') >= 0 'g'"
        ),
        observation_quantity
        >= quantity.Quantity(value=decimal.Decimal(0), unit='g'),
        observation,
        True,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') = 2 'g'"
        ),
        observation_quantity
        == quantity.Quantity(value=decimal.Decimal(2), unit='g'),
        observation,
        False,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Observation', "value.ofType('Quantity') = 1 'g'"
        ),
        observation_quantity
        == quantity.Quantity(value=decimal.Decimal(1.0), unit='g'),
        observation,
        True,
    )

  def test_none_comparison_for_resource_has_no_value(self):
    patient = self._new_patient()
    expr = python_compiled_expressions.PythonCompiledExpression.from_builder(
        self.builder('Patient').address.state > 'CA'
    )
    self.assertFalse(expr.evaluate(patient).has_value())

  def test_root_fields_on_builder_match_fhir_fields(self):
    patient_fields = dir(self.builder('Patient'))
    self.assertIn('active', patient_fields)

    # Check choice type field shorthand.
    self.assertIn('multipleBirth', patient_fields)
    self.assertIn('multipleBirthBoolean', patient_fields)
    self.assertIn('multipleBirthInteger', patient_fields)

    self.assertNotIn('bogusField', patient_fields)

  def test_nested_fields_on_builder_match_fhir_fields(self):
    builder = self.builder('Patient')
    self.assertIn('address', dir(builder.contact))
    self.assertIn('relationship', dir(builder.contact))
    self.assertIn('given', dir(builder.contact.name))
    self.assertNotIn('bogusField', dir(builder.contact))

  def test_methods_on_builder_dir(self):
    builder = self.builder('Patient')
    self.assertIn('fhir_path', dir(builder))

  def test_member_of_resolving_valueset_succeeds(self):
    """Tests valueset usage when resolving one from context."""
    observation = self._new_observation()
    coding = observation.code.coding.add()
    coding.system.value = 'http://loinc.org'
    coding.code.value = '10346-5'

    # Create a valueset and add it to the context so it is resolved
    # in memberOf evaluation.
    hba1c_valueset_uri = 'url:test:valueset'
    value_set = (
        self.value_set_builder(hba1c_valueset_uri)
        .with_codes('http://loinc.org', ['10346-5', '10486-9'])
        .build()
    )

    self.context().add_local_value_set(value_set)

    parsed_expr = self.compile_expression(
        'Observation', f"code.memberOf('{hba1c_valueset_uri}')"
    )
    built_expr = self.builder('Observation').code.memberOf(hba1c_valueset_uri)

    self.assert_expression_result(parsed_expr, built_expr, observation, True)
    coding.code.value = '10486-9'
    self.assert_expression_result(parsed_expr, built_expr, observation, True)

    # Confirm non-matching value returns false.
    coding.code.value = '10500-7'
    self.assert_expression_result(parsed_expr, built_expr, observation, False)

  def test_member_of_with_literal_valueset_succeeds(self):
    """Tests valueset usage when given a literal."""
    observation = self._new_observation()
    coding = observation.code.coding.add()
    coding.system.value = 'http://loinc.org'
    coding.code.value = '10346-5'

    value_set = (
        self.value_set_builder('url:test:valueset')
        .with_codes('http://loinc.org', ['10346-5', '10486-9'])
        .build()
    )

    expr = python_compiled_expressions.PythonCompiledExpression.from_builder(
        self.builder('Observation').code.memberOf(value_set)
    )

    # Check matches on multiple code values.
    self.assertTrue(expr.evaluate(observation).as_bool())
    coding.code.value = '10486-9'
    self.assertTrue(expr.evaluate(observation).as_bool())

    # Confirm non-matching value returns false.
    coding.code.value = '10500-7'
    self.assertFalse(expr.evaluate(observation).as_bool())

  def test_choice_type_with_direct_access_succeeds(self) -> None:
    """Tests direct use of a choice type succeeds."""
    observation = self._new_observation()
    compiled_expr = self.compile_expression('Observation', 'value')
    built_expr = self.builder('Observation').value

    self.assertFalse(compiled_expr.evaluate(observation).has_value())

    observation.value.string_value.value = 'foo'
    self.assert_expression_result(compiled_expr, built_expr, observation, 'foo')

    observation.value.boolean.value = True
    self.assert_expression_result(compiled_expr, built_expr, observation, True)

  def test_choice_type_without_of_type_fails(self) -> None:
    # Choice types should only be accessed directly or with ofType.
    with self.assertRaisesRegex(
        AttributeError,
        r'Cannot directly access polymorphic fields. '
        r"Please use ofType\['quantity'\] instead.",
    ):
      _ = self.builder('Observation').value.quantity  # pylint: disable=pointless-statement

  def test_choice_type_with_of_type_succeeds(self) -> None:
    """Tests ofType access of choice types succeeds."""
    observation = self._new_observation()
    compiled_expr = self.compile_expression(
        'Observation', "value.ofType('FHIR.string')"
    )
    built_expr = self.builder('Observation').value.ofType('FHIR.string')

    self.assertFalse(compiled_expr.evaluate(observation).has_value())

    # Eval result should be filtered since it does not match the desired type.
    observation.value.boolean.value = True
    self.assertFalse(compiled_expr.evaluate(observation).has_value())

    observation.value.string_value.value = 'foo'
    self.assert_expression_result(compiled_expr, built_expr, observation, 'foo')

  def test_choice_type_with_choice_type_shorthand_succeeds(self) -> None:
    """Tests shorthand for ofType use of choice types succeeds."""
    observation = self._new_observation()
    observation.value.string_value.value = 'foo'
    compiled_expr = self.compile_expression(
        'Observation', "value.ofType('string')"
    )
    built_expr = self.builder('Observation').valueString
    self.assert_expression_result(compiled_expr, built_expr, observation, 'foo')

  def test_choice_type_with_invalid_shorthand_fails(self) -> None:
    """Tests incorrect shorthand throws an exception with the right fields."""
    with self.assertRaisesRegex(
        AttributeError, r'.*valueCodable.*valueCodeable.*'
    ):
      _ = self.builder('Observation').valueCodable  # pylint: disable=pointless-statement

  def test_choice_type_with_oftype_returns_expected_fields(self) -> None:
    """Ensure ofType operations return expected child node type."""
    self.assertContainsSubset(
        ['value', 'unit', 'system', 'code'],
        dir(self.builder('Observation').value.ofType('Quantity')),
    )
    self.assertNoCommonElements(
        ['coding', 'text'],
        dir(self.builder('Observation').value.ofType('Quantity')),
    )

    self.assertContainsSubset(
        ['coding', 'text'],
        dir(self.builder('Observation').value.ofType('CodeableConcept')),
    )
    self.assertNoCommonElements(
        ['value', 'unit', 'system', 'code'],
        dir(self.builder('Observation').value.ofType('CodeableConcept')),
    )

    self.assertNoCommonElements(
        ['coding', 'text', 'value', 'unit', 'system', 'code'],
        dir(self.builder('Observation').value.ofType('string')),
    )

  def test_choice_type_with_shorthand_returns_expected_fields(self) -> None:
    """Ensure ofType operations return expected child node type."""
    self.assertContainsSubset(
        ['value', 'unit', 'system', 'code'],
        dir(self.builder('Observation').valueQuantity),
    )
    self.assertContainsSubset(
        ['coding', 'text'],
        dir(self.builder('Observation').valueCodeableConcept),
    )

  def test_to_integer_with_coercible_string_returns_integer(self) -> None:
    patient = self._new_patient()
    address = patient.address.add()
    address.state.value = '123'

    self.assert_expression_result(
        self.compile_expression('Patient', 'address.state.toInteger()'),
        self.builder('Patient').address.state.toInteger(),
        patient,
        123,
    )

  def test_to_integer_with_non_coercible_string_returns_empty(self) -> None:
    patient = self._new_patient()
    address = patient.address.add()
    address.state.value = 'abc'

    self.assert_expression_result(
        self.compile_expression('Patient', 'address.state.toInteger()'),
        self.builder('Patient').address.state.toInteger(),
        patient,
        None,
    )

  def test_to_integer_with_true_boolean_returns_one(self) -> None:
    patient = self._new_patient()
    patient.active.value = True

    self.assert_expression_result(
        self.compile_expression('Patient', 'active.toInteger()'),
        self.builder('Patient').active.toInteger(),
        patient,
        1,
    )

  def test_to_integer_with_false_boolean_returns0(self) -> None:
    patient = self._new_patient()
    patient.active.value = False

    self.assert_expression_result(
        self.compile_expression('Patient', 'active.toInteger()'),
        self.builder('Patient').active.toInteger(),
        patient,
        0,
    )

  def test_to_integer_with_integer_returns_integer(self) -> None:
    patient = self._new_patient()
    patient.telecom.add().rank.value = 3

    self.assert_expression_result(
        self.compile_expression('Patient', 'telecom.rank.toInteger()'),
        self.builder('Patient').telecom.rank.toInteger(),
        patient,
        3,
    )

  def test_to_integer_with_non_coercible_type_returns_emepty(self) -> None:
    patient = self._new_patient()
    patient.gender.value = 1

    self.assert_expression_result(
        self.compile_expression('Patient', 'gender.toInteger()'),
        self.builder('Patient').gender.toInteger(),
        patient,
        None,
    )

  def test_to_integer_with_collection_more_than_one_element_raises_error(
      self,
  ) -> None:
    """Ensures an error is raised for collections with more than one element."""
    patient = self._new_patient()
    address1 = patient.address.add()
    address1.state.value = '123'

    address2 = patient.address.add()
    address2.state.value = '456'

    builder = self.builder('Patient').address.state.toInteger()
    with self.assertRaises(ValueError):
      python_compiled_expressions.PythonCompiledExpression.from_builder(
          builder
      ).evaluate(patient)

  def test_not_function_succeeds(self) -> None:
    """Tests not_()."""
    patient = self._new_patient()
    # Check builder and expression return correct results when the field does
    # not exist.
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.exists().not()'),
        self.builder('Patient').active.exists().not_(),
        patient,
        True,
    )

    # Check builder and expression return correct results when the field exists.
    patient.active.value = True
    self.assert_expression_result(
        self.compile_expression('Patient', 'active.exists().not()'),
        self.builder('Patient').active.exists().not_(),
        patient,
        False,
    )

    # Tests edge case that function at the root node should still work.
    self.assert_expression_result(
        self.compile_expression('Patient', 'exists().not()'),
        self.builder('Patient').exists().not_(),
        patient,
        False,
    )

  def test_fhir_path_enum_value_converts_to_string(self):
    """Tests enum-based code values converts to expected FHIR strings."""
    patient = self._new_patient()
    home_address = patient.address.add()
    home_address.state.value = 'WA'
    self._set_fhir_enum_by_name(home_address.use, 'HOME')

    self.assert_expression_result(
        self.compile_expression('Patient', 'address.use'),
        self.builder('Patient').address.use,
        patient,
        'home',
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.use = 'home'"),
        self.builder('Patient').address.use == 'home',
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.use = 'work'"),
        self.builder('Patient').address.use == 'work',
        patient,
        False,
    )

  # TODO(b/226131331): Expand tests to nested $this when that is added.
  def test_where_expression_succeeds(self):
    """Test FHIRPath where() expressions."""
    patient = self._new_patient()
    home_address = patient.address.add()
    home_address.city.value = 'Home City'
    self._set_fhir_enum_by_name(home_address.use, 'HOME')

    work_address = patient.address.add()
    work_address.city.value = 'Work City'
    self._set_fhir_enum_by_name(work_address.use, 'WORK')

    old_address = patient.address.add()
    old_address.city.value = 'Old City'
    self._set_fhir_enum_by_name(old_address.use, 'OLD')

    # Simple where filter.
    pat = self.builder('Patient')
    self.assert_expression_result(
        self.compile_expression('Patient', "address.where(use = 'home').city"),
        pat.address.where(pat.address.use == 'home').city,
        patient,
        'Home City',
    )

    # Function within where filter.
    pat = self.builder('Patient')
    self.assert_expression_result(
        self.compile_expression(
            'Patient', 'address.where(use.exists()).count()'
        ),
        pat.address.where(pat.address.use.exists()).count(),
        patient,
        3,
    )

    # Expression within the where filter.
    self.assert_expression_result(
        self.compile_expression(
            'Patient', "address.where(use = 'home' or use = 'work').count()"
        ),
        pat.address.where(
            (pat.address.use == 'home') | (pat.address.use == 'work')
        ).count(),
        patient,
        2,
    )

    # Where filter with no results.
    self.assert_expression_result(
        self.compile_expression(
            'Patient', "address.where(use = 'temp').exists()"
        ),
        pat.address.where(pat.address.use == 'temp').exists(),
        patient,
        False,
    )

    # Nested where expression.
    period = pat.address.period
    self.assert_expression_result(
        self.compile_expression(
            'Patient',
            'address.where(period.where(start.exists()).exists()).count()',
        ),
        pat.address.where(period.where(period.start.exists()).exists()).count(),
        patient,
        0,
    )

  def test_where_function_builder_preserves_fields(self):
    pat = self.builder('Patient')
    self.assertContainsSubset(
        ['use', 'line', 'city', 'state', 'postalCode'],
        dir(pat.address.where(pat.address.use == 'home')),
    )

  def test_where_function_builder_rejects_non_boolean_predicate(self):
    pat = self.builder('Patient')
    with self.assertRaises(ValueError):
      pat.address.where(pat.address.use)

  def test_all_expression_succeeds(self):
    """Test FHIRPath all() expressions."""
    patient = self._new_patient()
    home_address = patient.address.add()
    home_address.city.value = 'Home City'
    self._set_fhir_enum_by_name(home_address.use, 'HOME')

    work_address = patient.address.add()
    work_address.city.value = 'Work City'
    self._set_fhir_enum_by_name(work_address.use, 'WORK')

    old_address = patient.address.add()
    old_address.city.value = 'Old City'
    self._set_fhir_enum_by_name(old_address.use, 'OLD')

    # Some but not all items match.
    pat = self.builder('Patient')
    self.assert_expression_result(
        self.compile_expression('Patient', "address.all(use = 'home')"),
        pat.address.all(pat.address.use == 'home'),
        patient,
        False,
    )

    # All items match parent reference.
    pat = self.builder('Patient')
    self.assert_expression_result(
        self.compile_expression(
            'Patient', "address.city.all($this.matches('[a-zA-Z]* City'))"
        ),
        pat.address.city.all(pat.address.city.matches('[a-zA-Z]* City')),
        patient,
        True,
    )

    # All items match.
    pat = self.builder('Patient')
    self.assert_expression_result(
        self.compile_expression('Patient', 'address.all(use.exists())'),
        pat.address.all(pat.address.use.exists()),
        patient,
        True,
    )

    # There are no items to match, so all(...) should be true.
    empty_patient = self._new_patient()
    self.assert_expression_result(
        self.compile_expression('Patient', 'address.all(use.exists())'),
        pat.address.all(pat.address.use.exists()),
        empty_patient,
        True,
    )

  def test_all_function_builder_rejects_non_boolean_predicate(self):
    pat = self.builder('Patient')
    with self.assertRaises(ValueError):
      pat.address.all(pat.address.use)

  def test_contains_operator_succeeds(self):
    """Ensures contains indicates an elements presence in a collection."""
    patient = self._new_patient()

    address1 = patient.address.add()
    address1.city.value = 'city1'

    address2 = patient.address.add()
    address2.city.value = 'city2'

    pat = self.builder('Patient')

    self.assert_expression_result(
        self.compile_expression('Patient', "address.city contains 'city1'"),
        pat.address.city.contains('city1'),
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression('Patient', "address.city contains 'city2'"),
        pat.address.city.contains('city2'),
        patient,
        True,
    )

    self.assert_expression_result(
        self.compile_expression(
            'Patient', "address.city contains 'mystery_city'"
        ),
        pat.address.city.contains('mystery_city'),
        patient,
        False,
    )

  def test_contains_operator_with_empty_collection_returns_false(self):
    """Ensures contains returns False when called against empty collections."""
    patient = self._new_patient()
    pat = self.builder('Patient')

    self.assert_expression_result(
        self.compile_expression('Patient', "address.city contains 'city1'"),
        pat.address.city.contains('city1'),
        patient,
        False,
    )

  def test_contains_operator_with_empty_element_returns_empty(self):
    """Ensures contains returns empty when the rhs is empty."""
    patient = self._new_patient()

    address1 = patient.address.add()
    address1.city.value = 'city1'

    expression = self.compile_expression('Patient', 'address.city contains {}')
    self.assertFalse(expression.evaluate(patient).has_value())

  def test_contains_operator_with_non_element_operand_raises_error(self):
    """Ensures contains raises an error when the rhs is not a single value."""
    patient = self._new_patient()

    address1 = patient.address.add()
    address1.city.value = 'city1'

    address2 = patient.address.add()
    address2.city.value = 'city2'

    pat = self.builder('Patient')
    builder = pat.address.city.contains(pat.address.city)
    with self.assertRaises(ValueError):
      python_compiled_expressions.PythonCompiledExpression.from_builder(
          builder
      ).evaluate(patient)

  def test_contains_operator_with_nesting_in_where_succeeds(self):
    """Ensures contains can be nested in a where function."""
    patient = self._new_patient()

    first_name = patient.name.add()
    first_name.given.add().value = 'namey'

    address1 = patient.address.add()
    address1.city.value = 'city1'

    pat = self.builder('Patient')
    self.assert_expression_result(
        self.compile_expression(
            'Patient', "where(address.city contains 'city1').name.first().given"
        ),
        pat.where(pat.address.city.contains('city1')).name.first().given,
        patient,
        'namey',
    )

  def test_in_operator_succeeds(self):
    """Ensures "in" indicates an elements presence in a collection.

    We don't offer a builder for 'in' operators (yet?) so we don't test builder
    operations here.
    """
    patient = self._new_patient()

    address1 = patient.address.add()
    address1.city.value = 'city1'

    address2 = patient.address.add()
    address2.city.value = 'city2'

    expr = self.compile_expression('Patient', "'city1' in address.city")
    self.assertTrue(expr.evaluate(patient).as_bool())
    self.assertEqual(expr.fhir_path, "'city1' in address.city")

    self.assertTrue(
        self.compile_expression('Patient', "'city2' in address.city")
        .evaluate(patient)
        .as_bool()
    )
    self.assertFalse(
        self.compile_expression('Patient', "'mystery_city' in address.city")
        .evaluate(patient)
        .as_bool()
    )

  def test_in_operator_with_empty_collection_returns_false(self):
    """Ensures "in" returns False when called against empty collections.

    We don't offer a builder for 'in' operators (yet?) so we don't test builder
    operations here.
    """
    patient = self._new_patient()

    self.assertFalse(
        self.compile_expression('Patient', "'city1' in address.city")
        .evaluate(patient)
        .as_bool()
    )

  def test_in_operator_with_empty_element_returns_empty(self):
    """Ensures "in" returns empty when the lhs is empty.

    We don't offer a builder for 'in' operators (yet?) so we don't test builder
    operations here.
    """
    patient = self._new_patient()

    address1 = patient.address.add()
    address1.city.value = 'city1'

    self.assertFalse(
        self.compile_expression('Patient', '{} in address.city')
        .evaluate(patient)
        .has_value()
    )

  def test_in_operator_with_non_element_operand_raises_error(self):
    """Ensures "in" raises an error when the lhs is not a single value.

    We don't offer a builder for 'in' operators (yet?) so we don't test builder
    operations here.
    """
    patient = self._new_patient()

    address1 = patient.address.add()
    address1.city.value = 'city1'

    address2 = patient.address.add()
    address2.city.value = 'city2'

    with self.assertRaises(ValueError):
      self.compile_expression(
          'Patient', 'address.city in address.city'
      ).evaluate(patient)

  def test_union_operator_with_homogeneous_collections_succeeds(self):
    """Ensures "union" works with collections of the same type."""
    patient = self._new_patient()

    address1 = patient.address.add()
    address1.city.value = 'a'

    address2 = patient.address.add()
    address2.city.value = 'b'

    first_name = patient.name.add()
    first_name.given.add().value = 'b'
    first_name.given.add().value = 'c'

    pat = self.builder('Patient')
    builder = pat.address.city.union(pat.name.given)
    fhir_path_expr = self.compile_expression(
        'Patient', 'address.city | name.given)'
    )

    self.assertEqual(builder.fhir_path, 'address.city | name.given')
    self.assertEqual(builder.node.return_type, _fhir_path_data_types.String)

    builder_expr = (
        python_compiled_expressions.PythonCompiledExpression.from_builder(
            builder
        )
    )
    for result in (
        builder_expr.evaluate(patient),
        fhir_path_expr.evaluate(patient),
    ):
      self.assertCountEqual(
          [
              proto_utils.get_value_at_field(message, 'value')
              for message in result.messages
          ],
          ['a', 'b', 'c'],
      )

  def test_union_operator_with_heterogeneous_collections_succeeds(self):
    """Ensures "union" works with collections of different types."""
    patient = self._new_patient()

    first_name = patient.name.add()
    first_name.given.add().value = 'a'
    first_name.given.add().value = 'b'

    patient.telecom.add().rank.value = 1
    patient.telecom.add().rank.value = 2

    pat = self.builder('Patient')
    builder = pat.telecom.rank.union(pat.name.given)
    fhir_path_expr = self.compile_expression(
        'Patient', 'telecom.rank | name.given'
    )

    self.assertEqual(builder.fhir_path, 'telecom.rank | name.given')
    self.assertEqual(
        builder.node.return_type,
        _fhir_path_data_types.Collection(
            types={
                _fhir_path_data_types.String,
                _fhir_path_data_types.Integer,
            }
        ),
    )

    builder_expr = (
        python_compiled_expressions.PythonCompiledExpression.from_builder(
            builder
        )
    )
    for result in (
        builder_expr.evaluate(patient),
        fhir_path_expr.evaluate(patient),
    ):
      self.assertCountEqual(
          [
              proto_utils.get_value_at_field(message, 'value')
              for message in result.messages
          ],
          ['a', 'b', 1, 2],
      )

  def test_get_parent_builder_succeeds(self):
    """Tests getting the parent builder."""
    pat = self.builder('Patient')
    builder_expr = pat.name.given

    self.assertEqual(
        builder_expr.get_parent_builder().fhir_path, pat.name.fhir_path
    )

    builder_expr = pat.name.given.exists()
    self.assertEqual(
        builder_expr.get_parent_builder().fhir_path, pat.name.given.fhir_path
    )

  def test_get_root_builder_of_function_succeeds(self):
    """Tests getting a root builder for a builder."""
    pat = self.builder('Patient')
    builder_expr = pat.name.given

    self.assertEqual(builder_expr.get_root_builder().fhir_path, 'Patient')

  def test_resolve_nested_reference_succeeds(self):
    """"Tests that FHIRPath expressions properly follow nested references."""
    # component.referenceRange is a reference to the base backbone element.
    builder = self.builder('Observation').component.referenceRange.exists()

    builder_expr = (
        python_compiled_expressions.PythonCompiledExpression.from_builder(
            builder
        )
    )

    obs = self._new_observation()
    self.assertFalse(builder_expr.evaluate(obs).as_bool())

    obs.component.add().reference_range.add().low.value.value = '1.0'
    self.assertTrue(builder_expr.evaluate(obs).as_bool())

  def test_union_operator_with_empty_collections_succeeds(self):
    """Ensures "union" works with empty collections."""
    patient = self._new_patient()

    first_name = patient.name.add()
    first_name.given.add().value = 'a'
    first_name.given.add().value = 'b'

    pat = self.builder('Patient')
    builder = pat.name.given.union(None)  # pytype: disable=wrong-arg-types
    fhir_path_expr = self.compile_expression('Patient', 'name.given | {}')

    self.assertEqual(builder.fhir_path, 'name.given | {}')
    self.assertEqual(builder.node.return_type, _fhir_path_data_types.String)

    builder_expr = (
        python_compiled_expressions.PythonCompiledExpression.from_builder(
            builder
        )
    )
    for result in (
        builder_expr.evaluate(patient),
        fhir_path_expr.evaluate(patient),
    ):
      self.assertCountEqual(
          [
              proto_utils.get_value_at_field(message, 'value')
              for message in result.messages
          ],
          ['a', 'b'],
      )

  def test_union_operator_with_both_empty_collections_succeeds(self):
    """Ensures "union" returns empty when given two empties."""
    patient = self._new_patient()
    expr = self.compile_expression('Patient', '{} | {}')
    self.assertEqual(expr.evaluate(patient).messages, [])

  def test_multiple_resource_builder(self):
    """Test multiple resources in one builder."""
    multi_resource = (
        self.builder('Patient').name.first().family
        == self.builder('Encounter').status
    )
    self.assertMultiLineEqual(
        textwrap.dedent("""\
          + name.first().family = status <EqualityNode> (
          | + name.first().family <InvokeExpressionNode> (
          | | + name.first() <FirstFunction> (
          | | | + name <InvokeExpressionNode> (
          | | | | + Patient <RootMessageNode> ())))
          | + status <InvokeExpressionNode> (
          | | + Encounter <RootMessageNode> ()))"""),
        multi_resource.debug_string(),
    )

    self.assertSameElements(
        ['Patient', 'Encounter'],
        [p.fhir_path for p in multi_resource.get_resource_builders()],
    )

  def test_replace_operand(self):
    """Tests replace_operand of a builder."""
    original = self.builder('Patient').name.where(
        self.builder('Patient').name.given == 'Name'
    )
    self.assertSameElements(
        ['Patient'], [p.fhir_path for p in original.get_resource_builders()]
    )
    self.assertEqual(original.fhir_path, "name.where(given = 'Name')")
    self.assertEqual(
        textwrap.dedent("""\
          + name.where(given = 'Name') <WhereFunction> (
          | + name <InvokeExpressionNode> (
          | | + Patient <RootMessageNode> ())
          | + given = 'Name' <EqualityNode> (
          | | + given <InvokeExpressionNode> (
          | | | + <ReferenceNode> (&name))
          | | + 'Name' <LiteralNode> ()))"""),
        original.debug_string(),
    )

    # Replace referenced node with another builder.
    replacement = self.builder('Patient').name.family
    new_builder = expressions.Builder.replace_with_operand(
        original, old_path='given', replacement_node=replacement.node
    )

    self.assertEqual(
        textwrap.dedent("""\
          + name.where(name.family = 'Name') <WhereFunction> (
          | + name <InvokeExpressionNode> (
          | | + Patient <RootMessageNode> ())
          | + name.family = 'Name' <EqualityNode> (
          | | + name.family <InvokeExpressionNode> (
          | | | + name <InvokeExpressionNode> (
          | | | | + Patient <RootMessageNode> ()))
          | | + 'Name' <LiteralNode> ()))"""),
        new_builder.debug_string(),
    )
    self.assertSameElements(
        ['Patient'], [p.fhir_path for p in new_builder.get_resource_builders()]
    )
    self.assertEqual(new_builder.fhir_path, "name.where(name.family = 'Name')")
    self.assertIs(new_builder.node.context, original.node.context)
    self.assertIs(new_builder.node.return_type, original.node.return_type)

    # Replace with a builder with a different resource.
    replacement = self.builder('Encounter').status
    new_builder = expressions.Builder.replace_with_operand(
        original, old_path="'Name'", replacement_node=replacement.node
    )

    self.assertEqual(
        textwrap.dedent("""\
          + name.where(given = status) <WhereFunction> (
          | + name <InvokeExpressionNode> (
          | | + Patient <RootMessageNode> ())
          | + given = status <EqualityNode> (
          | | + given <InvokeExpressionNode> (
          | | | + <ReferenceNode> (&name))
          | | + status <InvokeExpressionNode> (
          | | | + Encounter <RootMessageNode> ())))"""),
        new_builder.debug_string(),
    )
    self.assertSameElements(
        ['Patient', 'Encounter'],
        [p.fhir_path for p in new_builder.get_resource_builders()],
    )
    self.assertEqual(new_builder.fhir_path, 'name.where(given = status)')
    self.assertIs(new_builder.node.context, original.node.context)
    self.assertIs(new_builder.node.return_type, original.node.return_type)

  def test_node_debug_string(self):
    """Tests debug_string print functionality."""
    # Basic FHIRView
    self.assertMultiLineEqual(
        textwrap.dedent(
            """\
        + active.exists() <ExistsFunction> (
        | + active <InvokeExpressionNode> (
        | | + Patient <RootMessageNode> ()))"""
        ),
        self.builder('Patient').active.exists().debug_string(),
    )

    # Indexing
    self.assertMultiLineEqual(
        textwrap.dedent("""\
          + address[0] <IndexerNode> (
          | + address <InvokeExpressionNode> (
          | | + Patient <RootMessageNode> ())
          | + 0 <LiteralNode> ())"""),
        self.builder('Patient').address[0].debug_string(),
    )

    # Multiple arguments
    self.assertMultiLineEqual(
        textwrap.dedent("""\
          + subject.idFor('patient') <IdForFunction> (
          | + subject <InvokeExpressionNode> (
          | | + Encounter <RootMessageNode> ())
          | + 'patient' <LiteralNode> ())"""),
        self.builder('Encounter').subject.idFor('patient').debug_string(),
    )

    # Complicated FHIRView
    self.assertMultiLineEqual(
        textwrap.dedent("""\
          + address.all(use = 'home') <AllFunction> (
          | + address <InvokeExpressionNode> (
          | | + Patient <RootMessageNode> ())
          | + use = 'home' <EqualityNode> (
          | | + use <InvokeExpressionNode> (
          | | | + <ReferenceNode> (&address))
          | | + 'home' <LiteralNode> ()))"""),
        self.builder('Patient')
        .address.all(self.builder('Patient').address.use == 'home')
        .debug_string(),
    )

    # Complicated FHIRView with type printing
    self.assertMultiLineEqual(
        textwrap.dedent("""\
          + address.all(use = 'home') <AllFunction type=<Boolean>> (
          | + address <InvokeExpressionNode type=[<StructureDataType(url=http://hl7.org/fhir/StructureDefinition/Address)>]> (
          | | + Patient <RootMessageNode type=<StructureDataType(url=http://hl7.org/fhir/StructureDefinition/Patient)>> ())
          | + use = 'home' <EqualityNode type=<Boolean>> (
          | | + use <InvokeExpressionNode type=[<String>]> (
          | | | + <ReferenceNode type=[<StructureDataType(url=http://hl7.org/fhir/StructureDefinition/Address)>]> (&address))
          | | + 'home' <LiteralNode type=<String>> ()))"""),
        self.builder('Patient')
        .address.all(self.builder('Patient').address.use == 'home')
        .debug_string(with_typing=True),
    )

    # Polymorphic choice type printing.
    self.assertMultiLineEqual(
        textwrap.dedent(
            """\
          + value <InvokeExpressionNode type=PolymorphicDataType(types=['boolean: http://hl7.org/fhirpath/System.Boolean', 'codeableconcept: http://hl7.org/fhir/StructureDefinition/CodeableConcept', 'datetime: http://hl7.org/fhirpath/System.DateTime', 'integer: http://hl7.org/fhirpath/System.Integer', 'period: http://hl7.org/fhir/StructureDefinition/Period', 'quantity: http://hl7.org/fhirpath/System.Quantity', 'range: http://hl7.org/fhir/StructureDefinition/Range', 'ratio: http://hl7.org/fhir/StructureDefinition/Ratio', 'sampleddata: http://hl7.org/fhir/StructureDefinition/SampledData', 'string: http://hl7.org/fhirpath/System.String', 'time: http://hl7.org/fhirpath/System.DateTime'])> (
          | + Observation <RootMessageNode type=<StructureDataType(url=http://hl7.org/fhir/StructureDefinition/Observation)>> ())"""
        ),
        self.builder('Observation').value.debug_string(with_typing=True),
    )

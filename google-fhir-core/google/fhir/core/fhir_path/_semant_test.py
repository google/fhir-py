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
"""Tests Python FHIRPath semantic analysis functionality."""

from typing import List, Optional, Set, cast

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.core import fhir_errors
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _navigation
from google.fhir.core.fhir_path import _semant
from google.fhir.core.fhir_path import _structure_definitions as sdefs
from google.fhir.r4 import json_format


class FhirPathSemanticAnalyzerTest(parameterized.TestCase):
  """Unit tests for `_semant.FhirPathSemanticAnalyzer`.

  The test resources are arranged as follows:

  Foo {
    Bar bar;
    Inline baz;
    string name;
    integer id;
    repeated bool boolList;
    ChoiceType choiceExample[ 'string', 'integer', 'CodeableConcept' ];
    repeated ChoiceType multipleChoiceExample[ 'string', 'CodeableConcept' ];
  }

  Bar {
    Tin tin;
    string fuzz;
  }

  Tin {
    string fig;
    Struct struct;
  }

  Struct {
    repeated string value;
    repeated int num;
  }

  CodeableConcept {
    Coding coding
  }
  """

  def setUp(self) -> None:
    super().setUp()

    # Foo resource
    self.foo_root = sdefs.build_element_definition(
        id_='Foo',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    self.foo = sdefs.build_resource_definition(
        id_='Foo',
        element_definitions=[
            self.foo_root,
            sdefs.build_element_definition(
                id_='Foo.bar',
                type_codes=['Bar'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Foo.baz',
                type_codes=['string', 'decimal'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Foo.name',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Foo.choiceExample[x]',
                type_codes=['string', 'integer', 'CodeableConcept'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Foo.multipleChoiceExample[x]',
                type_codes=['string', 'CodeableConcept'],
                cardinality=sdefs.Cardinality(min=0, max='*'),
            ),
            sdefs.build_element_definition(
                id_='Foo.id',
                type_codes=['integer'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Foo.boolList',
                type_codes=['boolean'],
                cardinality=sdefs.Cardinality(min=0, max='*'),
            ),
        ],
    )

    # Bar resource
    self.bar_root = sdefs.build_element_definition(
        id_='Bar',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    self.bar = sdefs.build_resource_definition(
        id_='Bar',
        element_definitions=[
            self.bar_root,
            sdefs.build_element_definition(
                id_='Bar.tin',
                type_codes=['Tin'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Bar.fuzz',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
        ],
    )

    # Tin resource.
    self.tin_root = sdefs.build_element_definition(
        id_='Tin',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    self.tin = sdefs.build_resource_definition(
        id_='Tin',
        element_definitions=[
            self.tin_root,
            sdefs.build_element_definition(
                id_='Tin.fig',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
            sdefs.build_element_definition(
                id_='Tin.struct',
                type_codes=['Struct'],
                cardinality=sdefs.Cardinality(min=0, max='1'),
            ),
        ],
    )

    # Struct resource.
    self.struct_root = sdefs.build_element_definition(
        id_='Struct',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=0, max='1'),
    )
    self.struct = sdefs.build_resource_definition(
        id_='Struct',
        element_definitions=[
            self.struct_root,
            sdefs.build_element_definition(
                id_='Struct.value',
                type_codes=['string'],
                cardinality=sdefs.Cardinality(min=0, max='*'),
            ),
            sdefs.build_element_definition(
                id_='Struct.num',
                type_codes=['integer'],
                cardinality=sdefs.Cardinality(min=0, max='*'),
            ),
        ],
    )

    # CodeableConcept resource
    codeable_concept_root_element_definition = sdefs.build_element_definition(
        id_='CodeableConcept',
        type_codes=None,
        cardinality=sdefs.Cardinality(min=1, max='1'),
    )
    codeable_concept_coding_system_element_definition = (
        sdefs.build_element_definition(
            id_='CodeableConcept.coding',
            type_codes=['Coding'],
            cardinality=sdefs.Cardinality(min=0, max='*'),
        )
    )
    codeable_concept = sdefs.build_resource_definition(
        id_='CodeableConcept',
        element_definitions=[
            codeable_concept_root_element_definition,
            codeable_concept_coding_system_element_definition,
        ],
    )

    structure_definitions = [
        self.foo,
        self.bar,
        self.tin,
        self.struct,
        codeable_concept,
    ]

    self.semantic_analyzer = _semant.FhirPathSemanticAnalyzer(
        _navigation._Environment(structure_definitions)
    )

    self.error_reporter = fhir_errors.ListErrorReporter()

  def assert_semantic_analysis_succeds_with_no_errors(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(ast.data_type, expected_data_type)

  def assert_semantic_analysis_fails_with_error(
      self, fhir_path_expression: str
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertLen(self.error_reporter.errors, 1)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(ast.data_type, _fhir_path_data_types.Empty)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_empty_literal',
          fhir_path_expression='{ }',
          expected_data_type=_fhir_path_data_types.Empty,
      ),
      dict(
          testcase_name='with_boolean_literal',
          fhir_path_expression='true',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_string_literal',
          fhir_path_expression="'foo'",
          expected_data_type=_fhir_path_data_types.String,
      ),
      dict(
          testcase_name='with_quantity_literal',
          fhir_path_expression="10 'mg'",
          expected_data_type=_fhir_path_data_types.Quantity,
      ),
      dict(
          testcase_name='with_integer_literal',
          fhir_path_expression='100',
          expected_data_type=_fhir_path_data_types.Integer,
      ),
      dict(
          testcase_name='with_decimal_literal',
          fhir_path_expression='3.14',
          expected_data_type=_fhir_path_data_types.Decimal,
      ),
      dict(
          testcase_name='with_date_literal',
          fhir_path_expression='@2000-01-01',
          expected_data_type=_fhir_path_data_types.Date,
      ),
      dict(
          testcase_name='with_date_time_literal',
          fhir_path_expression='@2000-01-01T12:34:56+09:00',
          expected_data_type=_fhir_path_data_types.DateTime,
      ),
  )
  def test_semantic_analysis_with_fhir_path_literal_succeeds(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    self.assert_semantic_analysis_succeds_with_no_errors(
        fhir_path_expression, expected_data_type
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_logical_and',
          fhir_path_expression='true and false',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_logical_and_boolean_empty',
          fhir_path_expression='true and { }',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_logical_and_empty',
          fhir_path_expression='{ } and { }',
          expected_data_type=_fhir_path_data_types.Empty,
      ),
      dict(
          testcase_name='with_logical_or',
          fhir_path_expression='true or 1',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_logical_implies',
          fhir_path_expression="'foo' implies false",
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_logical_xor_boolean_evaluated_operands',
          fhir_path_expression="'foo' xor false",
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_logical_xor_empty_operand',
          fhir_path_expression="'foo' xor { }",
          expected_data_type=_fhir_path_data_types.Empty,
      ),
  )
  def test_semantic_analysis_with_fhir_path_boolean_logic_succeeds(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    self.assert_semantic_analysis_succeds_with_no_errors(
        fhir_path_expression, expected_data_type
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_comparable_operands',
          fhir_path_expression='10 = 11',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_implicitly_convertible_comparable_operands',
          fhir_path_expression='10 = 11.0',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_empty_operand',
          fhir_path_expression='10 = { }',
          expected_data_type=_fhir_path_data_types.Empty,
      ),
      dict(
          testcase_name='with_both_as_empty_operands',
          fhir_path_expression='{ } = { }',
          expected_data_type=_fhir_path_data_types.Empty,
      ),
  )
  def test_semantic_analysis_with_fhir_path_equality_succeeds(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    self.assert_semantic_analysis_succeds_with_no_errors(
        fhir_path_expression, expected_data_type
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_non_comparable_operands',
          fhir_path_expression='true = 1',
      ),
      dict(
          testcase_name='with_invalid_implicit_conversion',
          fhir_path_expression="7 = 'foo'",
      ),
  )
  def test_semantic_analysis_with_invalid_fhir_path_equality_reports_error(
      self, fhir_path_expression: str
  ):
    self.assert_semantic_analysis_fails_with_error(fhir_path_expression)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_comparable_operands',
          fhir_path_expression='10 ~ 11',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_implicitly_convertible_comparable_operands',
          fhir_path_expression='10 ~ 11.0',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_empty_operand',
          fhir_path_expression='10 ~ { }',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_both_as_empty_operands',
          fhir_path_expression='{ } ~ { }',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
  )
  def test_semantic_analysis_with_fhir_path_equivalence_succeeds(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    self.assert_semantic_analysis_succeds_with_no_errors(
        fhir_path_expression, expected_data_type
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_comparable_operands',
          fhir_path_expression='10 < 11',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_implicitly_convertible_comparable_operands',
          fhir_path_expression='10 < 11.0',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_empty_operand',
          fhir_path_expression='10 < { }',
          expected_data_type=_fhir_path_data_types.Empty,
      ),
  )
  def test_semantic_analysis_with_fhir_path_comparison_succeeds(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    self.assert_semantic_analysis_succeds_with_no_errors(
        fhir_path_expression, expected_data_type
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_non_comparable_operands',
          fhir_path_expression='true < false',
      ),
      dict(
          testcase_name='with_invalid_implicit_conversion',
          fhir_path_expression="7 < 'foo'",
      ),
  )
  def test_semantic_analysis_with_invalid_fhir_path_comparison_reports_error(
      self, fhir_path_expression: str
  ):
    self.assert_semantic_analysis_fails_with_error(fhir_path_expression)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_comparable_operands',
          fhir_path_expression='10 + 11',
          expected_data_type=_fhir_path_data_types.Integer,
      ),
      dict(
          testcase_name='with_implicitly_convertible_comparable_operands',
          fhir_path_expression='10 * 11.0',
          expected_data_type=_fhir_path_data_types.Decimal,
      ),
      dict(
          testcase_name='with_empty_operand',
          fhir_path_expression='10 - { }',
          expected_data_type=_fhir_path_data_types.Empty,
      ),
      dict(
          testcase_name='with_comparable_string_operands',
          fhir_path_expression="'a' + 'b'",
          expected_data_type=_fhir_path_data_types.String,
      ),
      dict(
          testcase_name='with_comparable_operands_and_expression',
          fhir_path_expression='10 + (11 + 1)',
          expected_data_type=_fhir_path_data_types.Integer,
      ),
  )
  def test_semantic_analysis_with_fhir_path_arithmetic_succeeds(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    self.assert_semantic_analysis_succeds_with_no_errors(
        fhir_path_expression, expected_data_type
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_invalid_implicit_conversion',
          fhir_path_expression="7 + 'foo'",
      ),
      dict(
          testcase_name='with_invalid_implicit_conversion_and_expression',
          fhir_path_expression='6 * (1 = 2)',
      ),
  )
  def test_semantic_analysis_with_invalid_fhir_path_arithmetic_reports_error(
      self, fhir_path_expression: str
  ):
    self.assert_semantic_analysis_fails_with_error(fhir_path_expression)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_integer_operand',
          fhir_path_expression='+7',
          expected_data_type=_fhir_path_data_types.Integer,
      ),
      dict(
          testcase_name='with_decimal_operand',
          fhir_path_expression='-7.5',
          expected_data_type=_fhir_path_data_types.Decimal,
      ),
  )
  def test_semantic_analysis_with_valid_polarity_succeeds(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    self.assert_semantic_analysis_succeds_with_no_errors(
        fhir_path_expression, expected_data_type
    )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_string_operand',
          fhir_path_expression="-'foo'",
      ),
      dict(
          testcase_name='with_empty_operand',
          fhir_path_expression='-{ }',
      ),
  )
  def test_semantic_analysis_with_invalid_fhir_path_polarity_reports_error(
      self, fhir_path_expression: str
  ):
    self.assert_semantic_analysis_fails_with_error(fhir_path_expression)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_identifier_of_single_type',
          fhir_path_expression='bar',
          expected_data_type_url='http://hl7.org/fhir/StructureDefinition/Bar',
      ),
      dict(
          testcase_name='with_identifier_of_multiple_types',
          fhir_path_expression='baz',
          expected_data_type_url='',
      ),
  )
  def test_semantic_analysis_with_identifiers(
      self, fhir_path_expression: str, expected_data_type_url: Optional[str]
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(ast.data_type.url, expected_data_type_url)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_identifier_of_single_type',
          fhir_path_expression='name',
          expected_data_type_url=_fhir_path_data_types.String,
      ),
      dict(
          testcase_name='with_different_identifier_of_single_type',
          fhir_path_expression='id',
          expected_data_type_url=_fhir_path_data_types.Integer,
      ),
  )
  def test_semantic_analysis_with_identifiers_and_primitive_types(
      self, fhir_path_expression: str, expected_data_type_url: Optional[str]
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(ast.data_type, expected_data_type_url)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_identifier_and_identifier',
          fhir_path_expression='bar.tin',
          expected_data_type_url='http://hl7.org/fhir/StructureDefinition/Tin',
      ),
      dict(
          testcase_name='with_nested_identifier',
          fhir_path_expression='bar.tin.struct',
          expected_data_type_url=(
              'http://hl7.org/fhir/StructureDefinition/Struct'
          ),
      ),
  )
  def test_semantic_analysis_with_invocation(
      self, fhir_path_expression: str, expected_data_type_url: Optional[str]
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(ast.data_type.url, expected_data_type_url)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_identifier_and_identifier',
          fhir_path_expression='bar.fuzz',
          expect_repeated=False,
          expected_data_types=[
              _fhir_path_data_types.String,
          ],
      ),
      dict(
          testcase_name='with_nested_identifier',
          fhir_path_expression='bar.tin.fig',
          expect_repeated=False,
          expected_data_types=[
              _fhir_path_data_types.String,
          ],
      ),
      dict(
          testcase_name='with_identifier_and_expression',
          fhir_path_expression="bar.fuzz = ''",
          expect_repeated=False,
          expected_data_types=[
              _fhir_path_data_types.Boolean,
          ],
      ),
      dict(
          testcase_name='with_repeated_identifier',
          fhir_path_expression='bar.tin.struct.value',
          expect_repeated=True,
          expected_data_types={_fhir_path_data_types.String},
      ),
      dict(
          testcase_name='with_member_of_call',
          fhir_path_expression="name.memberOf('http://value.set')",
          expect_repeated=False,
          expected_data_types=[_fhir_path_data_types.Boolean],
      ),
      dict(
          testcase_name='with_of_type_call',
          fhir_path_expression="choiceExample.ofType('String')",
          expect_repeated=False,
          expected_data_types=[_fhir_path_data_types.String],
      ),
      dict(
          testcase_name='with_of_type_callon_multiple',
          fhir_path_expression="multipleChoiceExample.ofType('String')",
          expect_repeated=True,
          expected_data_types={_fhir_path_data_types.String},
      ),
      dict(
          testcase_name='with_repeated_member_of_call',
          fhir_path_expression=(
              "bar.tin.struct.value.memberOf('http://value.set')"
          ),
          expect_repeated=True,
          expected_data_types={_fhir_path_data_types.Boolean},
      ),
      dict(
          testcase_name='with_id_for_of_call',
          fhir_path_expression='name.idFor()',
          expect_repeated=False,
          expected_data_types=[_fhir_path_data_types.String],
      ),
      dict(
          testcase_name='with_repeated_id_for_call',
          fhir_path_expression='bar.tin.struct.value.idFor()',
          expect_repeated=True,
          expected_data_types={_fhir_path_data_types.String},
      ),
      dict(
          testcase_name='with_of_type_callon_message',
          fhir_path_expression="choiceExample.ofType('CodeableConcept').coding",
          expect_repeated=False,
          expected_data_types=[_fhir_path_data_types.Any_],
      ),
      dict(
          testcase_name='with_of_type_callon_multiple_message',
          fhir_path_expression=(
              "multipleChoiceExample.ofType('CodeableConcept').coding"
          ),
          expect_repeated=False,
          expected_data_types=[_fhir_path_data_types.Any_],
      ),
      dict(
          testcase_name='with_of_type_callon_multiple_message_and_where',
          fhir_path_expression=(
              "multipleChoiceExample.ofType('CodeableConcept').coding.where(system"
              " = 'test')"
          ),
          expect_repeated=False,
          expected_data_types=[_fhir_path_data_types.Any_],
      ),
  )
  def test_semantic_analysis_with_invocation_and_primitive_types(
      self,
      fhir_path_expression: str,
      expect_repeated: bool,
      expected_data_types: List[_fhir_path_data_types.FhirPathDataType],
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)

    if expect_repeated:
      data_type = ast.data_type
      self.assertIsInstance(data_type, _fhir_path_data_types.Collection)
      self.assertEqual(
          len(cast(_fhir_path_data_types.Collection, data_type).types),
          len(expected_data_types),
      )
      self.assertEqual(
          set(cast(_fhir_path_data_types.Collection, data_type).types),
          expected_data_types,
      )
    else:
      self.assertLen(expected_data_types, 1)
      self.assertEqual(ast.data_type, expected_data_types[0])

  @parameterized.named_parameters(
      dict(
          testcase_name='with_nested_member_access_leading_expression',
          fhir_path_expression='(true and false).bar.bats',
      ),
      dict(
          testcase_name='with_deep_nested_member_access_leading_expression',
          fhir_path_expression='(true and false).bar.bats',
      ),
  )
  def test_semantic_analysis_with_unsupported_invocation_raises_value_error(
      self, fhir_path_expression: str
  ):
    """Tests FHIRPath expressions that are unsupported for Standard SQL."""
    with self.assertRaises(ValueError):
      ast = _ast.build_fhir_path_ast(fhir_path_expression)
      self.semantic_analyzer.add_semantic_annotations(
          ast, self.error_reporter, self.foo, self.foo_root
      )

  @parameterized.named_parameters(
      dict(
          testcase_name='with_no_identifier',
          fhir_path_expression='count()',
          expected_data_type=_fhir_path_data_types.Empty,
      ),
      dict(
          testcase_name='with_identifier',
          fhir_path_expression='bar.count()',
          expected_data_type=_fhir_path_data_types.Integer,
      ),
      dict(
          testcase_name='with_nested_identifier',
          fhir_path_expression='bar.tin.exists()',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_deep_nested_identifier',
          fhir_path_expression='bar.tin.fig.empty()',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_first_on_repeated_field',
          fhir_path_expression='bar.tin.struct.value.first()',
          expected_data_type=_fhir_path_data_types.String,
      ),
      dict(
          testcase_name='with_first_on_non_repeated_field',
          fhir_path_expression='bar.fuzz.first()',
          expected_data_type=_fhir_path_data_types.String,
      ),
      dict(
          testcase_name='with_any_true',
          fhir_path_expression='boolList.anyTrue()',
          expected_data_type=_fhir_path_data_types.Boolean,
      ),
  )
  def test_semantic_analysis_with_function(
      self,
      fhir_path_expression: str,
      expected_data_type: _fhir_path_data_types.FhirPathDataType,
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)
    if expected_data_type is _fhir_path_data_types.Collection:
      # Needed because Collection does not use module-level instances unlike
      # the other types.
      self.assertIsInstance(ast.data_type, expected_data_type)
    else:
      self.assertEqual(ast.data_type, expected_data_type)

  def test_semantic_analysis_with_function_analyzes_all_identifiers(self):
    """Ensure identifiers on which the function is called are analyzed as well."""
    ast = _ast.build_fhir_path_ast('bar.tin.fig.exists()')
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)

    # bar.tin.fig invocation
    self.assertIsInstance(ast, _ast.Invocation)
    self.assertEqual(ast.lhs.data_type, _fhir_path_data_types.String)
    # bar.tin.fig identifier
    lhs = ast.lhs
    self.assertIsInstance(lhs, _ast.Invocation)
    self.assertEqual(lhs.rhs.data_type, _fhir_path_data_types.String)

  @parameterized.named_parameters(
      dict(
          testcase_name='with_lhs_collection_and_in',
          fhir_path_expression='bar.tin.struct.value in 4',
      ),
      dict(
          testcase_name='with_rhs_collection_and_contains',
          fhir_path_expression='3 contains bar.tin.struct.value',
      ),
  )
  def test_semantic_analysis_with_unsupported_operands_membership_raises_an_error(
      self, fhir_path_expression
  ):
    self.assert_semantic_analysis_fails_with_error(fhir_path_expression)

  @parameterized.named_parameters(
      dict(
          testcase_name='any_true_with_non_collection',
          fhir_path_expression='bar.fuzz.anyTrue()',
          expected_err_substring=(
              'anyTrue() must be called on a Collection of booleans'
          ),
      ),
      dict(
          testcase_name='any_true_with_non_boolean_collection',
          fhir_path_expression='bar.tin.struct.value.anyTrue()',
          expected_err_substring=(
              'anyTrue() must be called on a Collection of booleans'
          ),
      ),
  )
  def test_semantic_analysis_raises_an_error(
      self, fhir_path_expression: str, expected_err_substring: str
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertLen(self.error_reporter.errors, 1)
    self.assertIn(expected_err_substring, self.error_reporter.errors[0])

  @parameterized.named_parameters(
      dict(
          testcase_name='with_lhs_empty',
          fhir_path_expression='{} | bar.fuzz',
          expect_repeated=False,
          expected_data_types={
              _fhir_path_data_types.String,
          },
      ),
      dict(
          testcase_name='with_rhs_empty',
          fhir_path_expression='bar.fuzz | {}',
          expect_repeated=False,
          expected_data_types={
              _fhir_path_data_types.String,
          },
      ),
      dict(
          testcase_name='with_both_sides_empty',
          fhir_path_expression='{} | {}',
          expect_repeated=False,
          expected_data_types={
              _fhir_path_data_types.Empty,
          },
      ),
      dict(
          testcase_name='with_both_sides_single_identifiers',
          fhir_path_expression='bar.fuzz | id',
          expect_repeated=True,
          expected_data_types={
              _fhir_path_data_types.String,
              _fhir_path_data_types.Integer,
          },
      ),
      dict(
          testcase_name='with_lhs_collection',
          fhir_path_expression='bar.tin.struct.value | 1',
          expect_repeated=True,
          expected_data_types={
              _fhir_path_data_types.String,
              _fhir_path_data_types.Integer,
          },
      ),
      dict(
          testcase_name='with_rhs_collection',
          fhir_path_expression='1 | bar.tin.struct.value',
          expect_repeated=True,
          expected_data_types={
              _fhir_path_data_types.String,
              _fhir_path_data_types.Integer,
          },
      ),
      dict(
          testcase_name='with_both_sides_as_collections',
          fhir_path_expression='bar.tin.struct.num | bar.tin.struct.value',
          expect_repeated=True,
          expected_data_types={
              _fhir_path_data_types.String,
              _fhir_path_data_types.Integer,
          },
      ),
      dict(
          testcase_name='with_expression',
          fhir_path_expression="bar.tin.struct.value | (bar.fuzz = '')",
          expect_repeated=True,
          expected_data_types={
              _fhir_path_data_types.String,
              _fhir_path_data_types.Boolean,
          },
      ),
  )
  def test_semantic_analysis_with_union_operation_and_primitive_types(
      self,
      fhir_path_expression: str,
      expect_repeated: bool,
      expected_data_types: Set[_fhir_path_data_types.FhirPathDataType],
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)

    if expect_repeated:
      data_type = ast.data_type
      self.assertIsInstance(data_type, _fhir_path_data_types.Collection)
      self.assertEqual(
          len(cast(_fhir_path_data_types.Collection, data_type).types),
          len(expected_data_types),
      )
      self.assertEqual(
          set(cast(_fhir_path_data_types.Collection, data_type).types),
          expected_data_types,
      )
    else:
      self.assertLen(expected_data_types, 1)
      self.assertEqual(ast.data_type, list(expected_data_types)[0])

  @parameterized.named_parameters(
      dict(
          testcase_name='and_is',
          fhir_path_expression='bar is string',
          expected_data_type_url=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='and_as',
          fhir_path_expression='bar.tin.struct.num as integer',
          expected_data_type_url=_fhir_path_data_types.Integer,
      ),
      dict(
          testcase_name='with_nested_function_and_is',
          fhir_path_expression='bar.tin.exists() is boolean',
          expected_data_type_url=_fhir_path_data_types.Boolean,
      ),
      dict(
          testcase_name='with_deep_nested_identifier_and_as',
          fhir_path_expression='bar.tin.fig as string',
          expected_data_type_url=_fhir_path_data_types.String,
      ),
  )
  def test_semantic_analysis_with_type_specifier(
      self, fhir_path_expression: str, expected_data_type_url: Optional[str]
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(ast.data_type, expected_data_type_url)

  @parameterized.named_parameters(
      dict(
          testcase_name='and_is',
          fhir_path_expression='bar is unknown',
          expected_error=(
              'FHIR Path Error: Semantic Analysis; Expected rhs of type'
              ' expression to describe a type but got:... is unknown.'
          ),
      ),
      dict(
          testcase_name='and_as',
          fhir_path_expression='bar.tin.struct.num as unknown',
          expected_error=(
              'FHIR Path Error: Semantic Analysis; Expected rhs of type'
              ' expression to describe a type but got:... as unknown.'
          ),
      ),
  )
  def test_semantic_analysis_with_type_specifier_reports_error(
      self, fhir_path_expression: str, expected_error: str
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    self.assertLen(self.error_reporter.errors, 1)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(self.error_reporter.errors, [expected_error])
    self.assertEqual(ast.data_type, _fhir_path_data_types.Empty)

  @parameterized.named_parameters(
      dict(
          testcase_name='and_nested_identifier',
          fhir_path_expression='bar.tin.fig[1]',
          expected_data_type_url=_fhir_path_data_types.String,
      ),
      dict(
          testcase_name='and_deep_nested_identifier',
          fhir_path_expression='bar.tin.struct.num[0]',
          expected_data_type_url=_fhir_path_data_types.Integer,
      ),
      dict(
          testcase_name='and_collection_of_single_type',
          fhir_path_expression='(bar.tin.struct.num | id)[0]',
          expected_data_type_url=_fhir_path_data_types.Integer,
      ),
      dict(
          testcase_name='and_collection_with_multiple_type',
          fhir_path_expression='(bar.tin.struct.num | bar.tin.fig)[0]',
          expected_data_type_url=_fhir_path_data_types.Empty,
          expect_error=True,
      ),
      dict(
          testcase_name='with_string_as_index',
          fhir_path_expression="bar.tin.fig['abc']",
          expected_data_type_url=_fhir_path_data_types.Empty,
          expect_error=True,
      ),
  )
  def test_semantic_analysis_with_indexer_and_primitive_types(
      self,
      fhir_path_expression: str,
      expected_data_type_url: Optional[str],
      expect_error: bool = False,
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.semantic_analyzer.add_semantic_annotations(
        ast, self.error_reporter, self.foo, self.foo_root
    )
    if not expect_error:
      self.assertEmpty(self.error_reporter.errors)
    self.assertEmpty(self.error_reporter.warnings)
    self.assertEqual(ast.data_type, expected_data_type_url)

if __name__ == '__main__':
  absltest.main()

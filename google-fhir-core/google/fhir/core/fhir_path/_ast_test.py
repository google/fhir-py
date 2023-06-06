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
"""Tests Python FHIRPath Abstract Syntax Tree functionality."""

import textwrap
import unittest.mock

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types


class FhirPathAstTest(parameterized.TestCase):
  """Unit tests for `_ast.build_fhir_path_ast`."""

  @parameterized.named_parameters(
      dict(
          testcase_name='withEmptyLiteral',
          fhir_path_expression='{ }',
          expected_ast_string='None',
      ),
      dict(
          testcase_name='withBooleanLiteral',
          fhir_path_expression='true',
          expected_ast_string='True',
      ),
      dict(
          testcase_name='withStringLiteral',
          fhir_path_expression="'foo'",
          expected_ast_string="'foo'",
      ),
      dict(
          testcase_name='withQuantityLiteral',
          fhir_path_expression="10 'mg'",
          expected_ast_string="Quantity(value='10', unit='mg')",
      ),
      dict(
          testcase_name='withIntegerLiteral',
          fhir_path_expression='100',
          expected_ast_string='100',
      ),
      dict(
          testcase_name='withDecimalLiteral',
          fhir_path_expression='3.14',
          expected_ast_string="Decimal('3.14')",
      ),
  )
  def test_build_ast_with_fhir_path_literal_succeeds(
      self, fhir_path_expression: str, expected_ast_string: str
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.assertEqual(_ast.ast_to_string(ast), expected_ast_string)

  @parameterized.named_parameters(
      dict(
          testcase_name='withLogicalAnd',
          fhir_path_expression='{} and false',
          expected_ast_string='(and None False)',
      ),
      dict(
          testcase_name='withLogicalOr',
          fhir_path_expression='true or 2',
          expected_ast_string='(or True 2)',
      ),
      dict(
          testcase_name='withLogicalImplies',
          fhir_path_expression="'foo' implies false",
          expected_ast_string="(implies 'foo' False)",
      ),
      dict(
          testcase_name='withLogicalXor',
          fhir_path_expression="'foo' xor false",
          expected_ast_string="(xor 'foo' False)",
      ),
      dict(
          testcase_name='withComparableOperandsLt',
          fhir_path_expression='10 < 9',
          expected_ast_string='(< 10 9)',
      ),
      dict(
          testcase_name='withComparableOperandsGt',
          fhir_path_expression="'foo' > 'bar'",
          expected_ast_string="(> 'foo' 'bar')",
      ),
      dict(
          testcase_name='withImplicitlyConvertibleComparableOperandsLe',
          fhir_path_expression='10 <= 9.0',
          expected_ast_string="(<= 10 Decimal('9.0'))",
      ),
      dict(
          testcase_name='withImplicitlyConvertibleComparableOperandsGe',
          fhir_path_expression='9.0 >= 10',
          expected_ast_string="(>= Decimal('9.0') 10)",
      ),
      dict(
          testcase_name='withEmptyOperandEq',
          fhir_path_expression='10 = { }',
          expected_ast_string='(= 10 None)',
      ),
  )
  def test_build_ast_with_fhir_path_boolean_operator_succeeds(
      self, fhir_path_expression: str, expected_ast_string: str
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.assertEqual(_ast.ast_to_string(ast), expected_ast_string)

  @parameterized.named_parameters(
      dict(
          testcase_name='withIdentifierAccess',
          fhir_path_expression='patient.id',
          expected_ast_string='(. patient id)',
      ),
      dict(
          testcase_name='withFunctionInvocation',
          fhir_path_expression='patient.exists()',
          expected_ast_string='(. patient (function exists))',
      ),
      dict(
          testcase_name='withIdentifierFollowedByFunctionInvocation',
          fhir_path_expression='patient.id.exists()',
          expected_ast_string='(. (. patient id) (function exists))',
      ),
      dict(
          testcase_name='withFunctionWithParameterInvocation',
          fhir_path_expression="patient.where(id = '123')",
          expected_ast_string="(. patient (function where (= id '123')))",
      ),
  )
  def test_build_ast_with_fhir_path_invocation_succeeds(
      self, fhir_path_expression: str, expected_ast_string: str
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.assertEqual(_ast.ast_to_string(ast), expected_ast_string)

  @parameterized.named_parameters(
      dict(
          testcase_name='withNoIdentifiers_returnsEmpty',
          fhir_path_expression='1 + 2',
          expected_paths=[],
      ),
      dict(
          testcase_name='withLiteral_succeeds',
          fhir_path_expression='a',
          expected_paths=['a'],
      ),
      dict(
          testcase_name='withFunctionCall_succeeds',
          fhir_path_expression='a.exists()',
          expected_paths=['a'],
      ),
      dict(
          testcase_name='withDottedPath_succeeds',
          fhir_path_expression='a.b.exists()',
          expected_paths=['a.b'],
      ),
      dict(
          testcase_name='withLongerDottedPath_succeeds',
          fhir_path_expression='a.b.c.d.e.exists()',
          expected_paths=['a.b.c.d.e'],
      ),
      dict(
          testcase_name='withLogicalOperator_succeeds',
          fhir_path_expression='a.exists() or a.b.exists() or c',
          expected_paths=['a', 'a.b', 'c'],
      ),
      dict(
          testcase_name='withLogicalOperator_succeeds_again',
          fhir_path_expression='a or b or c',
          expected_paths=['a', 'b', 'c'],
      ),
      dict(
          testcase_name='withIdentifiersInFunctionCall_succeeds',
          fhir_path_expression='a.where(b)',
          expected_paths=['a', 'a.b'],
      ),
      dict(
          testcase_name='withIdentifiersAsChildrenOfFunctionCall_succeeds',
          fhir_path_expression='a.where(b > c.d)',
          expected_paths=['a', 'a.b', 'a.c.d'],
      ),
      dict(
          testcase_name='withChainedFunctionCalls_succeeds',
          fhir_path_expression='where(b > c.d).exists()',
          expected_paths=['b', 'c.d'],
      ),
      dict(
          testcase_name='withChainedInvocationsAndFunctionCalls_succeeds',
          fhir_path_expression='a.where(b > c.d).exists()',
          expected_paths=['a', 'a.b', 'a.c.d'],
      ),
      dict(
          testcase_name='withNestedFunctionCalls_succeeds',
          fhir_path_expression='a.where(b > c.where(d > e))',
          expected_paths=['a', 'a.b', 'a.c', 'a.c.d', 'a.c.e'],
      ),
      dict(
          testcase_name='withDuplicateIdentifiers_findsUniqueIdentifiers',
          fhir_path_expression='a.where(b > b)',
          expected_paths=['a', 'a.b'],
      ),
      dict(
          testcase_name='withThis_resolvesThis',
          fhir_path_expression='a.all($this = b)',
          expected_paths=['a', 'a.b'],
      ),
      dict(
          testcase_name='withPathOnEndOfFunctionCall_includesOperand',
          fhir_path_expression="telecom.where(use = 'home').value.empty()",
          expected_paths=['telecom', 'telecom.use', 'telecom.value'],
      ),
  )
  def test_paths_referenced_by_(self, fhir_path_expression, expected_paths):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    paths = _ast.paths_referenced_by(ast)
    self.assertCountEqual(paths, expected_paths)

  @parameterized.named_parameters(
      dict(
          testcase_name='withIdentifierAccess',
          fhir_path_expression='patient.id',
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Identifier<patient>
          | Identifier<id>"""),
      ),
      dict(
          testcase_name='withFunctionInvocation',
          fhir_path_expression='patient.exists()',
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Identifier<patient>
          | Function<function>
          | | Identifier<exists>"""),
      ),
      dict(
          testcase_name='withIdentifierFollowedByFunctionInvocation',
          fhir_path_expression='patient.id.exists()',
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Invocation<.>
          | | Identifier<patient>
          | | Identifier<id>
          | Function<function>
          | | Identifier<exists>"""),
      ),
      dict(
          testcase_name='withFunctionWithParameterInvocation',
          fhir_path_expression="patient.where(id = '123')",
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Identifier<patient>
          | Function<function>
          | | Identifier<where>
          | | EqualityRelation<=>
          | | | Identifier<id>
          | | | Literal<'123'>"""),
      ),
  )
  def test_debug_string_produces_expected_string(
      self, fhir_path_expression: str, expected_debug_string: str
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.assertEqual(ast.debug_string(), expected_debug_string)

  @parameterized.named_parameters(
      dict(
          testcase_name='withBareReference_returnsTrue',
          fhir_path_expression='reference',
          expected_result=True,
      ),
      dict(
          testcase_name='withNestedBareReference_returnsTrue',
          fhir_path_expression='foo.reference',
          expected_result=True,
      ),
      dict(
          testcase_name='withReferenceAndNonIdForFunctionCall_returnsTrue',
          fhir_path_expression='reference.exists()',
          expected_result=True,
      ),
      dict(
          testcase_name=(
              '_withNestedReferenceAndNonIdForFunctionCall_returnsTrue'
          ),
          fhir_path_expression='foo.reference.exists()',
          expected_result=True,
      ),
      dict(
          testcase_name='withReferenceAndIdForCall_returnsFalse',
          fhir_path_expression="reference.idFor('Patient')",
          expected_result=False,
      ),
      dict(
          testcase_name='withNestedReferenceAndIdForCall_returnsFalse',
          fhir_path_expression="foo.reference.idFor('Patient')",
          expected_result=False,
      ),
      dict(
          testcase_name=(
              '_withReferenceAndIdForCallAndTrailingCall_returnsFalse'
          ),
          fhir_path_expression="reference.idFor('Patient').exists()",
          expected_result=False,
      ),
      dict(
          testcase_name=(
              '_withNestedReferenceAndIdForCallAndTrailingCall_returnsFalse'
          ),
          fhir_path_expression="foo.reference.idFor('Patient').exists()",
          expected_result=False,
      ),
  )
  def test_contains_reference_without_id_for(
      self, fhir_path_expression: str, expected_result: bool
  ):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    _decorate_ast_reference_data_types(ast)
    self.assertEqual(
        _ast.contains_reference_without_id_for(ast), expected_result
    )


def _decorate_ast_reference_data_types(node: _ast.AbstractSyntaxTree) -> None:
  """Adds data types for reference nodes.

  Sets the data_type for any identifier node named 'reference' to that of a
  Reference type.

  Args:
    node: The root node of the AST to modify.
  """
  if isinstance(node, _ast.Identifier) and node.value == 'reference':
    node.data_type = unittest.mock.Mock(
        spec=_fhir_path_data_types.StructureDataType, element_type='Reference'
    )

  for child in node.children or ():
    _decorate_ast_reference_data_types(child)


if __name__ == '__main__':
  absltest.main()

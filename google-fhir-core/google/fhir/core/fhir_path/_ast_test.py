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

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.core.fhir_path import _ast


class FhirPathAstTest(parameterized.TestCase):
  """Unit tests for `_ast.build_fhir_path_ast`."""

  @parameterized.named_parameters(
      dict(
          testcase_name='_withEmptyLiteral',
          fhir_path_expression='{ }',
          expected_ast_string='None'),
      dict(
          testcase_name='_withBooleanLiteral',
          fhir_path_expression='true',
          expected_ast_string='True'),
      dict(
          testcase_name='_withStringLiteral',
          fhir_path_expression="'foo'",
          expected_ast_string="'foo'"),
      dict(
          testcase_name='_withQuantityLiteral',
          fhir_path_expression="10 'mg'",
          expected_ast_string="Quantity(value='10', unit='mg')"),
      dict(
          testcase_name='_withIntegerLiteral',
          fhir_path_expression='100',
          expected_ast_string='100'),
      dict(
          testcase_name='_withDecimalLiteral',
          fhir_path_expression='3.14',
          expected_ast_string="Decimal('3.14')"),
  )
  def testBuildAst_withFhirPathLiteral_succeeds(self, fhir_path_expression: str,
                                                expected_ast_string: str):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.assertEqual(_ast.ast_to_string(ast), expected_ast_string)

  @parameterized.named_parameters(
      dict(
          testcase_name='_withLogicalAnd',
          fhir_path_expression='{} and false',
          expected_ast_string='(and None False)'),
      dict(
          testcase_name='_withLogicalOr',
          fhir_path_expression='true or 2',
          expected_ast_string='(or True 2)'),
      dict(
          testcase_name='_withLogicalImplies',
          fhir_path_expression="'foo' implies false",
          expected_ast_string="(implies 'foo' False)"),
      dict(
          testcase_name='_withLogicalXor',
          fhir_path_expression="'foo' xor false",
          expected_ast_string="(xor 'foo' False)"),
      dict(
          testcase_name='_withComparableOperandsLt',
          fhir_path_expression='10 < 9',
          expected_ast_string='(< 10 9)'),
      dict(
          testcase_name='_withComparableOperandsGt',
          fhir_path_expression="'foo' > 'bar'",
          expected_ast_string="(> 'foo' 'bar')"),
      dict(
          testcase_name='_withImplicitlyConvertibleComparableOperandsLe',
          fhir_path_expression='10 <= 9.0',
          expected_ast_string="(<= 10 Decimal('9.0'))"),
      dict(
          testcase_name='_withImplicitlyConvertibleComparableOperandsGe',
          fhir_path_expression='9.0 >= 10',
          expected_ast_string="(>= Decimal('9.0') 10)"),
      dict(
          testcase_name='_withEmptyOperandEq',
          fhir_path_expression='10 = { }',
          expected_ast_string='(= 10 None)'),
  )
  def testBuildAst_withFhirPathBooleanOperator_succeeds(
      self, fhir_path_expression: str, expected_ast_string: str):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.assertEqual(_ast.ast_to_string(ast), expected_ast_string)

  @parameterized.named_parameters(
      dict(
          testcase_name='_withIdentifierAccess',
          fhir_path_expression='patient.id',
          expected_ast_string='(. patient id)'),
      dict(
          testcase_name='_withFunctionInvocation',
          fhir_path_expression='patient.exists()',
          expected_ast_string='(. patient (function exists))'),
      dict(
          testcase_name='_withIdentifierFollowedByFunctionInvocation',
          fhir_path_expression='patient.id.exists()',
          expected_ast_string='(. (. patient id) (function exists))'),
      dict(
          testcase_name='_withFunctionWithParameterInvocation',
          fhir_path_expression="patient.where(id = '123')",
          expected_ast_string="(. patient (function where (= id '123')))"),
  )
  def testBuildAst_withFhirPathInvocation_succeeds(self,
                                                   fhir_path_expression: str,
                                                   expected_ast_string: str):
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
  def testPathsReferencedBy_(self, fhir_path_expression, expected_paths):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    paths = _ast.paths_referenced_by(ast)
    self.assertCountEqual(paths, expected_paths)

  @parameterized.named_parameters(
      dict(
          testcase_name='_withIdentifierAccess',
          fhir_path_expression='patient.id',
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Identifier<patient>
          | Identifier<id>""")),
      dict(
          testcase_name='_withFunctionInvocation',
          fhir_path_expression='patient.exists()',
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Identifier<patient>
          | Function<function>
          | | Identifier<exists>""")),
      dict(
          testcase_name='_withIdentifierFollowedByFunctionInvocation',
          fhir_path_expression='patient.id.exists()',
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Invocation<.>
          | | Identifier<patient>
          | | Identifier<id>
          | Function<function>
          | | Identifier<exists>""")),
      dict(
          testcase_name='_withFunctionWithParameterInvocation',
          fhir_path_expression="patient.where(id = '123')",
          expected_debug_string=textwrap.dedent("""\
          Invocation<.>
          | Identifier<patient>
          | Function<function>
          | | Identifier<where>
          | | EqualityRelation<=>
          | | | Identifier<id>
          | | | Literal<'123'>""")),
  )
  def testDebugString_producesExpectedString(self, fhir_path_expression: str,
                                             expected_debug_string: str):
    ast = _ast.build_fhir_path_ast(fhir_path_expression)
    self.assertEqual(ast.debug_string(), expected_debug_string)


if __name__ == '__main__':
  absltest.main()

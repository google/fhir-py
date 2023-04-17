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
"""Transforms expression nodes into elm nodes."""

from typing import Any, Optional, Mapping

from google.fhir.core.execution import nodes
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types

_ARITHMETIC_NODES: Mapping[str, type(nodes.BinaryExpressionNode)] = {
    _ast.Arithmetic.Op.ADDITION: nodes.AddNode,
    _ast.Arithmetic.Op.SUBTRACTION: nodes.SubtractNode,
    _ast.Arithmetic.Op.MULTIPLICATION: nodes.MultiplyNode,
    _ast.Arithmetic.Op.DIVISION: nodes.DivideNode,
    _ast.Arithmetic.Op.TRUNCATED_DIVISION: nodes.TruncatedDivideNode,
    _ast.Arithmetic.Op.MODULO: nodes.ModuloNode,
    _ast.Arithmetic.Op.STRING_CONCATENATION: nodes.AddNode,
}

_BOOLEAN_NODES: Mapping[str, type(nodes.BinaryExpressionNode)] = {
    _ast.BooleanLogic.Op.IMPLIES: nodes.ImpliesNode,
    _ast.BooleanLogic.Op.XOR: nodes.XorNode,
    _ast.BooleanLogic.Op.AND: nodes.AndNode,
    _ast.BooleanLogic.Op.OR: nodes.OrNode,
}


def _get_elm_type_name(url: str):
  """Given a FHIR type url, return the equivalent elm type name."""
  if not url or '/' not in url:
    raise ValueError(f"Input url {url} is not valid. Missing '/'.")
  index = url.rfind('/')
  elm_name = url[index + 1 :]

  # System types may also start with a 'System.' prefix
  # * https://www.hl7.org/fhir/fhirpath.html#types
  if elm_name.startswith('System.'):
    elm_name = elm_name[7:]
  return f'fhir:{elm_name}'


def _convert_to_node_type(
    fhir_type: Optional[_fhir_path_data_types.FhirPathDataType],
) -> Optional[nodes.TypeSpecifierNode]:
  """Returns equivalent TypeSpecifier for the given FhirPathDataType."""
  if not fhir_type or fhir_type == _fhir_path_data_types.Empty:
    return None

  node_type: nodes.TypeSpecifierNode = None
  if isinstance(fhir_type, _fhir_path_data_types.PolymorphicDataType):
    raise NotImplementedError

  type_name = _get_elm_type_name(fhir_type.url)
  node_type = nodes.NamedTypeSpecifierNode(name=type_name)
  if _fhir_path_data_types.is_collection(fhir_type):
    return nodes.ListTypeSpecifierNode(element_type=node_type)

  return node_type


class NodeTransformer(_evaluation.ExpressionNodeBaseVisitor):
  """Traverses the ExpressionNodes and builds the equivalent tree in Nodes."""

  def transform(self, node: _evaluation.ExpressionNode) -> nodes.ExpressionNode:
    return self.visit(node)

  def visit_root(
      self, root: _evaluation.RootMessageNode
  ) -> nodes.ExpressionNode:
    return nodes.RetrieveNode(
        data_type=root.return_type().url,
        result_type_specifier=_convert_to_node_type(root.return_type()),
    )

  def visit_reference(self, reference: _evaluation.ExpressionNode) -> Any:
    raise NotImplementedError

  def visit_literal(
      self, literal: _evaluation.LiteralNode
  ) -> nodes.LiteralNode:
    return nodes.LiteralNode(
        value_type=str(literal.get_value()),
        value=literal.to_fhir_path(),
        result_type_specifier=_convert_to_node_type(literal.return_type()),
    )

  def visit_invoke_expression(
      self, identifier: _evaluation.InvokeExpressionNode
  ) -> Any:
    return nodes.PropertyNode(
        path=identifier.identifier,
        source=self.visit(identifier.get_parent_node()),
        result_type_specifier=_convert_to_node_type(identifier.return_type()),
    )

  def visit_indexer(
      self, indexer: _evaluation.IndexerNode
  ) -> nodes.IndexerNode:
    operand = (self.visit(indexer.collection), self.visit(indexer.index))
    return nodes.IndexerNode(
        operand=operand,
        result_type_specifier=_convert_to_node_type(indexer.return_type()),
    )

  def visit_arithmetic(
      self, arithmetic: _evaluation.ArithmeticNode
  ) -> nodes.BinaryExpressionNode:
    operand = (self.visit(arithmetic.right), self.visit(arithmetic.left))
    result_type = _convert_to_node_type(arithmetic.return_type())
    node_class = _ARITHMETIC_NODES.get(arithmetic.op)
    if node_class is None:
      raise ValueError(f'Unrecognized arithmetic operator: {arithmetic.op}')

    return node_class(operand=operand, result_type_specifier=result_type)

  def visit_equality(self, equality: _evaluation.EqualityNode) -> Any:
    raise NotImplementedError

  def visit_comparison(self, comparison: _evaluation.ComparisonNode) -> Any:
    raise NotImplementedError

  def visit_boolean_op(
      self, boolean_logic: _evaluation.BooleanOperatorNode
  ) -> nodes.BinaryExpressionNode:
    operand = (self.visit(boolean_logic.right), self.visit(boolean_logic.left))
    result_type = _convert_to_node_type(boolean_logic.return_type())

    node_class = _BOOLEAN_NODES.get(boolean_logic.op)
    if node_class is None:
      raise ValueError(f'Unrecognized boolean operator: {boolean_logic.op}')

    return node_class(operand=operand, result_type_specifier=result_type)

  def visit_membership(
      self, relation: _evaluation.MembershipRelationNode
  ) -> Any:
    raise NotImplementedError

  def visit_union(self, union: _evaluation.UnionNode) -> Any:
    raise NotImplementedError

  def visit_polarity(self, polarity: _evaluation.NumericPolarityNode) -> Any:
    raise NotImplementedError

  def visit_function(self, function: _evaluation.FunctionNode) -> Any:
    raise NotImplementedError

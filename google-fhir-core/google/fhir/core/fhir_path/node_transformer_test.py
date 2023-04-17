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

from typing import List, Dict

from absl.testing import absltest
from absl.testing import parameterized
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.core.execution import nodes
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _structure_definitions as sdefs
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.core.fhir_path import node_transformer
from google.fhir.r4 import primitive_handler


class TestTransformer(node_transformer.NodeTransformer):

  def __init__(self):
    super().__init__()
    self.visitation_stack = []

  def visit(self, node: _evaluation.ExpressionNode):
    result_node = super().visit(node)
    node_name = result_node.__class__.__name__
    self.visitation_stack.append(node_name)
    return result_node


class NodeTransformerTest(parameterized.TestCase):

  """Tests transforming _expression nodes into nodes.py.

  Class Attributes:
    resources: A mapping from `StructureDefinition.url.value` to the associated
      `StructureDefinition`.
    mock_context: Stores all of the structure definitions to use.
  """

  resources: Dict[str, structure_definition_pb2.StructureDefinition]
  mock_context: context.FhirPathContext

  @classmethod
  def setUpClass(cls):
    super().setUpClass()

    # HumanName resource
    human_name_root = sdefs.build_element_definition(
        id_='HumanName', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    human_name_first = sdefs.build_element_definition(
        id_='HumanName.first',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    human_name_last = sdefs.build_element_definition(
        id_='HumanName.last',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    cls.human_name = sdefs.build_resource_definition(
        id_='HumanName',
        element_definitions=[
            human_name_root,
            human_name_first,
            human_name_last,
        ],
    )

    # Patient resource
    patient_root = sdefs.build_element_definition(
        id_='Patient', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    patient_name = sdefs.build_element_definition(
        id_='Patient.name',
        type_codes=['HumanName'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    patient_addresses = sdefs.build_element_definition(
        id_='Patient.addresses',
        type_codes=['Address'],
        cardinality=sdefs.Cardinality(0, '*'),
    )
    cls.patient = sdefs.build_resource_definition(
        id_='Patient',
        element_definitions=[
            patient_root,
            patient_name,
            patient_addresses,
        ],
    )

    # Address resource
    address_root = sdefs.build_element_definition(
        id_='Address', type_codes=None, cardinality=sdefs.Cardinality(0, '1')
    )
    address_city = sdefs.build_element_definition(
        id_='Address.city',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    address_state = sdefs.build_element_definition(
        id_='Address.state',
        type_codes=['string'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    address_zip = sdefs.build_element_definition(
        id_='Address.zip',
        type_codes=['integer'],
        cardinality=sdefs.Cardinality(0, '1'),
    )
    cls.address = sdefs.build_resource_definition(
        id_='Address',
        element_definitions=[
            address_root,
            address_city,
            address_state,
            address_zip,
        ],
    )

    all_resources = [
        cls.address,
        cls.human_name,
        cls.patient,
    ]
    cls.resources = {resource.url.value: resource for resource in all_resources}
    cls.mock_context = context.MockFhirPathContext(all_resources)

  @parameterized.named_parameters(
      dict(
          testcase_name='_LiteralOpLiteral',
          fhir_path_expression='1 or 2',
          expected_visitation_stack=['LiteralNode', 'LiteralNode', 'OrNode'],
      ),
      dict(
          testcase_name='_FieldOpLiteral',
          fhir_path_expression='addresses.zip xor true',
          expected_visitation_stack=[
              'LiteralNode',
              'RetrieveNode',
              'PropertyNode',
              'PropertyNode',
              'XorNode',
          ],
      ),
      dict(
          testcase_name='_LiteralOpField',
          fhir_path_expression='1 implies addresses.zip',
          expected_visitation_stack=[
              'RetrieveNode',
              'PropertyNode',
              'PropertyNode',
              'LiteralNode',
              'ImpliesNode',
          ],
      ),
  )
  def testSimpleBinaryOp(
      self, fhir_path_expression: str, expected_visitation_stack=List[str]
  ):
    builder = expressions.from_fhir_path_expression(
        fhir_path_expression,
        self.mock_context,
        _fhir_path_data_types.StructureDataType(self.patient),
        primitive_handler.PrimitiveHandler(),
    )

    transformer = TestTransformer()
    transformer.transform(builder.get_node())
    self.assertEqual(transformer.visitation_stack, expected_visitation_stack)

  def testArithmeticOp(self):
    ops = {
        '+': 'AddNode',
        '-': 'SubtractNode',
        '*': 'MultiplyNode',
        '/': 'DivideNode',
        'div': 'TruncatedDivideNode',
        'mod': 'ModuloNode',
        '&': 'AddNode',
    }
    for op, node_name in ops.items():
      fhir_path_expression = f'1 {op} 2'
      builder = expressions.from_fhir_path_expression(
          fhir_path_expression,
          self.mock_context,
          _fhir_path_data_types.StructureDataType(self.patient),
          primitive_handler.PrimitiveHandler(),
      )

      transformer = TestTransformer()
      transformer.transform(builder.get_node())
      self.assertEqual(transformer.visitation_stack[-1], node_name)

  def testBooleanOp(self):
    ops = {
        'implies': 'ImpliesNode',
        'or': 'OrNode',
        'and': 'AndNode',
        'xor': 'XorNode',
    }
    for op, node_name in ops.items():
      fhir_path_expression = f'true {op} false'
      builder = expressions.from_fhir_path_expression(
          fhir_path_expression,
          self.mock_context,
          _fhir_path_data_types.StructureDataType(self.patient),
          primitive_handler.PrimitiveHandler(),
      )

      transformer = TestTransformer()
      transformer.transform(builder.get_node())
      self.assertEqual(transformer.visitation_stack[-1], node_name)

  @parameterized.named_parameters(
      dict(
          testcase_name='_accessField',
          fhir_path_expression='name',
          expected_visitation_stack=[
              'RetrieveNode',
              'PropertyNode',
          ],
      ),
      dict(
          testcase_name='_accessDeepField',
          fhir_path_expression='addresses.city',
          expected_visitation_stack=[
              'RetrieveNode',
              'PropertyNode',
              'PropertyNode',
          ],
      ),
      dict(
          testcase_name='_indexing',
          fhir_path_expression='addresses[0]',
          expected_visitation_stack=[
              'RetrieveNode',
              'PropertyNode',
              'LiteralNode',
              'IndexerNode',
          ],
      ),
  )
  def testExpressions(
      self, fhir_path_expression: str, expected_visitation_stack=List[str]
  ):
    builder = expressions.from_fhir_path_expression(
        fhir_path_expression,
        self.mock_context,
        _fhir_path_data_types.StructureDataType(self.patient),
        primitive_handler.PrimitiveHandler(),
    )

    transformer = TestTransformer()
    transformer.transform(builder.get_node())
    self.assertEqual(transformer.visitation_stack, expected_visitation_stack)

  @parameterized.named_parameters(
      dict(
          testcase_name='_Empty', fhir_path_expression='{ }', expected_type=None
      ),
      dict(
          testcase_name='_LiteralInt',
          fhir_path_expression='5',
          expected_type=nodes.NamedTypeSpecifierNode(name='fhir:Integer'),
      ),
      dict(
          testcase_name='_LiteralString',
          fhir_path_expression="'5'",
          expected_type=nodes.NamedTypeSpecifierNode(name='fhir:String'),
      ),
      dict(
          testcase_name='_PropertyType',
          fhir_path_expression='name',
          expected_type=nodes.NamedTypeSpecifierNode(name='fhir:HumanName'),
      ),
      dict(
          testcase_name='_ArrayType',
          fhir_path_expression='addresses',
          expected_type=nodes.ListTypeSpecifierNode(
              element_type=nodes.NamedTypeSpecifierNode(name='fhir:Address')
          ),
      ),
      dict(
          testcase_name='_AccessArray',
          fhir_path_expression='addresses[0]',
          expected_type=nodes.NamedTypeSpecifierNode(name='fhir:Address'),
      ),
      dict(
          testcase_name='_ArithExpression',
          fhir_path_expression='1 + 4',
          expected_type=nodes.NamedTypeSpecifierNode(name='fhir:Integer'),
      ),
      dict(
          testcase_name='_BoolExpression',
          fhir_path_expression='1 implies 4',
          expected_type=nodes.NamedTypeSpecifierNode(name='fhir:Boolean'),
      ),
  )
  def testTypeTransformation(
      self, fhir_path_expression: str, expected_type: nodes.TypeSpecifierNode
  ):
    builder = expressions.from_fhir_path_expression(
        fhir_path_expression,
        self.mock_context,
        _fhir_path_data_types.StructureDataType(self.patient),
        primitive_handler.PrimitiveHandler(),
    )
    transformer = node_transformer.NodeTransformer()
    node = transformer.transform(builder.get_node())

    self.assertEqual(expected_type, node.result_type_specifier)


if __name__ == '__main__':
  absltest.main()

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

import functools
from typing import List
from google.fhir.core.execution import nodes
from google.fhir.core.execution import visitor
from absl.testing import absltest


class _GenericVisitTrackingVisitor(visitor.NodeVisitor):
  """A test `NodeVisitor` that simply tracks the stack of visited nodes handled by generic visits."""

  def __init__(self):
    self.full_visitation_stack: List[str] = []

  def generic_visit(self, node: nodes.Node):
    super().generic_visit(node)
    self.full_visitation_stack.append(node.__class__.__name__)


class NodeVisitorTest(absltest.TestCase):

  def test_registered_visits_are_invoked(self):
    library = nodes.LibraryNode(identifier=nodes.VersionedIdentifierNode())

    class TrackingNodeVisitor(_GenericVisitTrackingVisitor):
      """A test `NodeVisitor` that simply tracks the stack of visited nodes handled by generic and explicit visits."""

      def __init__(self):
        self.implemented_visitation_stack: List[str] = []
        super().__init__()

      @functools.singledispatchmethod
      def visit(self, node: nodes.Node):
        return super().visit(node)

      @visit.register
      def _(self, node: nodes.LibraryNode):
        self.generic_visit(node)
        self.implemented_visitation_stack.append(node.__class__.__name__)

      @visit.register
      def _(self, node: nodes.VersionedIdentifierNode):
        self.generic_visit(node)
        self.implemented_visitation_stack.append(node.__class__.__name__)

    test_visitor = TrackingNodeVisitor()
    test_visitor.visit(library)

    self.assertEqual(
        test_visitor.full_visitation_stack,
        [
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'VersionedIdentifierNode',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'NoneType',
            'LibraryNode',
        ],
    )
    self.assertEqual(
        test_visitor.implemented_visitation_stack,
        ['VersionedIdentifierNode', 'LibraryNode'],
    )

  def test_list_fields_are_visited(self):
    library = nodes.TupleNode(element=[nodes.NullNode(), nodes.NullNode()])

    test_visitor = _GenericVisitTrackingVisitor()
    test_visitor.visit(library)

    self.assertEqual(
        test_visitor.full_visitation_stack,
        [
            'NoneType',
            'NoneType',
            'NoneType',
            'NullNode',
            'NoneType',
            'NoneType',
            'NoneType',
            'NullNode',
            'NoneType',
            'NoneType',
            'TupleNode',
        ],
    )


if __name__ == '__main__':
  absltest.main()

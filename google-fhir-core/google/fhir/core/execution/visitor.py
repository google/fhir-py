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

"""Supports visitation of the execution node tree.

This approach is influenced by Python's `ast` module -
https://github.com/python/cpython/blob/3.10/Lib/ast.py#L394-L449.
"""

from typing import Any, Iterator
from google.fhir.core.execution import nodes


def _iter_fields(node: nodes.Node) -> Iterator[Any]:
  """Yield each field that is present on the `node`."""
  yield from (
      attr_value
      for attr_name in dir(node)
      if not attr_name.startswith("__")
      and not callable(attr_value := getattr(node, attr_name))
  )


class NodeVisitor(object):
  """A node visitor base class that traverses the FHIR core execution node tree.

  A visitor function is called for every node. This function may return a value
  which is forwarded by the `visit` method.

  This class is meant to be subclassed, with the subclass adding visitor
  methods.

  Visit functions for the nodes can register against the visit method using
  `functools.singledispatchmethod`. Below is a primitive example of a
  visitor subclass which simply visits the `LibraryNode`.

  ```
  class MyNodeVisitor(NodeVisitor):

    @functools.singledispatchmethod
    def visit(self, node):
      return super().visit(node)

    @visit.register
    def _(self, node: LibraryNode):
      print('Visiting a library node.')
  ```
  """

  def generic_visit(self, node: nodes.Node):
    """Generically visits all fields of the `node`.

    Called if no explicit visitor function exists for a node.

    Args:
      node: The node being generically visited.
    """
    for field in _iter_fields(node):
      if isinstance(field, (list, tuple, set)):
        for item in field:
          self.visit(item)
      else:
        self.visit(field)

  def visit(self, node: nodes.Node) -> Any:
    """Visit a node."""
    return self.generic_visit(node)

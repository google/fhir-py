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
"""Utilities and classes for traversing a FHIR resource graph.

A FHIR resource graph is a directed graph of resources. It contains
structure definitions in along with their fields. It also shows us what
element definitions and types those fields are.
"""

from typing import Any, Dict, Iterable, List, Optional, cast

from google.protobuf import message
from google.fhir.core.fhir_path import _utils

# TODO(b/201107372): Update FHIR-agnostic types to a protocol.
StructureDefinition = message.Message
ElementDefinition = message.Message
Constraint = message.Message


# TODO(b/205890502): Refactor _ElementDefinitionTree to make it work more
# efficiently.
class _ElementDefinitionTree:
  """Creates a tree of all the `ElementDefinition`s in the structure definition.

  Allowing us to map from an element definition in this structure definition to
  its children.
  """

  @property
  def structure_definition(self) -> StructureDefinition:
    """Returns a reference to the underlying `StructureDefintion`."""
    return self._structure_definition

  @property
  def root_element(self) -> ElementDefinition:
    """Returns a reference to the root `ElementDefinition`."""
    return self._root_element

  @property
  def all_elements(self) -> List[ElementDefinition]:
    """Returns all elements in the `StructureDefinition`'s snapshot."""
    return cast(Any, self.structure_definition).snapshot.element[:]

  def __init__(self, structure_definition: StructureDefinition) -> None:
    """Creates a new `_ElementDefinitionTree`.

    Args:
      structure_definition: The `StructureDefinition` used to create an
        `ElementDefinition` tree from.

    Raises:
      ValueError in the event that more than one root `ElementDefinition`
      exists.
    """
    self._structure_definition = structure_definition

    # A mapping from `ElementDefinition` id to its "children".
    self._child_memos: Dict[str, List[ElementDefinition]] = {}
    self._root_element = None

    for element_definition in cast(Any, structure_definition).snapshot.element:
      if _utils.is_root_element(element_definition):
        if self._root_element is not None:
          raise ValueError(
              'Expected a single root ElementDefinition but got: '
              f'{cast(Any, self._root_element).id.value!r} and '
              f'{cast(Any, element_definition).id.value!r}.'
          )
        self._root_element = element_definition

  def _get_descendants(
      self, element_definition: ElementDefinition
  ) -> List[ElementDefinition]:
    """Returns a list of descendants of an `ElementDefinition`."""
    parent_id_prefix = cast(Any, element_definition).id.value + '.'
    return [
        element
        for element in self.all_elements
        if cast(Any, element).id.value.startswith(parent_id_prefix)
    ]

  def get_children(
      self, element_definition: ElementDefinition
  ) -> List[ElementDefinition]:
    """Returns a list of direct children of an `ElementDefinition`."""
    id_value: str = cast(Any, element_definition).id.value
    if id_value not in self._child_memos:
      descendants = self._get_descendants(element_definition)
      id_parts_len = len(id_value.split('.'))
      self._child_memos[id_value] = [
          descendant
          for descendant in descendants
          if len(cast(Any, descendant).id.value.split('.')) == id_parts_len + 1
      ]
    return self._child_memos[id_value]


class _Environment:
  """Provides methods for navigating a FHIR resource graph."""

  def __init__(
      self, structure_definitions: Iterable[StructureDefinition]
  ) -> None:
    """Creates a new `Environment` from `structure_definitions`.

    Args:
      structure_definitions: The list of `StructureDefinition`s that represent a
        closed transitive set of resources that will be traversed during
        FHIRPath encoding.

    Raises:
      ValueError in the event that a `StructureDefinition` with a duplicate
      URL is provided or a `StructureDefinition` without a root
      `ElementDefinition` is provided.
    """
    self._structure_definition_map: Dict[str, _ElementDefinitionTree] = {}

    # Populate mappings
    for sd in structure_definitions:
      url_value: str = cast(Any, sd).url.value
      if url_value in self._structure_definition_map:
        raise ValueError(
            'Unexpected duplicate `StructureDefinition` URL '
            f'value: {url_value!r}.'
        )
      self._structure_definition_map[url_value] = _ElementDefinitionTree(sd)

  def get_structure_definition_for(
      self, url_value: str
  ) -> Optional[StructureDefinition]:
    """Returns the `StructureDefinition` given a uniquely-identifying URL."""
    map_ = self._structure_definition_map.get(url_value)
    return map_.structure_definition if map_ is not None else None

  def get_root_element_for(
      self, structure_definition: StructureDefinition
  ) -> Optional[ElementDefinition]:
    """Returns the root ElementDefinition for a type."""
    url_value = cast(Any, structure_definition).url.value
    map_ = self._structure_definition_map.get(url_value)
    return map_.root_element if map_ is not None else None

  def get_children(
      self,
      structure_definition: StructureDefinition,
      element_definition: ElementDefinition,
  ) -> List[ElementDefinition]:
    """Returns the direct children of `element_definition`."""
    url_value = cast(Any, structure_definition).url.value
    map_ = self._structure_definition_map.get(url_value)
    return map_.get_children(element_definition) if map_ is not None else []


class FhirStructureDefinitionWalker:
  """A walker that traverses a FHIR resource graph.

  Given an environment to operate in, along with and some initial type/element
  state, the `FhirStructureDefinitionWalker` moves to a new state given a
  single `identifier` argument to `step`.

  Attributes:
    selected_choice_type: The current choice type selected in an ofType call.
      Used to keep track of the selected type in a choice type field.
  """

  @property
  def current_type(self) -> Optional[StructureDefinition]:
    """If self.element has a single type, returns the structure definition."""
    if _utils.is_root_element(self.element):
      return self.containing_type

    type_codes = _utils.element_type_codes(self.element)

    if len(type_codes) == 1:
      return self._env.get_structure_definition_for(
          _utils.get_absolute_uri_for_structure(type_codes[0])
      )

    return None

  @property
  def containing_type(self) -> StructureDefinition:
    """Returns the current containing type."""
    return self._containing_type

  @property
  def element(self) -> ElementDefinition:
    """Returns the current element."""
    return self._element

  def __init__(
      self,
      env: _Environment,
      initial_type: StructureDefinition,
      initial_element: Optional[ElementDefinition] = None,
  ) -> None:
    """Creates a new `FhirStructureDefinitionWalker`.

    Args:
      env: A reference to the underlying `_Environment` to traverse over.
      initial_type: The initial `StructureDefinition`.
      initial_element: The initial `ElementDefinition`. If `None`, the root
        element of `initial_type` is chosen. Defaults to `None`.
    """
    self.selected_choice_type = ''
    self._env = env
    self._containing_type = initial_type
    self._element = (
        self._env.get_root_element_for(initial_type)
        if initial_element is None
        else initial_element
    )

  def __copy__(self) -> 'FhirStructureDefinitionWalker':
    return type(self)(self._env, self.containing_type, self.element)

  def step(self, identifier: str):
    """Traverses to element at `identifier` and advances walker to new state.

    Args:
      identifier: The new path identifier indicating a single step in the FHIR
        resource traversal.

    Raises:
      ValueError: In the event that the `identifier` traversal is malformed, and
        a new "child" in the hierarchy cannot be found.
    """
    # First search through children of `self.element` that does not need to
    # switch to a different containing_type. Return if any is found.
    # E.g. backbone elements, including FHIR choice types (with the [X] suffix).
    inline_children = self._env.get_children(self.containing_type, self.element)
    element_path = cast(Any, self.element).path.value
    inline_path = '.'.join([element_path, identifier])
    choice_type_inline_path = '.'.join([element_path, f'{identifier}[x]'])
    extension_inline_id = '.'.join([element_path, f'extension:{identifier}'])
    for child in cast(List[Any], inline_children):
      # Recursive elements and non-extension slices are not in the SQL
      # representation, so skip those.
      # TODO(b/223622513): Define strategy for recursive element valiation.
      regular_path_element = not (
          (
              _utils.is_slice_element(child)
              and not _utils.is_slice_on_extension(child)
          )
          or _utils.is_recursive_element(child)
      )
      path_matches = (
          child.path.value == inline_path
          or child.path.value == choice_type_inline_path
          or child.id.value == extension_inline_id
      )
      if regular_path_element and path_matches:
        self._element = child
        return

    # No children were found within the current containing_type; search for them
    # while updating `self.containing_type` if any is found.
    # E.g. `self.element` is `Period`, identifier is `start`.
    if not _utils.is_root_element(self.element):
      uri_value = ''
      # TODO(b/221322122): Potentially remove this after semantic refactoring.
      if len(_utils.element_type_codes(self.element)) > 1:
        uri_value = self.selected_choice_type
        if not uri_value:
          raise ValueError(
              'Selected choice type not set for element:'
              f' {cast(Any, self.element).id.value!r} and identifier:'
              f' {identifier!r}'
          )
        else:
          self.selected_choice_type = ''
      else:
        uri_value = _utils.element_type_code(self.element)

      url = _utils.get_absolute_uri_for_structure(uri_value)
      path = _utils.get_absolute_identifier(uri_value, identifier)

      # If the current element is a slice on an extension, use the url of that
      # slice instead.
      if _utils.is_slice_on_extension(self.element):
        urls = _utils.slice_element_urls(self.element)
        if not urls:
          raise ValueError(
              'Unable to get url for slice on extension with id: '
              f'{cast(Any, self.element).id.value}'
          )
        if len(urls) > 1:
          raise ValueError(
              'TODO(b/190679571): Add support for more than one type.'
          )

        url = urls[0]

      containing_type = self._env.get_structure_definition_for(url)
      if containing_type is None:
        raise ValueError(f'Unable to find `StructureDefinition` for: {url}.')

      root_element = self._env.get_root_element_for(containing_type)
      if root_element is None:
        raise ValueError(f'Unable to find root `ElementDefinition` for: {url}.')

      children = self._env.get_children(containing_type, root_element)
      for child in cast(List[Any], children):
        if not _utils.is_slice_element(child) and child.path.value == path:
          self._containing_type = containing_type
          self._element = child
          return

    raise ValueError(
        'Unable to find child under containing_type: '
        f'{cast(Any, self.containing_type).url.value!r}, '
        f'element: {cast(Any, self.element).id.value!r}, '
        f'for identifier: {identifier!r}.'
    )

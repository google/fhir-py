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
"""Utilities used across functions in the fhir_path module."""

import itertools
import re
from typing import Any, List, Optional, cast

from google.protobuf import message
from google.fhir.core.utils import proto_utils

# TODO(b/201107372): Update FHIR-agnostic types to a protocol.
StructureDefinition = message.Message
ElementDefinition = message.Message
Constraint = message.Message


def is_root_element(element_definition: ElementDefinition) -> bool:
  """Returns `True` if `element_definition` is the root element."""
  path_value: str = cast(Any, element_definition).path.value
  return '.' not in path_value


def element_type_code(element_definition: ElementDefinition) -> str:
  """Returns the first element type code value."""
  # TODO(b/190679571): Handle choice types, which may have more than one
  # `type.code` value present.
  type_codes: List[str] = element_type_codes(element_definition)
  if len(type_codes) != 1:
    raise ValueError('TODO(b/190679571): Add support for more than one type.')
  return type_codes[0]


def element_type_codes(element_definition: ElementDefinition) -> List[str]:
  """Returns the list of element type codes.

  Describes the URL of the data type or resource that is a(or the) type used for
  this element. References are URLs that are relative to
  http://hl7.org/fhir/StructureDefinition. E.g.: "string" is a reference to:
  http://hl7.org/fhir/StructureDefinition/string.

  Absolute URLs are only allowed in logical models.

  Args:
    element_definition: The `ElementDefinition` whose types to retrieve.

  Returns:
    A list of strings representing the element's type code values.
  """
  result: List[str] = []
  if proto_utils.field_is_set(element_definition, 'type'):
    type_refs: List[StructureDefinition] = proto_utils.get_value_at_field(
        element_definition, 'type'
    )
    result.extend([cast(Any, t).code.value for t in type_refs])
  return result


def slice_element_urls(element_definition: ElementDefinition) -> List[str]:
  """Returns the list of profile urls for the given slice element.

  Args:
    element_definition: The `ElementDefinition` whose profile urls we are
      retrieving.

  Returns:
    A list of strings representing the element's profile urls.
  """
  result: List[str] = []
  if proto_utils.field_is_set(element_definition, 'type'):
    type_refs: List[StructureDefinition] = proto_utils.get_value_at_field(
        element_definition, 'type'
    )
    profile_lists = [cast(Any, t).profile for t in type_refs]
    urls = [
        cast(Any, profile).value
        for profile in itertools.chain.from_iterable(profile_lists)
    ]
    result.extend(urls)
  return result


_SLICE_ON_EXTENSION_ID_RE = re.compile(r'(^|.)extension:')


def is_slice_on_extension(element_definition: message.Message) -> bool:
  """Returns `True` if the given element is describing a slice on an extension.

  More information about extensions:
  http://hl7.org/fhir/defining-extensions.html.

  Args:
    element_definition: The element definition (element) that we are checking.
  """
  type_codes = element_type_codes(element_definition)
  return (
      'Extension' in type_codes
      and _SLICE_ON_EXTENSION_ID_RE.search(
          cast(Any, element_definition).id.value
      )
      is not None
  )


def is_slice_element(element_definition: message.Message) -> bool:
  """Returns `True` if the `element_definition` is describing a slice.

  More information about slices:
  https://www.hl7.org/fhir/profiling.html#slicing.

  Args:
    element_definition: The element definition that we are checking.
  """
  id_value = cast(Any, element_definition).id.value
  return ':' in id_value


def is_slice_but_not_on_extension(
    element_definition: message.Message,
) -> bool:
  """Returns `True` if the `element_definition` is a slice not on extension.

  Args:
    element_definition: The element definition that we are checking.
  """
  return is_slice_element(element_definition) and not is_slice_on_extension(
      element_definition
  )


def is_recursive_element(element_definition: message.Message) -> bool:
  """Returns `True` if the `element_definition` describes a recursive element.

  A FHIR element is recursive if it is a content reference to an element
  that is an ancestor in the structure definition. For example,
  https://www.hl7.org/fhir/parameters.html has a recursive "part" element.

  Args:
    element_definition: The element definition that we are checking.
  """
  content_ref = cast(Any, element_definition).content_reference.value
  if not content_ref or not content_ref.startswith('#'):
    return False

  elem_path = cast(Any, element_definition).path.value
  return elem_path.startswith(content_ref[1:])


def get_absolute_uri_for_structure(uri_value: str) -> str:
  """Returns an absolute Structure Definition `uri_value`.

  If `uri_value` is a string describing a relative type, the returned absolute
  uri will be prefixed by 'http://hl7.org/fhir/StructureDefinition/'. Otherwise,
  if `uri_value` is already an absolute uri, this method returns the input
  string.

  Args:
    uri_value: The URI of a data type or resource used for an element. This may
      be a URL or URI.

  Returns:
    An absolute URI to the data type or resource.
  """
  if (
      uri_value.startswith('http:')
      or uri_value.startswith('https:')
      or uri_value.startswith('urn:')
  ):
    return uri_value  # No-op
  return f'http://hl7.org/fhir/StructureDefinition/{uri_value}'


def get_absolute_identifier(root: str, identifier: str) -> str:
  """Returns `identifier` prefixed by '{root}.'."""
  if identifier.startswith(root):
    return identifier  # No-op
  return '.'.join([root, identifier])


def is_repeated_element(element_definition: ElementDefinition) -> bool:
  """Returns `True` if the `element_definition` describes a repeated field."""
  # Assume single field if `max` is not set. Note that both min/max *must* be
  # set in the context of a `StructureDefinition.Snapshot`, per the FHIR
  # specification: https://www.hl7.org/fhir/elementdefinition.html#min-max.
  if not proto_utils.field_is_set(element_definition, 'max'):
    return False
  max_value: str = cast(Any, element_definition).max.value
  return max_value != '0' and max_value != '1'


def get_element(
    structdef: StructureDefinition, path: str
) -> Optional[ElementDefinition]:
  """Returns the ElementDefintion proto for a path."""
  struct_id = cast(Any, structdef).id.value
  qualified_path = struct_id + '.' + path if path else struct_id
  qualified_choice_path = qualified_path + '[x]'
  for elem in cast(Any, structdef).snapshot.element:
    if (
        elem.id.value == qualified_path
        or elem.id.value == qualified_choice_path
    ):
      return elem

  return None


def is_backbone_element(elem: ElementDefinition) -> bool:
  for elem_type in cast(Any, elem).type:
    if elem_type.code.value == 'BackboneElement':
      return True
  return False


def is_polymorphic_element(elem: ElementDefinition) -> bool:
  return '[x]' in cast(Any, elem).id.value


def get_patient_reference_element_paths(
    structdef: StructureDefinition,
) -> List[str]:
  """Returns all the top level patient elements for a given Reference.

  Args:
    structdef: a FHIR StructureDefinition proto.

  Returns:
    A list of patients.
  """
  results = []
  struct_id = cast(Any, structdef).id.value

  for elem in cast(Any, structdef).snapshot.element:
    for t in elem.type:
      for tp in t.target_profile:
        if tp.value.endswith('Patient'):
          results.append(elem.id.value[len(struct_id) + 1 :])

  return results


def get_backbone_element_fields(
    structdef: StructureDefinition, path: str
) -> List[str]:
  """Returns the field under the path to the given FHIR backbone element.

  Args:
    structdef: a FHIR StructureDefinition proto.
    path: a path to a backbone element within the structure definition.

  Returns:
    A list of nested field names.
  """
  results = []
  struct_id = cast(Any, structdef).id.value
  qualified_path = struct_id + '.' + path if path else struct_id

  for elem in cast(Any, structdef).snapshot.element:
    if elem.id.value.startswith(qualified_path):
      relative_path = elem.id.value[len(qualified_path) + 1 :]
      if relative_path and '.' not in relative_path:
        # Trim choice field annotation if present.
        if relative_path.endswith('[x]'):
          relative_path = relative_path[:-3]
        results.append(relative_path)

  return results


def get_root_element_definition(
    structure_definition: StructureDefinition,
) -> ElementDefinition:
  """Returns the root element definition in a given structure definition."""

  root_element: ElementDefinition = None
  for element_definition in cast(Any, structure_definition).snapshot.element:
    if is_root_element(element_definition):
      if root_element is not None:
        raise ValueError(
            'Expected a single root ElementDefinition but got: '
            f'{cast(Any, root_element).id.value!r} and '
            f'{cast(Any, element_definition).id.value!r}.'
        )
      root_element = element_definition

  return root_element

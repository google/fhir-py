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
"""Resource and and reference data context for FHIRPath usage."""

import abc
from typing import Any, Dict, Generic, Iterable, List, Optional, Type, TypeVar, cast

import requests

from google.protobuf import message
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.internal import primitive_handler
from google.fhir.core.internal.json_format import _json_parser
from google.fhir.core.utils import fhir_package


class UnableToLoadResourceError(Exception):
  """Unable to load a needed FHIR resource."""


# Type variables for FHIR StructureDefinition and ValueSet resources, allowing
# instances of FhirPathContext to be paramterized with FHIR version-specific
# resources.
_StructDefT = TypeVar('_StructDefT')
_ValueSetT = TypeVar('_ValueSetT')

VALUE_SET_URL = 'http://hl7.org/fhir/StructureDefinition/ValueSet'
QUANTITY_URL = 'http://hl7.org/fhir/StructureDefinition/Quantity'

ElementDefinition = message.Message


class FhirPathContext(Generic[_StructDefT, _ValueSetT], abc.ABC):
  """Resource and reference data context for FHIRPath usage.

  Implementations of this class should cache loaded resources so they can be
  reused. They may pull from a locally-stored implementation guide, a package,
  a remote FHIR server, or other mechanism as appropriate for the user.
  """

  @abc.abstractmethod
  def get_structure_definition(self, url: str) -> _StructDefT:
    """Returns the FHIR StructureDefinition defined by the URL.

    Args:
      url: URL of the FHIR StructureDefinition to load. Per the FHIR spec, an
        unqualified URL will be considered to be relative to
        http://hl7.org/fhir/StructureDefinition/, so for core datatypes or
          resources callers can simply pass in 'Patient' or 'HumanName', for
          example.

    Returns:
      A FHIR StructureDefinition.

    Raises:
      UnableToLoadResourceError if the resource cannot be loaded.
    """

  def get_dependency_definitions(self, url: str) -> List[_StructDefT]:
    """Returns all dependencies for the structure identified by the given URL.

    Args:
      url: The URL identifying the FHIR StructureDefinition to load dependencies
        for.

    Returns:
      The structure definitions depended on by the above URL.

    Raises:
      UnableToLoadResourceError if the resource cannot be loaded.
    """
    dependencies: Dict[str, _StructDefT] = {}
    urls_to_load: List[str] = [url]
    while urls_to_load:
      url_to_load = urls_to_load.pop()
      base_definition = self.get_structure_definition(url_to_load)
      for elem in base_definition.snapshot.element:
        for elem_type in elem.type:
          type_name = elem_type.code.value
          # Skip primitives and types we have already visited.
          if (_fhir_path_data_types.primitive_type_from_type_code(type_name) is
              None and type_name not in dependencies):
            child_struct = self.get_structure_definition(type_name)
            dependencies[type_name] = child_struct
            urls_to_load.append(child_struct.url.value)

    return list(dependencies.values())

  @abc.abstractmethod
  def get_value_set(self, value_set_url: str) -> Optional[_ValueSetT]:
    """Returns the ValueSet identified by the given URL.

    Args:
      value_set_url: The URL for the FHIR ValueSet to be returned.

    Returns:
      The corresponding value set, or None if no such value set exists.
    """

  def get_fhir_type_from_string(
      self, type_code: str, element_definition: Optional[ElementDefinition]
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Returns a FhirPathDataType from a type code string."""
    # If this is a primitive, simply return the corresponding primitive type.
    return_type = _fhir_path_data_types.primitive_type_from_type_code(type_code)

    # Load the structure definition for the non-primitive type.
    if return_type is None:
      child_structdef = self.get_structure_definition(type_code)
      if child_structdef.url.value == QUANTITY_URL:
        return_type = _fhir_path_data_types.QuantityStructureDataType(
            child_structdef)
      else:
        return_type = _fhir_path_data_types.StructureDataType(child_structdef)

    if not element_definition:
      return return_type

    # If an element definition is provided (from a parent) then override the
    # existing element definition saved.
    return return_type.get_fhir_type_with_root_element_definition(
        element_definition)

  def _maybe_return_collection_type(
      self, element: ElementDefinition,
      return_type: _fhir_path_data_types.FhirPathDataType,
      parent_type: Optional[_fhir_path_data_types.FhirPathDataType]
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Returns a new instance of return_type updated with its collection status.
    """
    if _utils.is_repeated_element(element):
      return return_type.get_new_cardinality_type(
          _fhir_path_data_types.Cardinality.COLLECTION)
    if parent_type and parent_type.returns_collection():
      return return_type.get_new_cardinality_type(
          _fhir_path_data_types.Cardinality.CHILD_OF_COLLECTION)
    return return_type

  def fhir_data_type_generator(
      self, element_definition: ElementDefinition, json_name: str,
      parent: Optional[_fhir_path_data_types.StructureDataType]
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Generated a FhirPathDataType from the parent and element definition."""
    elem = cast(Any, element_definition)

    if _utils.is_backbone_element(elem):
      if not parent:
        raise ValueError(f'Backbone element {json_name} needs to have a '
                         'parent to generate type.')
      structdef = parent.structure_definition
      elem_path = parent.backbone_element_path + '.' + json_name if parent.backbone_element_path else json_name
      return_type = _fhir_path_data_types.StructureDataType(
          structdef, elem_path)

    elif _utils.is_polymorphic_element(elem):
      struct_def_dict = {}
      for elem_type in elem.type:
        struct_def_dict[elem_type.code.value.casefold(
        )] = self._maybe_return_collection_type(
            elem, self.get_fhir_type_from_string(elem_type.code.value, elem),
            parent)
      return_type = _fhir_path_data_types.PolymorphicDataType(struct_def_dict)

    elif not elem.type or not elem.type[0].code.value:
      raise ValueError(f'Malformed ElementDefinition in struct {parent.url}')
    else:
      type_code = elem.type[0].code.value
      return_type = self.get_fhir_type_from_string(type_code, elem)

    return self._maybe_return_collection_type(elem, return_type, parent)

  def get_child_data_type(
      self, parent: Optional[_fhir_path_data_types.FhirPathDataType],
      json_name: str) -> Optional[_fhir_path_data_types.FhirPathDataType]:
    """Returns the data types of the given child field from the parent."""
    if parent is None:
      return None

    if isinstance(parent, _fhir_path_data_types.PolymorphicDataType):
      possible_types = cast(_fhir_path_data_types.PolymorphicDataType,
                            parent).types()
      if json_name.casefold() not in possible_types:
        raise ValueError(
            f'Identifier {json_name} not in {possible_types.keys()}')
      return possible_types[json_name.casefold()]

    if isinstance(parent, _fhir_path_data_types.StructureDataType):
      elem = parent.children().get(json_name)
      if elem is None:
        raise ValueError(
            f'Identifier {json_name} not in {parent.children().keys()}')
      return self.fhir_data_type_generator(elem, json_name, parent)
    else:
      raise ValueError(f'Parent {parent} does not contain children.')


class MockFhirPathContext(FhirPathContext[_StructDefT, _ValueSetT]):
  """FHIRPath context that simply pulls from a provided list of Structure Definitions.
  """

  def __init__(self,
               struct_defs: Iterable[_StructDefT],
               value_sets: Optional[Iterable[_ValueSetT]] = None) -> None:
    self._struct_defs: Dict[str, _StructDefT] = {}
    self._value_sets: Dict[str, _ValueSetT] = {}
    for value_set in value_sets or ():
      self.add_local_value_set(value_set)

    for struct_def in struct_defs or ():
      self.add_struct_def(struct_def)

  def add_struct_def(self, struct_def: _StructDefT) -> None:
    self._struct_defs[struct_def.url.value] = struct_def

  def add_local_value_set(self, value_set: _ValueSetT) -> None:
    """Adds a local valueset to the context so it can be used for valueset membership checks.
    """
    self._value_sets[value_set.url.value] = value_set

  def get_structure_definition(self, url: str) -> _StructDefT:
    qualified_url = _utils.get_absolute_uri_for_structure(url)
    result = self._struct_defs.get(qualified_url)
    if not result:
      raise ValueError(
          f'Missing structdef {qualified_url} from {self._struct_defs.keys()}')
    return result

  def get_value_set(self, value_set_url: str) -> Optional[_ValueSetT]:
    return self._value_sets.get(value_set_url)


class LocalFhirPathContext(FhirPathContext[_StructDefT, _ValueSetT]):
  """FHIRPath context that simply pulls from a provided collection of FHIR resources.
  """

  def __init__(self,
               package_manager: fhir_package.FhirPackageAccessor[_StructDefT,
                                                                 Any, Any,
                                                                 _ValueSetT],
               value_sets: Optional[Iterable[_ValueSetT]] = None) -> None:
    # Lazy load structure defintion since there may be many of them.
    self._package_manager = package_manager
    self._value_sets: Dict[str, _ValueSetT] = {}
    for value_set in value_sets or ():
      self.add_local_value_set(value_set)

  def add_local_value_set(self, value_set: _ValueSetT) -> None:
    """Adds a local valueset to the context so it can be used for valueset membership checks.
    """
    self._value_sets[value_set.url.value] = value_set

  def get_structure_definition(self, url: str) -> _StructDefT:

    # Add standard prefix to structure if necessary.
    qualified_url = _utils.get_absolute_uri_for_structure(url)
    result = self._package_manager.get_structure_definition(qualified_url)
    if result is None:
      raise UnableToLoadResourceError(f'Unknown structure definition URL {url}')
    return result

  def get_value_set(self, value_set_url: str) -> Optional[_ValueSetT]:
    return self._value_sets.get(value_set_url)


class ServerFhirPathContext(FhirPathContext[_StructDefT, _ValueSetT]):
  """FHIRPath context that obtains structure definitions from a specified server.
  """

  def __init__(self, server_base_url: str, struct_def_class: Type[_StructDefT],
               handler: primitive_handler.PrimitiveHandler):
    self._server_base_url = server_base_url
    self._struct_def_class = struct_def_class
    self._json_parser = _json_parser.JsonParser(handler, 'UTC')
    self._struct_defs: Dict[str, _StructDefT] = {}
    self._value_sets: Dict[str, _ValueSetT] = {}

  def _retreive_structure_definition(self, resource_url: str) -> _StructDefT:
    """Retreives the structure definition from the FHIR store."""
    response = requests.get(
        f'{self._server_base_url}/StructureDefinition',
        params={'_id': resource_url},
        headers={'accept': 'application/fhir+json'})

    if not response.ok:
      raise UnableToLoadResourceError(
          f'Unable to retrieve resource {resource_url}: {response.status_code} {response.reason}'
      )

    bundle_json = response.json()
    for entry in bundle_json.get('entry', ()):
      resource_json = entry.get('resource', {})
      if resource_json.get('url') == resource_url:
        struct_def = self._struct_def_class()
        self._json_parser.merge_value(resource_json, struct_def)
        return struct_def

    raise UnableToLoadResourceError(
        f'Expected resource not found in response: {resource_url}')

  def add_local_value_set(self, value_set: _ValueSetT):
    """Adds a local valueset to the context so it can be used for valueset membership checks.
    """
    self._value_sets[value_set.url.value] = value_set

  def get_structure_definition(self, url: str) -> _StructDefT:

    # Add standard prefix to structure if necessary.
    qualified_url = _utils.get_absolute_uri_for_structure(url)
    struct_def = self._struct_defs.get(qualified_url)
    if struct_def is None:
      struct_def = self._retreive_structure_definition(qualified_url)
      self._struct_defs[qualified_url] = struct_def
    return struct_def

  def get_value_set(self, value_set_url: str) -> Optional[_ValueSetT]:
    return self._value_sets.get(value_set_url)

#
# Copyright 2022 Google LLC
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
"""Utilities for working with Value Sets."""

import itertools
from typing import cast, Iterable, Optional, Set
import urllib.parse

import logging

from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.utils import fhir_package
from google.fhir.r4.terminology import local_value_set_resolver
from google.fhir.r4.terminology import terminology_service_client

_PRIMITIVE_STRUCTURE_DEFINITION_URLS = frozenset((
    'http://hl7.org/fhirpath/System.Boolean',
    'http://hl7.org/fhirpath/System.Date',
    'http://hl7.org/fhirpath/System.DateTime',
    'http://hl7.org/fhirpath/System.Decimal',
    'http://hl7.org/fhirpath/System.Integer',
    'http://hl7.org/fhirpath/System.String',
    'http://hl7.org/fhirpath/System.Time',
))


class ValueSetResolver:
  """Utility for retrieving and resolving value set resources to the codes they contain.

  Attributes:
    package_manager: The FhirPackageManager object to use when retrieving
      resource definitions. The FhirPackage objects contained in package_manager
      will be consulted when value set resource definitions are needed. The
      package manager should contain common resources, for instance, ones from
      the US Core implementation guide, in order to ensure definitions for all
      relevant value sets may be found. If a requisite value set definition is
      not present in the package manager, the resolver will throw an error
      instead of attempting to retrieve it over the network.
    terminology_client: The terminology service client to use when expanding
      value sets which can not be expanded using local definitions from the
      package_manager.
  """

  def __init__(
      self,
      package_manager: fhir_package.FhirPackageManager,
      terminology_client: terminology_service_client.TerminologyServiceClient,
  ) -> None:
    self.package_manager = package_manager
    self.terminology_client = terminology_client
    self._resolvers = (
        local_value_set_resolver.LocalResolver(package_manager),
        terminology_client,
    )

  def value_set_urls_from_fhir_package(
      self, package: fhir_package.FhirPackage
  ) -> Iterable[str]:
    """Retrieves URLs for all value sets referenced by the given FHIR package.

    Finds all value set resources in the package as well as any value sets
    referenced by structure definitions in the package.

    Args:
      package: The FHIR package from which to retrieve value sets.

    Yields:
      URLs for all value sets referenced by the FHIR package.
    """
    value_set_urls_from_structure_definitions = itertools.chain.from_iterable(
        self.value_set_urls_from_structure_definition(structure_definition)
        for structure_definition in package.structure_definitions
    )
    value_set_urls_from_value_sets = (
        value_set.url.value for value_set in package.value_sets
    )
    all_value_set_urls = itertools.chain(
        value_set_urls_from_value_sets,
        value_set_urls_from_structure_definitions,
    )
    yield from _unique_urls(all_value_set_urls)

  def value_set_urls_from_structure_definition(
      self, structure_definition: structure_definition_pb2.StructureDefinition
  ) -> Iterable[str]:
    """Retrieves URLs for value sets referenced by the structure definition.

    Finds value sets bound to any element in the definition's snapshot elements.
    Also finds value sets bound to the structure definitions of any of the
    elements' types.

    Args:
      structure_definition: The structure definition from which to retrieve
        value sets.

    Yields:
      URLs for all value sets referenced by the structure definition or its
      elements' types.

    Raises:
      NotImplementedError: If the structure definition does not provide a
      snapshot element definition.
    """
    element_structure_definitions = (
        self._get_structure_defintions_for_elements_of(structure_definition)
    )
    all_definitions = itertools.chain(
        (structure_definition,), element_structure_definitions
    )
    all_value_set_urls = itertools.chain.from_iterable(
        self._value_set_urls_bound_to_elements_of(definition)
        for definition in all_definitions
    )

    yield from _unique_urls(all_value_set_urls)

  def _value_set_urls_bound_to_elements_of(
      self, structure_definition: structure_definition_pb2.StructureDefinition
  ) -> Iterable[str]:
    """Gets URLs for value sets bound to the definition's snapshot elements."""
    # The FHIR spec requires at least one of these be present.
    if (
        structure_definition.differential.element
        and not structure_definition.snapshot.element
    ):
      raise NotImplementedError(
          'Structure definition %s with differential elements not yet'
          ' supported.'
          % structure_definition.url.value
      )

    value_set_urls = (
        element.binding.value_set.value
        for element in structure_definition.snapshot.element
        if element.binding.value_set.value
    )

    if structure_definition.url.value in (
        'http://hl7.org/fhir/StructureDefinition/ExplanationOfBenefit',
        'https://g.co/fhir/medicalrecords/StructureDefinition/ExplanationOfBenefit',
    ):
      # A bug in the FHIR spec has this structure definition bound to a code
      # system by mistake. It should be bound to a value set instead. We swap
      # the URLs until the bug is addressed.
      # https://jira.hl7.org/browse/FHIR-36128
      bad_code_system_url = (
          'http://terminology.hl7.org/CodeSystem/processpriority'
      )
      correct_value_set_url = 'http://hl7.org/fhir/ValueSet/process-priority'
      value_set_urls = (
          correct_value_set_url if url == bad_code_system_url else url
          for url in value_set_urls
      )

    return value_set_urls

  def _get_structure_defintions_for_elements_of(
      self,
      structure_definition: structure_definition_pb2.StructureDefinition,
      already_retrieved: Optional[Set[str]] = None,
  ) -> Iterable[structure_definition_pb2.StructureDefinition]:
    """Retrieves structure definitions for `structure_definition`'s elements.

    Given a structure definition, retrieve the structure definitions for each of
    its elements' types. Recursively descends into those elements' types'
    structure definitions as well.
    The structure definitions returned will be unique, even if the structure
    definition has multiple elements of the same type.
    Does not attempt to return structure definitions for primitives.

    Args:
      structure_definition: The structure definition for which to retrieve
        definitions of element types.
      already_retrieved: Any URLs of structure definitions already retrieved by
        previous _get_structure_defintions_for_elements_of calls.

    Yields:
      The unique set of structure definitions describing the types of all given
      structure definitions.
    """
    already_retrieved = already_retrieved or set()
    for url in _get_structure_defintion_urls_for_elements_of(
        structure_definition
    ):
      if (
          url in _PRIMITIVE_STRUCTURE_DEFINITION_URLS
          or url in already_retrieved
      ):
        continue

      definition_for_element = cast(
          structure_definition_pb2.StructureDefinition,
          self.package_manager.get_resource(url),
      )
      if definition_for_element is None:
        logging.warning(
            (
                'Unable to look up structure definition %s '
                'for element of structure definition %s'
            ),
            url,
            structure_definition.url.value,
        )
        continue

      already_retrieved.add(url)
      yield definition_for_element
      yield from self._get_structure_defintions_for_elements_of(
          definition_for_element, already_retrieved
      )

  def expand_value_set_url(self, url: str) -> Optional[value_set_pb2.ValueSet]:
    """Retrieves the expanded value set definition for the given URL.

    Attempts to expand the value set using definitions available to the
    instance's package manager. If the expansion can not be performed with
    available resources, makes network calls to a terminology service to perform
    the expansion.

    Args:
      url: The URL of the value set to expand.

    Returns:
      A value set protocol buffer expanded to include the codes it represents.
    """
    for resolver in self._resolvers:
      expanded_value_set = resolver.expand_value_set_url(url)
      if expanded_value_set is not None:
        return expanded_value_set
    return None


def _unique_urls(urls: Iterable[str]) -> Iterable[str]:
  """Filters URLs to remove duplicates.

  Args:
    urls: The URLs to filter.

  Yields:
    The URLs filtered to only those without duplicates.
  """
  seen: Set[str] = set()

  for url in urls:
    if url not in seen:
      seen.add(url)
      yield url


def _get_structure_defintion_urls_for_elements_of(
    structure_definition: structure_definition_pb2.StructureDefinition,
) -> Iterable[str]:
  """Finds the URLs of all types referenced by the definition's elements."""
  types = itertools.chain.from_iterable(
      element.type for element in structure_definition.snapshot.element
  )
  type_codes = (type_.code.value for type_ in types)
  # If not already a URL, create a URL for the type name.
  as_urls = (
      type_code
      if urllib.parse.urlparse(type_code).scheme
      else 'http://hl7.org/fhir/StructureDefinition/%s' % type_code
      for type_code in type_codes
  )

  return as_urls

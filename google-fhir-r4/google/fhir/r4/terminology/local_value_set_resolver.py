#
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
"""Utilities for expanding value sets without using a terminology service."""

import copy
import itertools
from typing import Iterable, List, Optional, Sequence

import logging

from google.fhir.r4.proto.core.resources import code_system_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.utils import fhir_package
from google.fhir.core.utils import proto_utils
from google.fhir.core.utils import url_utils


class LocalResolver:
  """Resolver which uses a local package manager to expand value sets.

  This resolver never attempts to access remote terminology services.
  """

  def __init__(self, package_manager: fhir_package.FhirPackageManager):
    self._package_manager = package_manager

  def expand_value_set_url(self, url: str) -> Optional[value_set_pb2.ValueSet]:
    """Attempts to expand value sets without contacting a terminology service.

    For value sets with an extensional set of codes, collect all codes
    referenced in the value set's 'compose' field.

    For value sets which reference a code system without specifying the
    extensional set of codes within it, attempt to find the definition of the
    code system in the ValueSetResolver and expand to all codes in the code
    system. If the code system resource is not available locally, returns `None`
    indicating a terminology service should instead be used to find the value
    set expansion.

    If the value set has an intensional set of codes, returns `None` indicating
    a terminology service should instead be used to find the value set
    expansion.

    See https://www.hl7.org/fhir/valueset.html#int-ext
    for more details about value set expansion and intensional versus
    extensional value sets.

    Args:
      url: The url of the value set for which to retrieve expanded codes.

    Returns:
      The expanded value set or None if a terminology service should be
      consulted instead.
    """
    value_set = self._value_set_from_url(url)
    if value_set is None:
      return None

    concept_sets = itertools.chain(
        value_set.compose.include, value_set.compose.exclude
    )
    if any(concept_set.filter for concept_set in concept_sets):
      # The value set requires intensional filtering rules we do not implement.
      # We may wish to reduce the frequency with which we need to defer to
      # external terminology services.
      # TODO(b/223659948): Add more support for filtering logic.
      return None

    includes = [
        self._concept_set_to_expansion(value_set, include)
        for include in value_set.compose.include
    ]
    excludes = [
        self._concept_set_to_expansion(value_set, exclude)
        for exclude in value_set.compose.exclude
    ]

    if None in includes or None in excludes:
      # The value set references code system definitions unavailable locally.
      return None

    logging.info(
        'Expanding value set url: %s version: %s locally',
        value_set.url.value,
        value_set.version.value,
    )
    includes = itertools.chain.from_iterable(includes)
    excludes = itertools.chain.from_iterable(excludes)

    # Build tuples of the fields to use for equality when testing if a code from
    # include is also in exclude.
    codes_to_remove = set(
        (concept.version.value, concept.system.value, concept.code.value)
        for concept in excludes
    )
    # Use tuples of the same form to filter excluded codes.
    codes = [
        concept
        for concept in includes
        if (concept.version.value, concept.system.value, concept.code.value)
        not in codes_to_remove
    ]

    expanded_value_set = copy.deepcopy(value_set)
    expanded_value_set.expansion.contains.extend(codes)
    return expanded_value_set

  def _value_set_from_url(self, url: str) -> Optional[value_set_pb2.ValueSet]:
    """Retrieves the value set for the given URL.

    The value set is assumed to be a member of one of the packages contained in
    self._package_manager. This function will not attempt to look up resources
    over the network in other locations.

    Args:
      url: The url of the value set to retrieve.

    Returns:
      The value set for the given URL or None if it can not be found in the
      package manager.

    Raises:
      ValueError: If the URL belongs to a resource that is not a value set.
    """
    url, version = url_utils.parse_url_version(url)
    value_set = self._package_manager.get_resource(url)
    if value_set is None:
      logging.info(
          'Unable to find value set for url: %s in given resolver packages.',
          url,
      )
      return None
    elif not isinstance(value_set, value_set_pb2.ValueSet):
      raise ValueError(
          'URL: %s does not refer to a value set, found: %s'
          % (url, value_set.DESCRIPTOR.name)
      )
    elif version is not None and version != value_set.version.value:
      logging.warning(
          (
              'Found incompatible version for value set with url: %s.'
              ' Requested: %s, found: %s'
          ),
          url,
          version,
          value_set.version.value,
      )
      return None
    else:
      return value_set

  def _concept_set_to_expansion(
      self,
      value_set: value_set_pb2.ValueSet,
      concept_set: value_set_pb2.ValueSet.Compose.ConceptSet,
  ) -> Optional[Sequence[value_set_pb2.ValueSet.Expansion.Contains]]:
    """Expands the ConceptSet into a collection of Expansion.Contains objects.

    Args:
      value_set: The value set for which the concept set is being expanded.
      concept_set: The concept set to expand.

    Returns:
      The expansion represented by the concept set or None if the expansion
      requires a code system definition not present locally.
    """
    if concept_set.concept:
      # If given, take the expansion as the list of given codes.
      concepts = concept_set.concept
    else:
      # No explicit codes list is given, meaning we should take the expansion as
      # the entire code system.
      logging.info(
          'Expanding value set: %s version: %s to entire code system: %s',
          value_set.url.value,
          value_set.version.value,
          concept_set.system.value,
      )
      code_system = self._package_manager.get_resource(concept_set.system.value)

      if code_system is None:
        logging.warning(
            (
                'Expansion of code system: %s for value set: %s version: %s'
                ' requires code system definition not available locally.'
                ' Deferring expansion to external terminology service.'
            ),
            concept_set.system.value,
            value_set.url.value,
            value_set.version.value,
        )
        return None
      elif not isinstance(code_system, code_system_pb2.CodeSystem):
        raise ValueError(
            'system: %s does not refer to a code system, found: %s'
            % (concept_set.system.value, code_system.DESCRIPTOR.name)
        )
      else:
        concepts = _flatten_code_system_concepts(code_system.concept)

    expansion: List[value_set_pb2.ValueSet.Expansion.Contains] = []
    for concept in concepts:
      contains = value_set_pb2.ValueSet.Expansion.Contains()
      for field, copy_from in (
          ('system', concept_set),
          ('version', concept_set),
          ('code', concept),
          ('display', concept),
      ):
        proto_utils.copy_common_field(copy_from, contains, field)

      for designation in concept.designation:
        designation_copy = contains.designation.add()
        for field in (
            'id',
            'extension',
            'modifier_extension',
            'language',
            'use',
            'value',
        ):
          proto_utils.copy_common_field(designation, designation_copy, field)

      expansion.append(contains)
    return expansion


def _flatten_code_system_concepts(
    concepts: Sequence[code_system_pb2.CodeSystem.ConceptDefinition],
) -> Iterable[code_system_pb2.CodeSystem.ConceptDefinition]:
  """Flattens all concepts in the given set of code system concepts."""
  for concept in concepts:
    yield concept

    # Code system concepts can contain a set of nested concepts. Yield
    # those nested concepts as well.
    if concept.concept:
      yield from _flatten_code_system_concepts(concept.concept)

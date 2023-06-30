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
"""Test value_sets functionality."""

from typing import Iterable, Type, TypeVar
from unittest import mock
from google.protobuf import message
from google.protobuf import text_format
from absl.testing import absltest
from google.fhir.r4.proto.core.resources import code_system_pb2
from google.fhir.r4.proto.core.resources import search_parameter_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import primitive_handler
from google.fhir.r4 import r4_package
from google.fhir.r4.terminology import terminology_service_client
from google.fhir.r4.terminology import value_sets

TResourceCollection = TypeVar('TResourceCollection', bound=message.Message)


def _get_empty_collection(
    msg_cls: Type[TResourceCollection],
) -> fhir_package.ResourceCollection[TResourceCollection]:
  return fhir_package.ResourceCollection(
      msg_cls, primitive_handler.PrimitiveHandler(), 'Z'
  )


def _build_mock_collection(
    resources: Iterable[TResourceCollection],
) -> mock.MagicMock:
  return mock.MagicMock(
      spec=fhir_package.ResourceCollection,
      __iter__=lambda _: iter(resources),
  )


class ValueSetsTest(absltest.TestCase):

  def testValueSetUrlsFromStructureDefinition_withValueSets_succeeds(self):
    definition = structure_definition_pb2.StructureDefinition()

    # Add an a element to the snapshot definition.
    element = definition.snapshot.element.add()
    element.binding.value_set.value = (
        'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )

    # Add an element without a URL.
    definition.snapshot.element.add()

    # Add another element to the snapshot definition.
    another_element = definition.snapshot.element.add()
    another_element.binding.value_set.value = (
        'http://hl7.org/fhir/ValueSet/account-status'
    )

    # Add yet another element to the snapshot definition.
    snapshot_element = definition.snapshot.element.add()
    snapshot_element.binding.value_set.value = (
        'http://hl7.org/fhir/ValueSet/action-participant-role'
    )

    # Add an element with a duplicated url which should be ignored.
    duplicate_element = definition.snapshot.element.add()
    duplicate_element.binding.value_set.value = (
        'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )

    # Add an element pointing to a type with a structure definition.
    typed_element = definition.snapshot.element.add()
    type_ = typed_element.type.add()
    type_.code.value = 'Magic'

    # Build a definition for the Magic type
    magic = structure_definition_pb2.StructureDefinition()
    magic.url.value = 'http://hl7.org/fhir/StructureDefinition/Magic'

    # Add an a element to magic's snapshot definition.
    pixie_dust = magic.snapshot.element.add()
    pixie_dust.binding.value_set.value = 'http://hl7.org/fhir/ValueSet/fey'

    package_manager = mock.MagicMock(spec=fhir_package.FhirPackageManager)
    package_manager.get_resource.side_effect = {magic.url.value: magic}.get

    resolver = value_sets.ValueSetResolver(package_manager, mock.MagicMock())

    result = resolver.value_set_urls_from_structure_definition(definition)
    self.assertCountEqual(
        list(result),
        [
            element.binding.value_set.value,
            another_element.binding.value_set.value,
            snapshot_element.binding.value_set.value,
            pixie_dust.binding.value_set.value,
        ],
    )

  def testValueSetUrlsFromStructureDefinition_withBuggyDefinition_succeeds(
      self,
  ):
    """Ensures we handle an incorrect binding to a code system.

    Addresses the issue https://jira.hl7.org/browse/FHIR-36128.
    """
    definition = structure_definition_pb2.StructureDefinition()
    definition.url.value = (
        'http://hl7.org/fhir/StructureDefinition/ExplanationOfBenefit'
    )

    element = definition.snapshot.element.add()
    element.binding.value_set.value = (
        'http://terminology.hl7.org/CodeSystem/processpriority'
    )

    result = get_resolver().value_set_urls_from_structure_definition(definition)
    self.assertEqual(
        list(result), ['http://hl7.org/fhir/ValueSet/process-priority']
    )

  def testValueSetUrlsFromStructureDefinition_withNoValueSets_returnsEmpty(
      self,
  ):
    definition = structure_definition_pb2.StructureDefinition()
    self.assertEqual(
        list(
            get_resolver().value_set_urls_from_structure_definition(definition)
        ),
        [],
    )

  def testValueSetUrlsFromStructureDefinition_withDifferentialElements_raisesNotImplemented(
      self,
  ):
    definition = structure_definition_pb2.StructureDefinition()
    element = definition.differential.element.add()
    element.binding.value_set.value = (
        'http://terminology.hl7.org/CodeSystem/processpriority'
    )
    with self.assertRaises(NotImplementedError):
      list(get_resolver().value_set_urls_from_structure_definition(definition))

  def testValueSetUrlsFromFhirPackage_withValueSets_succeeds(self):
    definition = structure_definition_pb2.StructureDefinition()
    element = definition.snapshot.element.add()
    element.binding.value_set.value = (
        'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )

    another_definition = structure_definition_pb2.StructureDefinition()
    another_element = another_definition.snapshot.element.add()
    another_element.binding.value_set.value = (
        'http://hl7.org/fhir/ValueSet/action-participant-role'
    )

    value_set = value_set_pb2.ValueSet()
    value_set.url.value = 'a-url'

    another_value_set = value_set_pb2.ValueSet()
    another_value_set.url.value = 'another-url'

    duplicate_value_set = value_set_pb2.ValueSet()
    duplicate_value_set.url.value = (
        'http://hl7.org/fhir/ValueSet/action-participant-role'
    )

    package = fhir_package.FhirPackage(
        ig_info=fhir_package.IgInfo(
            name='name',
            version='version',
        ),
        structure_definitions=_build_mock_collection(
            [definition, another_definition]
        ),
        search_parameters=_get_empty_collection(
            search_parameter_pb2.SearchParameter
        ),
        code_systems=_get_empty_collection(code_system_pb2.CodeSystem),
        value_sets=_build_mock_collection(
            [value_set, another_value_set, duplicate_value_set],
        ),
    )

    result = get_resolver().value_set_urls_from_fhir_package(package)

    self.assertCountEqual(
        list(result),
        [
            element.binding.value_set.value,
            another_element.binding.value_set.value,
            value_set.url.value,
            another_value_set.url.value,
        ],
    )

  def testValueSetUrlsFromFhirPackage_withEmptyPackage_returnsEmpty(self):
    package = fhir_package.FhirPackage(
        ig_info=fhir_package.IgInfo(
            name='name',
            version='version',
        ),
        structure_definitions=_get_empty_collection(
            structure_definition_pb2.StructureDefinition
        ),
        search_parameters=_get_empty_collection(
            search_parameter_pb2.SearchParameter
        ),
        code_systems=_get_empty_collection(code_system_pb2.CodeSystem),
        value_sets=_get_empty_collection(value_set_pb2.ValueSet),
    )
    self.assertEqual(
        list(get_resolver().value_set_urls_from_fhir_package(package)), []
    )

  def testExpandValueSet_withMissingResource_callsTerminologyService(self):
    mock_package_manager = mock.MagicMock()
    mock_package_manager.get_resource.return_value = None

    mock_client = mock.MagicMock(
        spec=terminology_service_client.TerminologyServiceClient
    )

    resolver = value_sets.ValueSetResolver(mock_package_manager, mock_client)

    result = resolver.expand_value_set_url('http://some-url')
    self.assertEqual(
        result, mock_client.expand_value_set_url('http://some-url')
    )

  def testExpandValueSet_returnsResultFromFirstSuccessfuleResolver(self):
    first_resolver = mock.MagicMock()
    second_resolver = mock.MagicMock()

    resolver = value_sets.ValueSetResolver(mock.MagicMock(), mock.MagicMock())
    resolver._resolvers = (first_resolver, second_resolver)

    result = resolver.expand_value_set_url('http://some-url')

    first_resolver.expand_value_set_url.assert_called_once_with(
        'http://some-url'
    )
    second_resolver.expand_value_set_url.assert_not_called()
    self.assertEqual(
        result, first_resolver.expand_value_set_url('http://some-url')
    )

  def testExpandValueSet_returnsResultFromSecondResolverIfFirstReturnsNone(
      self,
  ):
    first_resolver = mock.MagicMock()
    first_resolver.expand_value_set_url.return_value = None

    second_resolver = mock.MagicMock()

    resolver = value_sets.ValueSetResolver(mock.MagicMock(), mock.MagicMock())
    resolver._resolvers = (first_resolver, second_resolver)

    result = resolver.expand_value_set_url('http://some-url')

    first_resolver.expand_value_set_url.assert_called_once_with(
        'http://some-url'
    )
    second_resolver.expand_value_set_url.assert_called_once_with(
        'http://some-url'
    )
    self.assertEqual(
        result, second_resolver.expand_value_set_url('http://some-url')
    )

  def testGetStructureDefinitionsForElementsOf_withElements_findsDefinitionsForElements(
      self,
  ):
    definition = structure_definition_pb2.StructureDefinition()
    text_format.Parse(
        """
        url {
          value: "http://hl7.org/fhir/StructureDefinition/definition"
        }
        snapshot {"""
        # Ensure we handle both type names and URLs.
        """
          element {
            type {
              code {
                value: "Snake"
              }
            }
            type {
              code {
                value: "http://hl7.org/fhir/StructureDefinition/Ladder"
              }
            }
          }
          element {
            type {
              code {
                value: "Chute"
              }
            }
          }"""
        # Ensure repeated types are only returned once.
        """
          element {
            type {
              code {
                value: "Snake"
              }
            }
          }"""
        # Ensure types we don't have definitions for are skipped.
        """
          element {
            type {
              code {
                value: "Missing"
              }
            }
          }"""
        # Ensure recursive types don't recurse forever.
        """
          element {
            type {
              code {
                value: "definition"
              }
            }
          }"""
        # Ensure we don't bother with primitive types.
        """
          element {
            type {
              code {
                value: "http://hl7.org/fhirpath/System.String"
              }
            }
          }
        }
        """,
        definition,
    )

    # A type that isn't referenced by anything.
    lonely = structure_definition_pb2.StructureDefinition()
    text_format.Parse(
        """
      url {
        value: "http://hl7.org/fhir/StructureDefinition/Lonely"
      }
    """,
        lonely,
    )

    # We should recursive find the snake's element as well
    snake = structure_definition_pb2.StructureDefinition()
    text_format.Parse(
        """
      url {
        value: "http://hl7.org/fhir/StructureDefinition/Snake"
      }
      snapshot {
        element {
          type {
            code {
              value: "Hiss"
            }
          }
        }
      }
    """,
        snake,
    )

    # And recurse even further.
    hiss = structure_definition_pb2.StructureDefinition()
    text_format.Parse(
        """
      url {
        value: "http://hl7.org/fhir/StructureDefinition/Hiss"
      }
      snapshot {
        element {
          type {
            code {
              value: "Hisssssss"
            }
          }
        }
      }
    """,
        hiss,
    )

    hisssssss = structure_definition_pb2.StructureDefinition()
    text_format.Parse(
        """
      url {
        value: "http://hl7.org/fhir/StructureDefinition/Hisssssss"
      }
    """,
        hisssssss,
    )

    # Fill out some definitions for other referenced types.
    ladder = structure_definition_pb2.StructureDefinition()
    text_format.Parse(
        """
      url {
        value: "http://hl7.org/fhir/StructureDefinition/Ladder"
      }
    """,
        ladder,
    )

    chute = structure_definition_pb2.StructureDefinition()
    text_format.Parse(
        """
      url {
        value: "http://hl7.org/fhir/StructureDefinition/Chute"
      }
    """,
        chute,
    )

    definitions = {
        sd.url.value: sd
        for sd in (definition, lonely, snake, hiss, hisssssss, ladder, chute)
    }

    package_manager = mock.MagicMock(spec=fhir_package.FhirPackageManager)
    package_manager.get_resource.side_effect = definitions.get

    resolver = value_sets.ValueSetResolver(package_manager, mock.MagicMock())
    result = list(
        resolver._get_structure_defintions_for_elements_of(definition)
    )
    self.assertCountEqual(
        result, [definition, snake, hiss, hisssssss, ladder, chute]
    )


def get_resolver() -> value_sets.ValueSetResolver:
  """Build a ValueSetResolver for the common core package."""
  package_manager = fhir_package.FhirPackageManager()
  package_manager.add_package(r4_package.load_base_r4())

  return value_sets.ValueSetResolver(package_manager, mock.MagicMock())


if __name__ == '__main__':
  absltest.main()

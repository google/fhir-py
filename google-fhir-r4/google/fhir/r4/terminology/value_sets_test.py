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

import unittest.mock

from google.protobuf import text_format
from absl.testing import absltest
from google.fhir.r4.proto.core import datatypes_pb2
from google.fhir.r4.proto.core.resources import code_system_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import r4_package
from google.fhir.r4.terminology import terminology_service_client
from google.fhir.r4.terminology import value_sets


class ValueSetsTest(absltest.TestCase):

  def testValueSetFromUrl_withUsCoreDefinitions_findsValueSet(self):
    value_set = get_resolver().value_set_from_url(
        'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )
    self.assertIsNotNone(value_set)
    self.assertEqual(
        value_set.url.value, 'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )

  def testValueSetFromUrl_withUnknownUrl_raisesError(self):
    self.assertIsNone(
        get_resolver().value_set_from_url(
            'http://hl7.org/fhir/ValueSet/mystery'
        )
    )

  def testValueSetFromUrl_withWrongResourceType_raisesError(self):
    resolver = get_resolver()
    with unittest.mock.patch.object(
        resolver.package_manager, 'get_resource'
    ) as m_get_resource:
      m_get_resource.return_value = (
          structure_definition_pb2.StructureDefinition()
      )
      with self.assertRaises(ValueError):
        resolver.value_set_from_url('http://hl7.org/fhir/ValueSet/mystery')

  def testValueSetFromUrl_withVersionedUrl_findsValueSet(self):
    value_set = get_resolver().value_set_from_url(
        'http://hl7.org/fhir/ValueSet/financial-taskcode|4.0.1'
    )
    self.assertIsNotNone(value_set)
    self.assertEqual(
        value_set.url.value, 'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )

  def testValueSetFromUrl_withBadVersionedUrl_raisesError(self):
    self.assertIsNone(
        get_resolver().value_set_from_url(
            'http://hl7.org/fhir/ValueSet/financial-taskcode|500.0.1'
        )
    )

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

    package_manager = unittest.mock.MagicMock(
        spec=fhir_package.FhirPackageManager
    )
    package_manager.get_resource.side_effect = {magic.url.value: magic}.get

    resolver = value_sets.ValueSetResolver(
        package_manager, unittest.mock.MagicMock()
    )

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
        structure_definitions=[definition, another_definition],
        search_parameters=[],
        code_systems=[],
        value_sets=[value_set, another_value_set, duplicate_value_set],
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
        structure_definitions=[],
        search_parameters=[],
        code_systems=[],
        value_sets=[],
    )
    self.assertEqual(
        list(get_resolver().value_set_urls_from_fhir_package(package)), []
    )

  def testExpandValueSetLocally_withExtensionalSet_expandsCodes(self):
    value_set = value_set_pb2.ValueSet()

    # Add an include set with three codes.
    include_1 = value_set.compose.include.add()
    include_1.version.value = 'include-version-1'
    include_1.system.value = 'include-system-1'

    code_1_1 = include_1.concept.add()
    code_1_1.code.value = 'code-1-1'

    code_1_2 = include_1.concept.add()
    code_1_2.code.value = 'code-1-2'

    code_1_3 = include_1.concept.add()
    code_1_3.code.value = 'code-1-3'

    # Add an include set with one code.
    include_2 = value_set.compose.include.add()
    include_2.version.value = 'include-version-2'
    include_2.system.value = 'include-system-2'

    code_2_1 = include_2.concept.add()
    code_2_1.code.value = 'code-2-1'

    # Add a copy of code_1_3 to the exclude set.
    exclude = value_set.compose.exclude.add()
    exclude.version.value = 'include-version-1'
    exclude.system.value = 'include-system-1'
    exclude_code = exclude.concept.add()
    exclude_code.code.value = 'code-1-3'

    result = value_sets.ValueSetResolver(
        unittest.mock.MagicMock(), unittest.mock.MagicMock()
    )._expand_value_set_locally(value_set)
    expected = [
        value_set_pb2.ValueSet.Expansion.Contains(
            system=datatypes_pb2.Uri(value='include-system-1'),
            version=datatypes_pb2.String(value='include-version-1'),
            code=datatypes_pb2.Code(value='code-1-1'),
        ),
        value_set_pb2.ValueSet.Expansion.Contains(
            system=datatypes_pb2.Uri(value='include-system-1'),
            version=datatypes_pb2.String(value='include-version-1'),
            code=datatypes_pb2.Code(value='code-1-2'),
        ),
        value_set_pb2.ValueSet.Expansion.Contains(
            system=datatypes_pb2.Uri(value='include-system-2'),
            version=datatypes_pb2.String(value='include-version-2'),
            code=datatypes_pb2.Code(value='code-2-1'),
        ),
    ]
    self.assertCountEqual(result.expansion.contains, expected)

  def testExpandValueSetLocally_withFullCodeSystem_expandsCodes(self):
    value_set = value_set_pb2.ValueSet()

    # Add an include for a full code system.
    include_1 = value_set.compose.include.add()
    include_1.version.value = 'version'
    include_1.system.value = 'http://system'

    # Add the definition for the code system.
    code_system = code_system_pb2.CodeSystem()
    code_1 = code_system.concept.add()
    code_1.code.value = 'code-1'
    designation_1 = code_1.designation.add()
    designation_1.value.value = 'doing great'

    code_2 = code_system.concept.add()
    code_2.code.value = 'code-2'

    # Return the definition for the above code system.
    package_manager = unittest.mock.MagicMock()
    package_manager.get_resource.return_value = code_system
    resolver = value_sets.ValueSetResolver(
        package_manager, unittest.mock.MagicMock()
    )

    result = resolver._expand_value_set_locally(value_set)

    package_manager.get_resource.assert_called_once_with('http://system')
    expected = [
        value_set_pb2.ValueSet.Expansion.Contains(
            system=datatypes_pb2.Uri(value='http://system'),
            version=datatypes_pb2.String(value='version'),
            code=datatypes_pb2.Code(value='code-1'),
        ),
        value_set_pb2.ValueSet.Expansion.Contains(
            system=datatypes_pb2.Uri(value='http://system'),
            version=datatypes_pb2.String(value='version'),
            code=datatypes_pb2.Code(value='code-2'),
        ),
    ]
    expected_designation = expected[0].designation.add()
    expected_designation.value.value = 'doing great'

    self.assertCountEqual(result.expansion.contains, expected)

  def testExpandValueSetLocally_withMissingCodeSystem_returnsNone(self):
    value_set = value_set_pb2.ValueSet()

    # Add an include for a full code system.
    include_1 = value_set.compose.include.add()
    include_1.version.value = 'version'
    include_1.system.value = 'http://system'

    # However do not provide a definition for the code system.
    package_manager = unittest.mock.MagicMock()
    package_manager.get_resource.return_value = None
    resolver = value_sets.ValueSetResolver(
        package_manager, unittest.mock.MagicMock()
    )

    result = resolver._expand_value_set_locally(value_set)

    package_manager.get_resource.assert_called_once_with('http://system')
    self.assertIsNone(result)

  def testExpandValueSetLocally_withIntensionalSet_returnsNone(self):
    value_set = value_set_pb2.ValueSet()

    include = value_set.compose.include.add()
    filter_ = include.filter.add()
    filter_.op.value = 1
    filter_.value.value = 'medicine'
    self.assertIsNone(
        value_sets.ValueSetResolver(
            unittest.mock.MagicMock(), unittest.mock.MagicMock()
        )._expand_value_set_locally(value_set)
    )

  def testConceptSetToExpansion_wtihConceptSet_buildsExpansion(self):
    concept_set = value_set_pb2.ValueSet.Compose.ConceptSet()
    concept_set.system.value = 'system'
    concept_set.version.value = 'version'

    code_1 = concept_set.concept.add()
    code_1.code.value = 'code_1'
    code_1.display.value = 'display_1'
    designation_1 = code_1.designation.add()
    designation_1.value.value = 'doing great'

    code_2 = concept_set.concept.add()
    code_2.code.value = 'code_2'

    result = value_sets.ValueSetResolver(
        unittest.mock.MagicMock(), unittest.mock.MagicMock()
    )._concept_set_to_expansion(value_set_pb2.ValueSet(), concept_set)

    expected = [
        value_set_pb2.ValueSet.Expansion.Contains(),
        value_set_pb2.ValueSet.Expansion.Contains(),
    ]
    expected[0].system.value = 'system'
    expected[0].version.value = 'version'
    expected[0].code.value = 'code_1'
    expected[0].display.value = 'display_1'
    expected_designation = expected[0].designation.add()
    expected_designation.value.value = 'doing great'

    expected[1].system.value = 'system'
    expected[1].version.value = 'version'
    expected[1].code.value = 'code_2'

    self.assertCountEqual(result, expected)

  def testConceptSetToExpansion_wtihInvalidCodeSystem_raisesValueError(self):
    # Include an entire code system...
    concept_set = value_set_pb2.ValueSet.Compose.ConceptSet()
    concept_set.system.value = 'http://system'

    # ...but find a non-code system resource for the URL.
    package_manager = unittest.mock.MagicMock()
    package_manager.get_resource.return_value = value_set_pb2.ValueSet()
    resolver = value_sets.ValueSetResolver(
        package_manager, unittest.mock.MagicMock()
    )

    with self.assertRaises(ValueError):
      resolver._concept_set_to_expansion(value_set_pb2.ValueSet(), concept_set)

  def testExpandValueSet_withMissingResource_callsTerminologyService(self):
    mock_package_manager = unittest.mock.MagicMock()
    mock_package_manager.get_resource.return_value = None

    mock_client = unittest.mock.MagicMock(
        spec=terminology_service_client.TerminologyServiceClient
    )

    resolver = value_sets.ValueSetResolver(mock_package_manager, mock_client)

    result = resolver.expand_value_set_url('http://some-url')
    self.assertEqual(
        result, mock_client.expand_value_set_url('http://some-url')
    )

  def testExpandValueSet_withUnExpandableResource_callsTerminologyService(self):
    mock_package_manager = unittest.mock.MagicMock()
    mock_package_manager.get_resource.return_value = value_set_pb2.ValueSet()

    mock_client = unittest.mock.MagicMock(
        spec=terminology_service_client.TerminologyServiceClient
    )

    resolver = value_sets.ValueSetResolver(mock_package_manager, mock_client)
    resolver._expand_value_set_locally = unittest.mock.MagicMock(
        spec=resolver._expand_value_set_locally, return_value=None
    )

    result = resolver.expand_value_set_url('http://some-url')
    self.assertEqual(
        result, mock_client.expand_value_set_url('http://some-url')
    )

  def testExpandValueSet_withExpandableResource_doesNotcallTerminologyService(
      self,
  ):
    mock_package_manager = unittest.mock.MagicMock()
    mock_package_manager.get_resource.return_value = value_set_pb2.ValueSet()

    mock_client = unittest.mock.MagicMock(
        spec=terminology_service_client.TerminologyServiceClient
    )

    resolver = value_sets.ValueSetResolver(mock_package_manager, mock_client)
    resolver._expand_value_set_locally = unittest.mock.MagicMock(
        spec=resolver._expand_value_set_locally
    )

    result = resolver.expand_value_set_url('http://some-url')
    self.assertEqual(
        result, resolver._expand_value_set_locally(value_set_pb2.ValueSet())
    )
    mock_client.expand_value_set_url.assert_not_called()

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

    package_manager = unittest.mock.MagicMock(
        spec=fhir_package.FhirPackageManager
    )
    package_manager.get_resource.side_effect = definitions.get

    resolver = value_sets.ValueSetResolver(
        package_manager, unittest.mock.MagicMock()
    )
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

  return value_sets.ValueSetResolver(package_manager, unittest.mock.MagicMock())


if __name__ == '__main__':
  absltest.main()

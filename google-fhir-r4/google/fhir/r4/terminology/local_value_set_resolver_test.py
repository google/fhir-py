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
"""Test local value set resolver functionality."""
import unittest.mock
from absl.testing import absltest
from google.fhir.r4.proto.core import datatypes_pb2
from google.fhir.r4.proto.core.resources import code_system_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.r4.proto.core.resources import value_set_pb2
from google.fhir.core.utils import fhir_package
from google.fhir.r4 import r4_package
from google.fhir.r4.terminology import local_value_set_resolver


class LocalResolverTest(absltest.TestCase):

  def testExpandValueSetUrl_withExtensionalSet_expandsCodes(self):
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

    result = local_value_set_resolver.LocalResolver(
        unittest.mock.MagicMock(get_resource=lambda _: value_set)
    ).expand_value_set_url(value_set.url.value)
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
    self.assertIsInstance(result, value_set_pb2.ValueSet)
    self.assertCountEqual(result.expansion.contains, expected)

  def testExpandValueSetUrl_withFullCodeSystem_expandsCodes(self):
    value_set = value_set_pb2.ValueSet()

    # Add an include for a full code system.
    include_1 = value_set.compose.include.add()
    include_1.version.value = 'version'
    include_1.system.value = 'http://system'

    # Add the definition for the code system.
    code_system = code_system_pb2.CodeSystem()
    code_system.url.value = 'http://system'

    code_1 = code_system.concept.add()
    code_1.code.value = 'code-1'
    designation_1 = code_1.designation.add()
    designation_1.value.value = 'doing great'

    code_2 = code_system.concept.add()
    code_2.code.value = 'code-2'

    # Add a nested concept.
    code_3 = code_2.concept.add()
    code_3.code.value = 'code-3'

    # Return the definition for the above code system.
    package_manager = unittest.mock.MagicMock()
    package_manager.get_resource.side_effect = {
        code_system.url.value: code_system,
        value_set.url.value: value_set,
    }.get
    resolver = local_value_set_resolver.LocalResolver(package_manager)

    result = resolver.expand_value_set_url(value_set.url.value)

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
        value_set_pb2.ValueSet.Expansion.Contains(
            system=datatypes_pb2.Uri(value='http://system'),
            version=datatypes_pb2.String(value='version'),
            code=datatypes_pb2.Code(value='code-3'),
        ),
    ]
    expected_designation = expected[0].designation.add()
    expected_designation.value.value = 'doing great'

    self.assertIsInstance(result, value_set_pb2.ValueSet)
    self.assertCountEqual(result.expansion.contains, expected)

  def testExpandValueSetUrl_withMissingCodeSystem_returnsNone(self):
    value_set = value_set_pb2.ValueSet()

    # Add an include for a full code system.
    include_1 = value_set.compose.include.add()
    include_1.version.value = 'version'
    include_1.system.value = 'http://system'

    # However do not provide a definition for the code system.
    package_manager = unittest.mock.MagicMock()
    package_manager.get_resource.return_value = None

    resolver = local_value_set_resolver.LocalResolver(package_manager)
    resolver._value_set_from_url = unittest.mock.MagicMock(
        spec=resolver._value_set_from_url, return_value=value_set
    )

    result = resolver.expand_value_set_url(value_set.url.value)

    package_manager.get_resource.assert_called_once_with('http://system')
    self.assertIsNone(result)

  def testExpandValueSetUrl_withIntensionalSet_returnsNone(self):
    value_set = value_set_pb2.ValueSet()

    include = value_set.compose.include.add()
    filter_ = include.filter.add()
    filter_.op.value = 1
    filter_.value.value = 'medicine'
    self.assertIsNone(
        local_value_set_resolver.LocalResolver(
            unittest.mock.MagicMock(get_resource=lambda _: value_set)
        ).expand_value_set_url(value_set.url.value)
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

    result = local_value_set_resolver.LocalResolver(
        unittest.mock.MagicMock()
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
    resolver = local_value_set_resolver.LocalResolver(package_manager)

    with self.assertRaises(ValueError):
      resolver._concept_set_to_expansion(value_set_pb2.ValueSet(), concept_set)

  def testValueSetFromUrl_withUsCoreDefinitions_findsValueSet(self):
    value_set = get_local_resolver()._value_set_from_url(
        'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )
    self.assertIsNotNone(value_set)
    self.assertEqual(
        value_set.url.value, 'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )

  def testValueSetFromUrl_withUnknownUrl_raisesError(self):
    self.assertIsNone(
        get_local_resolver()._value_set_from_url(
            'http://hl7.org/fhir/ValueSet/mystery'
        )
    )

  def testValueSetFromUrl_withWrongResourceType_raisesError(self):
    resolver = get_local_resolver()
    with unittest.mock.patch.object(
        resolver._package_manager, 'get_resource'
    ) as m_get_resource:
      m_get_resource.return_value = (
          structure_definition_pb2.StructureDefinition()
      )
      with self.assertRaises(ValueError):
        resolver._value_set_from_url('http://hl7.org/fhir/ValueSet/mystery')

  def testValueSetFromUrl_withVersionedUrl_findsValueSet(self):
    value_set = get_local_resolver()._value_set_from_url(
        'http://hl7.org/fhir/ValueSet/financial-taskcode|4.0.1'
    )
    self.assertIsNotNone(value_set)
    self.assertEqual(
        value_set.url.value, 'http://hl7.org/fhir/ValueSet/financial-taskcode'
    )

  def testValueSetFromUrl_withBadVersionedUrl_raisesError(self):
    self.assertIsNone(
        get_local_resolver()._value_set_from_url(
            'http://hl7.org/fhir/ValueSet/financial-taskcode|500.0.1'
        )
    )


def get_local_resolver() -> local_value_set_resolver.LocalResolver:
  """Builds a LocalResolver for the common core package."""
  package_manager = fhir_package.FhirPackageManager()
  package_manager.add_package(r4_package.load_base_r4())

  return local_value_set_resolver.LocalResolver(package_manager)


if __name__ == '__main__':
  absltest.main()

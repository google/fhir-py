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
"""Common logic and a base class for testing FHIR views."""

import abc
import textwrap
from absl.testing import absltest
from google.fhir.views import views


class FhirViewsTest(absltest.TestCase, metaclass=abc.ABCMeta):
  """A suite of tests to ensure proper validation for FHIRPath evaluation."""

  @abc.abstractmethod
  def get_views(self) -> views.Views:
    raise NotImplementedError('Subclasses *must* implement get_views.')

  def test_python_keyword_access_for_encounter_succeeds(self):
    """Tests appending underscores to keyword fields succeeds."""
    enc = self.get_views().view_of('Encounter')

    enc_class = (
        enc.select({
            'class': enc.class_,
            'display': enc.class_.display
        }))

    expressions = enc_class.get_select_expressions()
    self.assertLen(expressions, 2)
    self.assertEqual('class', expressions[0].fhir_path)
    self.assertEqual('class.display', expressions[1].fhir_path)

  def test_create_simple_view_for_patient_succeeds(self):
    """Test minimal view definition."""
    pat = self.get_views().view_of('Patient')

    active_patients = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }).where(pat.active))
    self.assertIsNotNone(active_patients)

    expressions = active_patients.get_select_expressions()
    self.assertLen(expressions, 2)
    self.assertEqual('name.given', expressions[0].fhir_path)
    self.assertEqual('birthDate', expressions[1].fhir_path)

  def test_create_simple_view_from_list_for_patient_succeeds(self):
    """Test minimal view definition and select from a list."""
    pat = self.get_views().view_of('Patient')

    active_patients = pat.select(
        [pat.name.given.alias('name'), pat.birthDate.alias('birthDate')]
    ).where(pat.active)
    self.assertIsNotNone(active_patients)

    expressions = active_patients.get_select_expressions()
    self.assertLen(expressions, 2)
    self.assertEqual('name.given', expressions[0].fhir_path)
    self.assertEqual('birthDate', expressions[1].fhir_path)

  def test_invalid_field_without_alias_for_patient_fails(self):
    """Ensures that select field without alias raise an error."""
    pat = self.get_views().view_of('Patient')

    with self.assertRaises(ValueError):
      pat.select([pat.name.given.alias('name'), pat.birthDate])

  def test_view_to_string_for_patient_base_view(self):
    """Test View object __str__ has expected content."""
    pat = self.get_views().view_of('Patient')

    self.assertMultiLineEqual(
        textwrap.dedent("""\
          View<http://hl7.org/fhir/StructureDefinition/Patient.select(
            *
          )>"""),
        str(pat),
    )

  def test_view_to_string_for_patient_has_fields_but_no_constraints(self):
    """Test View object __str__ has expected content."""
    pat = self.get_views().view_of('Patient')

    patient_name_and_birth_date = pat.select(
        {'name_field': pat.name.given, 'birth_date_field': pat.birthDate}
    )
    self.assertMultiLineEqual(
        textwrap.dedent("""\
          View<http://hl7.org/fhir/StructureDefinition/Patient.select(
            name.given.alias(name_field),
            birthDate.alias(birth_date_field)
          )>"""),
        str(patient_name_and_birth_date),
    )

  def test_view_to_string_for_patient_has_fields_and_constraints(self):
    """Test View object __str__ has expected content."""
    pat = self.get_views().view_of('Patient')

    active_patients = (
        pat.select({
            'name_field': pat.name.given,
            'birth_date_field': pat.birthDate
        }).where(pat.active, pat.address.count() < 5))
    self.assertMultiLineEqual(
        textwrap.dedent("""\
          View<http://hl7.org/fhir/StructureDefinition/Patient.select(
            name.given.alias(name_field),
            birthDate.alias(birth_date_field)
          ).where(
            active,
            address.count() < 5
          )>"""),
        str(active_patients),
    )

  def test_invalid_where_predicate_for_patient_fails(self):
    """Ensures that non-boolean where expressions raise an error."""
    pat = self.get_views().view_of('Patient')

    with self.assertRaises(ValueError):
      pat.where(pat.address)

  def test_view_uses_parent_fields_for_patient_succeeds(self):
    """Tests views with selects expose those fields to child views."""
    # Views without select should have base FHIR fields.
    expected_fields_sample = {'address', 'birthDate', 'telecom', 'contact'}
    pat = self.get_views().view_of('Patient')
    self.assertContainsSubset(expected_fields_sample, dir(pat))
    active_pat = pat.where(pat.active)
    self.assertContainsSubset(expected_fields_sample, dir(active_pat))

    # Views with select should have the select fields but not the base
    # fields.
    parent_select = active_pat.select({
        'givenName': active_pat.name.given,
        'birthDate': active_pat.birthDate,
        'firstAddress': active_pat.address.first()
    })
    self.assertContainsSubset({'givenName', 'birthDate', 'firstAddress'},
                              dir(parent_select))
    self.assertNoCommonElements({'address', 'telecom', 'contact'},
                                dir(parent_select))

    # Child view should be able to further select from fields in parent.
    child_select = parent_select.select(
        {'firstState': parent_select.firstAddress.state})
    self.assertContainsSubset({'firstState'}, dir(child_select))
    self.assertNoCommonElements({'givenName', 'birthDate', 'firstAddress'},
                                dir(child_select))
    # Ensure the derived field has the full FHIRPath for runners to use when
    # building it.
    self.assertEqual('address.first().state', child_select.firstState.fhir_path)

  def test_filtered_view_for_patient_succeeds(self):
    """Test adding filter to a view definition."""
    pat = self.get_views().view_of('Patient')

    base_patients = pat.select({
        'name': pat.name.given,
        'derivedActive': pat.active
    })
    expressions = base_patients.get_select_expressions()
    self.assertLen(expressions, 2)
    self.assertEqual('name.given', expressions[0].fhir_path)
    self.assertEqual('active', expressions[1].fhir_path)
    self.assertEmpty(base_patients.get_constraint_expressions())

    # Active patients should have same expressions filtered with an active
    # constraint.
    active_patients = base_patients.where(base_patients.derivedActive)
    expressions = active_patients.get_select_expressions()
    self.assertLen(expressions, 2)
    self.assertEqual('name.given', expressions[0].fhir_path)
    self.assertEqual('active', expressions[1].fhir_path)
    constraint_expressions = list(active_patients.get_constraint_expressions())
    self.assertLen(constraint_expressions, 1)
    self.assertEqual('active', constraint_expressions[0].fhir_path)

  def test_create_value_set_view_for_patient_succeeds(self):
    """Test use of valuesets in a view definition."""
    pat = self.get_views().view_of('Patient')

    married_patients = (
        pat.select({
            'name': pat.name.given,
            'birthDate': pat.birthDate
        }).where(pat.maritalStatus.memberOf('urn:test:married_valueset')))
    constraints = married_patients.get_constraint_expressions()
    self.assertLen(constraints, 1)
    self.assertEqual("maritalStatus.memberOf('urn:test:married_valueset')",
                     constraints[0].fhir_path)

  def test_create_view_with_structure_expression_succeeds(self):
    pat = self.get_views().view_of('Patient')
    address = self.get_views().expression_for('Address')

    patient_zip_codes = pat.select(
        {'zip': pat.address.where(address.use == 'home').postalCode})

    expressions = patient_zip_codes.get_select_expressions()
    self.assertLen(expressions, 1)
    self.assertEqual(
        "address.where(use = 'home').postalCode", expressions[0].fhir_path
    )

  def test_cross_reference_for_patient_and_encounter_succeeds(self):
    """Test generation of views with two resources."""
    enc = self.get_views().view_of('Encounter')
    pat = self.get_views().view_of('Patient')

    enc_and_pat_class = (
        enc.select({
            'class':
                enc.class_,
            'where':
                enc.type.coding.where(enc.type.coding.system == 'url/here'),
            'pat':
                pat.name.given
        }).where(pat.address.exists(), enc.status.exists()))
    self.assertIsNotNone(enc_and_pat_class)

    structdef_urls = enc_and_pat_class.get_structdef_urls()
    self.assertSameElements([
        'http://hl7.org/fhir/StructureDefinition/Encounter',
        'http://hl7.org/fhir/StructureDefinition/Patient'
    ], structdef_urls)

    select_expressions = enc_and_pat_class.get_select_expressions()
    self.assertLen(select_expressions, 3)

    enc_fields = enc_and_pat_class.get_url_to_field_indexes()[
        'http://hl7.org/fhir/StructureDefinition/Encounter'
    ]
    self.assertSameElements([0, 1], enc_fields)

    pat_fields = enc_and_pat_class.get_url_to_field_indexes()[
        'http://hl7.org/fhir/StructureDefinition/Patient'
    ]
    self.assertLen(pat_fields, 1)
    self.assertSameElements([2], pat_fields)

    constraint_expressions = enc_and_pat_class.get_constraint_expressions()
    self.assertLen(constraint_expressions, 2)

    enc_constraints = enc_and_pat_class.get_url_to_constraint_indexes(
    )['http://hl7.org/fhir/StructureDefinition/Encounter']
    self.assertSameElements([1], enc_constraints)
    pat_constraints = enc_and_pat_class.get_url_to_constraint_indexes(
    )['http://hl7.org/fhir/StructureDefinition/Patient']
    self.assertSameElements([0], pat_constraints)

  def test_create_from_view_definition_succeeds(self):
    """Test create view from view definition."""
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'name', 'path': 'name.given'},
            {'alias': 'birthDate', 'path': 'birthDate'},
        ],
    }

    active_patients = self.get_views().from_view_definition(view_definition)
    self.assertIsNotNone(active_patients)

    expressions = active_patients.get_select_expressions()
    self.assertLen(expressions, 2)
    self.assertEqual('name.given', expressions[0].fhir_path)
    self.assertEqual('birthDate', expressions[1].fhir_path)
    self.assertEmpty(active_patients.get_constraint_expressions())

  def test_create_from_view_definition_with_constraints_succeeds(self):
    """Test create view from view definition."""
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'name', 'path': 'name.given'},
            {'alias': 'birthDate', 'path': 'birthDate'},
        ],
        'where': [{'path': 'active'}],
    }

    active_patients = self.get_views().from_view_definition(view_definition)
    self.assertIsNotNone(active_patients)

    constraints = active_patients.get_constraint_expressions()
    self.assertLen(constraints, 1)
    self.assertEqual('active', constraints[0].fhir_path)

  def test_create_from_invalid_where_predicate_view_definition_fails(self):
    """Ensures that non-boolean where expressions raise an error."""
    view_definition = {
        'resource': 'Patient',
        'select': [
            {'alias': 'name', 'path': 'name.given'},
            {'alias': 'birthDate', 'path': 'birthDate'},
        ],
        'where': [{'path': 'address'}],
    }

    with self.assertRaises(ValueError):
      self.get_views().from_view_definition(view_definition)

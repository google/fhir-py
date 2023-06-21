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
"""Module for encapsulating  synthetic resources for FHIRPath traversal."""

from typing import List, Optional

from google.fhir.r4.proto.core import codes_pb2
from google.fhir.r4.proto.core import datatypes_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _structure_definitions as sdefs
from google.fhir.core.fhir_path import context as context_lib
from google.fhir.core.fhir_path import expressions
from google.fhir.core.fhir_path import fhir_path
from google.fhir.r4 import primitive_handler


class FhirPathTestBase:
  """This suite stands up a list of synthetic resources for FHIRPath traversal.

  The resources have the following structure:
  ```
  string {}

  Foo {
    Bar bar;
    Bats bat;
    Reference reference;

    InlineElement {
      string value;
      repeated int numbers;
    }
    InlineElement inline;

    CodeFlavor codeFlavor;
    repeated CodeFlavor codeFlavors;
    repeated bool boolList;
    repeated string stringList;
    repeated Coding codingList;

    ChoiceType choiceExample['string', 'integer', 'CodeableConcept']
    repeated ChoiceType multipleChoiceExample['string', 'integer',
      'CodeableConcept']
  }
  Bar {
    repeated Bats bats;
  }
  Bats {
    Struct struct;
  }
  Struct {
    string value;
    string anotherValue;
    AnotherStruct anotherStruct;
  }
  AnotherStruct {
    string anotherValue
  }
  Div {
    string text;
  }
  CodeFlavor {
    string code;
    Coding coding;
    CodeableConcept codeableConcept;
  }
  Coding {
    string system;
    string code;
  }
  CodeableConcept {
    repeated Coding coding
  }
  ```

  Note that the type, `InlineElement`, is derived from the core resource:
  `http://hl7.fhir/org/StructureDefinition/BackboneElement`.

  Class Attributes:
    foo: A reference to the "Foo" `StructureDefinition`.
    foo_root: A reference to the "Foo" `ElementDefinition`.
    fhir_path_encoder: A reference to a `fhir_path.FhirPathEncoder` that has
      been initialized with the resource graph noted above.
  """

  # string datatype
  string_datatype = sdefs.build_resource_definition(
      id_='string',
      element_definitions=[
          sdefs.build_element_definition(
              id_='string',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='1'),
          )
      ],
  )
  # integer datatype
  integer_datatype = sdefs.build_resource_definition(
      id_='integer',
      element_definitions=[
          sdefs.build_element_definition(
              id_='integer',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='1'),
          )
      ],
  )
  # reference datatype
  reference_datatype = sdefs.build_resource_definition(
      id_='Reference',
      element_definitions=[
          sdefs.build_element_definition(
              id_='Reference',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='1'),
          ),
          sdefs.build_element_definition(
              id_='Reference.reference',
              type_codes=['string'],
              cardinality=sdefs.Cardinality(min=0, max='1'),
          ),
      ],
  )

  patient_datatype = sdefs.build_resource_definition(
      id_='Patient',
      element_definitions=[
          sdefs.build_element_definition(
              id_='Patient',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='1'),
          )
      ],
  )

  device_datatype = sdefs.build_resource_definition(
      id_='Device',
      element_definitions=[
          sdefs.build_element_definition(
              id_='Device',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='1'),
          )
      ],
  )

  observation_datatype = sdefs.build_resource_definition(
      id_='Observation',
      element_definitions=[
          sdefs.build_element_definition(
              id_='Observation',
              type_codes=None,
              cardinality=sdefs.Cardinality(min=0, max='1'),
          )
      ],
  )

  # Foo resource
  foo_root_element_definition = sdefs.build_element_definition(
      id_='Foo', type_codes=None, cardinality=sdefs.Cardinality(min=0, max='1')
  )
  bar_element_definition = sdefs.build_element_definition(
      id_='Foo.bar',
      type_codes=['Bar'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )

  reference_element_definition = sdefs.build_element_definition(
      id_='Foo.ref',
      type_codes=['Reference'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  reference_element_definition.type[0].target_profile.add(
      value='http://hl7.org/fhir/StructureDefinition/Patient'
  )
  reference_element_definition.type[0].target_profile.add(
      value='http://hl7.org/fhir/StructureDefinition/Device'
  )
  reference_element_definition.type[0].target_profile.add(
      value='http://hl7.org/fhir/StructureDefinition/Observation'
  )

  bat_element_definition = sdefs.build_element_definition(
      id_='Foo.bat',
      type_codes=['Bats'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  inline_element_definition = sdefs.build_element_definition(
      id_='Foo.inline',
      type_codes=['BackboneElement'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  inline_value_element_definition = sdefs.build_element_definition(
      id_='Foo.inline.value',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  inline_numbers_element_definition = sdefs.build_element_definition(
      id_='Foo.inline.numbers',
      type_codes=['integer'],
      cardinality=sdefs.Cardinality(min=0, max='*'),
  )
  choice_value_element_definition = sdefs.build_element_definition(
      id_='Foo.choiceExample[x]',
      type_codes=['string', 'integer', 'CodeableConcept'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  multiple_choice_value_element_definition = sdefs.build_element_definition(
      id_='Foo.multipleChoiceExample[x]',
      type_codes=['string', 'integer', 'CodeableConcept'],
      cardinality=sdefs.Cardinality(min=0, max='*'),
  )
  date_value_element_definition = sdefs.build_element_definition(
      id_='Foo.dateField',
      type_codes=['date'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  code_flavor_element_definition = sdefs.build_element_definition(
      id_='Foo.codeFlavor',
      type_codes=['CodeFlavor'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  code_flavors_element_definition = sdefs.build_element_definition(
      id_='Foo.codeFlavors',
      type_codes=['CodeFlavor'],
      cardinality=sdefs.Cardinality(min=0, max='*'),
  )
  bool_list_definition = sdefs.build_element_definition(
      id_='Foo.boolList',
      type_codes=['boolean'],
      cardinality=sdefs.Cardinality(min=0, max='*'),
  )
  string_list_definition = sdefs.build_element_definition(
      id_='Foo.stringList',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='*'),
  )
  coding_list_definition = sdefs.build_element_definition(
      id_='Foo.codingList',
      type_codes=['Coding'],
      cardinality=sdefs.Cardinality(min=0, max='*'),
  )
  foo = sdefs.build_resource_definition(
      id_='Foo',
      element_definitions=[
          foo_root_element_definition,
          bar_element_definition,
          bat_element_definition,
          reference_element_definition,
          inline_element_definition,
          inline_value_element_definition,
          inline_numbers_element_definition,
          choice_value_element_definition,
          multiple_choice_value_element_definition,
          date_value_element_definition,
          code_flavor_element_definition,
          code_flavors_element_definition,
          bool_list_definition,
          string_list_definition,
          coding_list_definition,
      ],
  )

  # Bar resource
  bar_root_element_definition = sdefs.build_element_definition(
      id_='Bar', type_codes=None, cardinality=sdefs.Cardinality(min=0, max='1')
  )
  bats_element_definition = sdefs.build_element_definition(
      id_='Bar.bats',
      type_codes=['Bats'],
      cardinality=sdefs.Cardinality(min=0, max='*'),
  )
  bar = sdefs.build_resource_definition(
      id_='Bar',
      element_definitions=[
          bar_root_element_definition,
          bats_element_definition,
      ],
  )

  # Bats resource
  bats_root_element_definition = sdefs.build_element_definition(
      id_='Bats', type_codes=None, cardinality=sdefs.Cardinality(min=0, max='1')
  )
  struct_element_definition = sdefs.build_element_definition(
      id_='Bats.struct',
      type_codes=['Struct'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  div_element_definition = sdefs.build_element_definition(
      id_='Bats.div',
      type_codes=['Div'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  bats = sdefs.build_resource_definition(
      id_='Bats',
      element_definitions=[
          bats_root_element_definition,
          struct_element_definition,
          div_element_definition,
      ],
  )

  # Struct resource; Standard SQL keyword
  struct_root_element_definition = sdefs.build_element_definition(
      id_='Struct',
      type_codes=None,
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  value_element_definition = sdefs.build_element_definition(
      id_='Struct.value',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  another_value_element_definition = sdefs.build_element_definition(
      id_='Struct.anotherValue',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  another_struct_element_definition = sdefs.build_element_definition(
      id_='Struct.anotherStruct',
      type_codes=['AnotherStruct'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  struct = sdefs.build_resource_definition(
      id_='Struct',
      element_definitions=[
          struct_root_element_definition,
          value_element_definition,
          another_value_element_definition,
          another_struct_element_definition,
      ],
  )

  # AnotherStruct resource
  another_struct_root_element_definition = sdefs.build_element_definition(
      id_='AnotherStruct',
      type_codes=None,
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  another_value_element_definition = sdefs.build_element_definition(
      id_='AnotherStruct.anotherValue',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  another_struct = sdefs.build_resource_definition(
      id_='AnotherStruct',
      element_definitions=[
          another_struct_root_element_definition,
          another_value_element_definition,
      ],
  )

  # Div resource; FHIRPath keyword
  div_root_element_definition = sdefs.build_element_definition(
      id_='Div', type_codes=None, cardinality=sdefs.Cardinality(min=1, max='1')
  )
  text_element_definition = sdefs.build_element_definition(
      id_='Div.text',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  div = sdefs.build_resource_definition(
      id_='Div',
      element_definitions=[
          div_root_element_definition,
          text_element_definition,
      ],
  )

  # CodeFlavor resource
  code_flavor_root_element_definition = sdefs.build_element_definition(
      id_='CodeFlavor',
      type_codes=None,
      cardinality=sdefs.Cardinality(min=1, max='1'),
  )
  code_element_definition = sdefs.build_element_definition(
      id_='CodeFlavor.code',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  coding_element_definition = sdefs.build_element_definition(
      id_='CodeFlavor.coding',
      type_codes=['Coding'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  codeable_concept_element_definition = sdefs.build_element_definition(
      id_='CodeFlavor.codeableConcept',
      type_codes=['CodeableConcept'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  code_flavor = sdefs.build_resource_definition(
      id_='CodeFlavor',
      element_definitions=[
          code_flavor_root_element_definition,
          code_element_definition,
          coding_element_definition,
          codeable_concept_element_definition,
      ],
  )

  # Coding resource
  coding_root_element_definition = sdefs.build_element_definition(
      id_='Coding',
      type_codes=None,
      cardinality=sdefs.Cardinality(min=1, max='1'),
  )
  coding_system_element_definition = sdefs.build_element_definition(
      id_='Coding.system',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  coding_code_element_definition = sdefs.build_element_definition(
      id_='Coding.code',
      type_codes=['string'],
      cardinality=sdefs.Cardinality(min=0, max='1'),
  )
  coding = sdefs.build_resource_definition(
      id_='Coding',
      element_definitions=[
          coding_root_element_definition,
          coding_system_element_definition,
          coding_code_element_definition,
      ],
  )

  # CodeableConcept resource
  codeable_concept_root_element_definition = sdefs.build_element_definition(
      id_='CodeableConcept',
      type_codes=None,
      cardinality=sdefs.Cardinality(min=1, max='1'),
  )
  codeable_concept_coding_system_element_definition = (
      sdefs.build_element_definition(
          id_='CodeableConcept.coding',
          type_codes=['Coding'],
          cardinality=sdefs.Cardinality(min=0, max='*'),
      )
  )
  codeable_concept = sdefs.build_resource_definition(
      id_='CodeableConcept',
      element_definitions=[
          codeable_concept_root_element_definition,
          codeable_concept_coding_system_element_definition,
      ],
  )

  # Set resources for test
  resources = [
      integer_datatype,
      string_datatype,
      reference_datatype,
      patient_datatype,
      device_datatype,
      observation_datatype,
      foo,
      bar,
      bats,
      struct,
      another_struct,
      div,
      code_flavor,
      coding,
      codeable_concept,
  ]

  context = context_lib.MockFhirPathContext(resources)

  foo = foo
  foo_root = foo_root_element_definition
  struct_element_def = struct_element_definition
  fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder(resources)

  div = div
  div_root = div_root_element_definition

  def create_builder_from_str(
      self,
      structdef_name: str,
      fhir_path_expression: str,
      fhir_context: Optional[context_lib.FhirPathContext] = None,
  ) -> expressions.Builder:
    """Creates an expression Builder from a FHIRPath string.

    Args:
      structdef_name: Name of the resource for the fhir_path_expression.
        Structure Definition is fetched from the fhir_context.
      fhir_path_expression: A FHIR path string to parse.
      fhir_context: An optional context to use. If not specified, will use
        self.context by default.

    Returns:
      An equivalent expressions.Builder to the fhir_path_expression.
    """
    structdef_type = None
    if not fhir_context:
      fhir_context = self.context

    structdef = fhir_context.get_structure_definition(structdef_name)
    if not structdef:
      raise ValueError(
          f'Structdef {structdef_name} was not found in the provided context.'
      )

    structdef_type = _fhir_path_data_types.StructureDataType.from_proto(
        structdef
    )

    return expressions.from_fhir_path_expression(
        fhir_path_expression,
        fhir_context,
        structdef_type,
        primitive_handler.PrimitiveHandler(),
    )

  @classmethod
  def build_constraint(
      cls,
      fhir_path_expression: str,
      key: str = 'key-1',
      severity: codes_pb2.ConstraintSeverityCode.Value = codes_pb2.ConstraintSeverityCode.ERROR,
  ) -> datatypes_pb2.ElementDefinition.Constraint:
    """Returns an `ElementDefinition.Constraint` for a FHIRPath expression.

    Args:
      fhir_path_expression: The raw FHIRPath expression.
      key: The FHIRPath constraint unique identifier. Defaults to 'key-1'.
      severity: The constraint severity.  Defaults to ERROR.

    Returns:
      An instance of `ElementDefinition.Constraint` capturing the raw underlying
      `fhir_path_expression`.
    """
    return datatypes_pb2.ElementDefinition.Constraint(
        key=datatypes_pb2.Id(value=key),
        expression=datatypes_pb2.String(value=fhir_path_expression),
        severity=datatypes_pb2.ElementDefinition.Constraint.SeverityCode(
            value=severity
        ),
    )

  @classmethod
  def build_profile(
      cls,
      id_: str,
      element_definitions: List[datatypes_pb2.ElementDefinition],
  ) -> structure_definition_pb2.StructureDefinition:
    """Returns a FHIR profile when given a list of `ElementDefinition`s.

    The URL, name, type, and base definition are all derived from the provided
    `id_`. The URL is relative to 'http://g.co/fhir/StructureDefinition/', and
    the
    base definition is relative to 'http://hl7.org/fhir/StructureDefinition/'.

    Args:
      id_: The logical ID of the resource, as used in the URL for the resource.
      element_definitions: The list of `ElementDefinition`s comprising the
        profile's `snapshot`.

    Returns:
      A `StructureDefinition` capturing the profile specified.
    """
    return sdefs.build_structure_definition(
        url=f'http://g.co/fhir/StructureDefinition/{id_}',
        name=id_,
        type_=id_,
        base_definition=f'http://hl7.org/fhir/StructureDefinition/{id_}',
        element_definitions=element_definitions,
        derivation_code=codes_pb2.TypeDerivationRuleCode.CONSTRAINT,
    )

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
"""Utilities for constructing FHIR data types for unit testing."""

import dataclasses
import os
from typing import List, Optional

# TODO(b/229908551): Remove use of R4 in core test helpers.
from google.fhir.r4.proto.core import codes_pb2
from google.fhir.r4.proto.core import datatypes_pb2
from google.fhir.r4.proto.core.resources import structure_definition_pb2


@dataclasses.dataclass
class Cardinality:
  """ElementDefinition field cardinality.

  Attributes:
    min: The minimum cardinality.
    max: The maximum cardinality. A number or *.
  """

  min: int
  max: str


def build_element_definition(
    *,
    id_: str,
    type_codes: Optional[List[str]],
    cardinality: Cardinality,
    path: Optional[str] = None,
    base_path: Optional[str] = None,
    constraints: Optional[
        List[datatypes_pb2.ElementDefinition.Constraint]
    ] = None,
    content_reference: Optional[str] = None,
    profiles: Optional[List[str]] = None,
    slice_name: Optional[str] = None,
    fixed: Optional[datatypes_pb2.ElementDefinition.FixedX] = None,
) -> datatypes_pb2.ElementDefinition:
  """Returns an `ElementDefinition` for testing FHIRPath encoding.

  The path is derived from the provided `id_`.

  Args:
    id_: The dot-separated ('.') unique identifier for inter-element
      referencing.
    type_codes: An optional list of relative URIs of the `ElementDefinition`.
    cardinality: The cardinality of the `ElementDefinition`. This corresponds to
      setting values for `min`/`max`.
    path: The dot-separated ('.') label denoting where in the FHIR resource
      hierarchy an element is located. If `None`, the returned
      `ElementDefinition`'s `path` will be set to `id_`. Defaults to `None`.
    base_path: An optional string denoting the `ElementDefinition.base.path`. If
      `None`, the returned `ElementDefinition`'s `base.path` will be set to
      `id_`. Defaults to `None`.
    constraints: An optional list of `ElementDefinition.Constraint`
    content_reference: An optional FHIR content reference for the element.
    profiles: An optional list of profile URLs of the `ElementDefinition`.
    slice_name: An optional value for the slice_name attribute.
    fixed: An optional Fixed message for the fixed attribute.

  Returns:
    An `ElementDefintion` for use as part of a snapshot composition within a
    `StructureDefinition`.
  """
  path = id_ if path is None else path
  base_path = id_ if base_path is None else base_path
  type_codes = [] if type_codes is None else type_codes
  profiles = (
      []
      if profiles is None
      else [datatypes_pb2.Canonical(value=profile) for profile in profiles]
  )
  type_ = [
      datatypes_pb2.ElementDefinition.TypeRef(
          code=datatypes_pb2.Uri(value=code_value), profile=profiles
      )
      for code_value in type_codes
  ]
  kwargs = {
      'id': datatypes_pb2.String(value=id_),
      'path': datatypes_pb2.String(value=path),
      'constraint': constraints,
      'min': datatypes_pb2.UnsignedInt(value=cardinality.min),
      'max': datatypes_pb2.String(value=cardinality.max),
      'base': datatypes_pb2.ElementDefinition.Base(
          path=datatypes_pb2.String(value=base_path)
      ),
      'content_reference': datatypes_pb2.Uri(value=content_reference),
      'type': type_,
  }
  if slice_name is not None:
    kwargs['slice_name'] = datatypes_pb2.String(value=slice_name)
  if fixed is not None:
    kwargs['fixed'] = fixed

  return datatypes_pb2.ElementDefinition(**kwargs)


def build_structure_definition(
    *,
    url: str,
    name: str,
    base_definition: str,
    element_definitions: List[datatypes_pb2.ElementDefinition],
    type_: str,
    derivation_code: codes_pb2.TypeDerivationRuleCode.Value,
) -> structure_definition_pb2.StructureDefinition:
  """Returns a `StructureDefinition` of a specialization or constraint."""
  return structure_definition_pb2.StructureDefinition(
      id=datatypes_pb2.Id(value=os.path.basename(url)),
      url=datatypes_pb2.Uri(value=url),
      kind=structure_definition_pb2.StructureDefinition.KindCode(
          value=codes_pb2.StructureDefinitionKindCode.RESOURCE
      ),
      fhir_version=structure_definition_pb2.StructureDefinition.FhirVersionCode(
          value=codes_pb2.FHIRVersionCode.V_4_0_1
      ),
      type=datatypes_pb2.Uri(value=type_),
      derivation=structure_definition_pb2.StructureDefinition.DerivationCode(
          value=derivation_code
      ),
      name=datatypes_pb2.String(value=name),
      abstract=datatypes_pb2.Boolean(value=False),
      base_definition=datatypes_pb2.Canonical(value=base_definition),
      snapshot=structure_definition_pb2.StructureDefinition.Snapshot(
          element=element_definitions
      ),
      status=structure_definition_pb2.StructureDefinition.StatusCode(
          value=codes_pb2.PublicationStatusCode.ACTIVE
      ),
  )


def build_resource_definition(
    *, id_: str, element_definitions: List[datatypes_pb2.ElementDefinition]
) -> structure_definition_pb2.StructureDefinition:
  """Returns a `StructureDefinition` depicting a FHIR resource.

  The URL, name, and type are derived from the provided `id_`.

  The returned `StructureDefinition` has a `base_definition` set to:
  'http://hl7.org/fhir/StructureDefinition/Resource'. In other words, all
  constructed types for test derive from `Resource`.

  Args:
    id_: The logical ID of the resource, as used in the URL for the resource.
    element_definitions: The list of `ElementDefinition`s comprising the
      resource's `snapshot`.

  Returns:
    A `StructureDefinition` capturing the specified resource.
  """
  return build_structure_definition(
      url=f'http://hl7.org/fhir/StructureDefinition/{id_}',
      name=id_,
      type_=id_,
      # TODO(b/244184211): May not be necessary to specify for test fixture
      # purposes. This could instead perhaps be an abstract type? Otherwise, we
      # should technically read-in the `Resource` structure definition to ensure
      # a complete graph.
      base_definition='http://hl7.org/fhir/StructureDefinition/Resource',
      element_definitions=element_definitions,
      derivation_code=codes_pb2.TypeDerivationRuleCode.SPECIALIZATION,
  )

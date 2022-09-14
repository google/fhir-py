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
"""Support for creating views of FHIR data from underlying data sources.

This module is focused on defining views that can then be realized via a runner.
See the View and Views classes below for details on use, and runner
implementations (like the BigQuery runner) for realizing the views themselves.
"""

import keyword
from typing import Any, cast, Dict, Optional, Tuple

import immutabledict

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.r4 import primitive_handler


class View:
  """Defines a view of a collection of FHIR resources of a specific type.

  Views are defined by two key elements:
   * One or more selected fields defined by FHIRPath expressions. This
     is analogous to a SELECT clause in SQL.
   * Zero or more constraints to filter the FHIR resources, also defined by
     FHIRPath expressions. This is analogous to a WHERE clause in SQL.

  A simple example of defining a view can be seen here:

  >>> active_patients = (
  >>>   pat.select({
  >>>       'name': pat.name.given,
  >>>       'birthDate': pat.birthDate
  >>>   }).where(pat.active))

  Users will define these in this class, and use a runner to apply the view
  logic to an underlying datasource, like FHIR data stored in BigQuery.
  """

  def __init__(self, structdef_url: str, root_builder: expressions.Builder,
               fhir_context: context.FhirPathContext,
               fields: immutabledict.immutabledict[str, expressions.Builder],
               constraints: Tuple[expressions.Builder, ...]) -> None:
    self._structdef_url = structdef_url
    self._root_builder = root_builder
    self._context = fhir_context
    self._fields = fields
    self._constraints = constraints

  def select(self, fields: Dict[str, expressions.Builder]) -> 'View':
    """Returns a View instance that selects the given fields."""
    # TODO: select statements should build on current fields.
    return View(self._structdef_url, self._root_builder, self._context,
                immutabledict.immutabledict(fields), self._constraints)

  def where(self, *constraints: expressions.Builder) -> 'View':
    """Returns a new View instance with these added constraints.

    Args:
      *constraints: a list of FHIRPath expressions to conjuctively constrain the
        underlying data.  The returned view will apply the both the current and
        additional constraints defined here.
    """
    return View(self._structdef_url, self._root_builder, self._context,
                self._fields, self._constraints + tuple(constraints))

  def __getattr__(self, name: str) -> expressions.Builder:
    """Used to support building expressions directly off of the base view.

    See the class-level documentation for guidance on use.

    Args:
      name: the name of the FHIR field to start with in the builder.

    Returns:
      A FHIRPath builder for the field in question
    """
    # If the name is a python keyword, then there will be an extra underscore
    # appended to the name.
    lookup = name[:-1] if name.endswith('_') and keyword.iskeyword(
        name[:-1]) else name
    if self._fields:
      # View has defined fields, so use them as the base builder expressions.
      expression = self._fields.get(lookup)
    else:
      # View is usuing the root resource, so look up fields from that
      # structure
      expression = getattr(self._root_builder, lookup)

    if expression is None:
      raise AttributeError(f'No such field {name}')
    return expression

  def __dir__(self):
    if self._fields:
      fields = list(self._fields.keys())
    else:
      fields = self._root_builder.fhir_path_fields()
    fields.extend(dir(type(self)))
    return fields

  def get_structdef_url(self) -> str:
    """Returns the URL of the structure definition for the resource."""
    return self._structdef_url

  def get_patient_id_expression(self) -> Optional[expressions.Builder]:
    """Returns the patient id of the root builder of the view."""
    structdef = self._context.get_structure_definition(self._structdef_url)
    if cast(Any, structdef).id.value == 'Patient':
      return self._root_builder.id
    else:
      patients = _utils.get_patient_reference_element_paths(structdef)
      if not patients:
        return None

      # If there is a 'subject' field that has a patient reference, use it per
      # FHIR conventions, otherwise use the first reference to patient in the
      # resource. If there are exceptions, they can be hardcoded as necessary.
      if 'subject' in patients:
        patient_ref = 'subject'
      else:
        patient_ref = patients[0]

    return self._root_builder.__getattr__(patient_ref).idFor('patient')

  def get_select_expressions(
      self) -> immutabledict.immutabledict[str, expressions.Builder]:
    """Returns the fields used in the view and their corresponding expressions.

    Returns:
      An immutable dictionary of selected field names and the expression
      used to populate them.
    """
    return self._fields

  def get_constraint_expressions(self) -> Tuple[expressions.Builder, ...]:
    """Returns the constraints used to define the view.

    Returns:
      A homogeneous tuple of FHIRPath expressions used to constrain the view.
    """
    return self._constraints

  def get_fhir_path_context(self) -> context.FhirPathContext:
    return self._context


class Views:
  """Helper class for creating FHIR views based on some resource definition."""

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler) -> None:
    self._context = fhir_context
    self._handler = handler

  def view_of(self, structdef_url: str) -> View:
    """Returns a view of the FHIR resource identified by the given string."""
    structdef = self._context.get_structure_definition(structdef_url)
    struct_type = _fhir_path_data_types.StructureDataType(structdef)
    builder = expressions.Builder(
        _evaluation.RootMessageNode(self._context, struct_type), self._context,
        self._handler)
    return View(structdef_url, builder, self._context,
                immutabledict.immutabledict(), ())

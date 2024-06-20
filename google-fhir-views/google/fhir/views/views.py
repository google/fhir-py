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
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import context
from google.fhir.r4 import primitive_handler
from google.fhir.views import _view_config
from google.fhir.views import column_expression_builder


class View:
  """Defines a view of a collection of FHIR resources of a specific type.

  Views are defined by two key elements:
   * One or more selected fields defined by FHIRPath expressions. This
     is analogous to a SELECT clause in SQL.
   * Zero or more constraints to filter the FHIR resources, also defined by
     FHIRPath expressions. This is analogous to a WHERE clause in SQL.

  A simple example of defining a view can be seen here:

  >>> active_patients = (
  >>>   pat.select([
  >>>       pat.name.given.named('name'),
  >>>       pat.birthDate.named('birthDate')
  >>>   }]).where(pat.active))

  Users will define these in this class, and use a runner to apply the view
  logic to an underlying datasource, like FHIR data stored in BigQuery.
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      root_resource: column_expression_builder.ColumnExpressionBuilder,
      fields: Tuple[column_expression_builder.ColumnExpressionBuilder, ...],
      constraints: Tuple[
          column_expression_builder.ColumnExpressionBuilder,
          ...,
      ],
      handler: primitive_handler.PrimitiveHandler,
  ) -> None:
    # In practice, the fhir_context should always include all of the structure
    # defs that anyone would ever use, but in theory, there could be contexts
    # for different views that don't share the same subset of structure defs.
    self._context = fhir_context
    self._root_resource = root_resource
    self._structdef_url = root_resource.return_type.url
    self._handler = handler
    self._has_unnest_or_sub_select = False

    for field in fields:
      if not field.column_name and not field.children:
        raise ValueError(
            'View `select` expressions must either have column names or'
            f' children. Got `{field}`'
        )
      if field.node.get_root_node().return_type.url != self._structdef_url:
        raise ValueError(
            'View `select` expressions must have the same root resource as '
            f'the view itself. Got `{field}`'
        )
      if field.needs_unnest or field.children:
        self._has_unnest_or_sub_select = True
    self._fields = fields

    for constraint in constraints:
      if constraint.node.get_root_node().return_type.url != self._structdef_url:
        raise ValueError(
            'View `where` expressions must have the same root resource as '
            f'the view itself. Got `{constraint}`'
        )
    self._constraints = constraints

  def select(
      self,
      fields: Union[
          Dict[
              str,
              column_expression_builder.ColumnExpressionBuilder,
          ],
          List[column_expression_builder.ColumnExpressionBuilder],
      ],
  ) -> 'View':
    """Returns a View instance that selects the given fields."""
    # TODO(b/244184211): select statements should build on current fields.

    if isinstance(fields, dict):
      fields_tuple = tuple(
          field if field.column_name else field.named(name)
          for name, field in fields.items()
      )
    else:
      fields_tuple = tuple(fields)

    return View(
        self._context,
        self._root_resource,
        fields_tuple,
        self._constraints,
        self._handler,
    )

  def where(
      self,
      *constraints: column_expression_builder.ColumnExpressionBuilder,
  ) -> 'View':
    """Returns a new View instance with these added constraints.

    Args:
      *constraints: a list of FHIRPath expressions to conjuctively constrain the
        underlying data.  The returned view will apply the both the current and
        additional constraints defined here.
    """
    for constraint in constraints:
      if constraint.node.return_type != _fhir_path_data_types.Boolean:
        raise ValueError((
            'view `where` expressions must be boolean predicates',
            f' got `{constraint.node.to_fhir_path()}`',
        ))

    return View(
        self._context,
        self._root_resource,
        self._fields,
        self._constraints + tuple(constraints),
        self._handler,
    )

  def __getattr__(
      self, name: str
  ) -> column_expression_builder.ColumnExpressionBuilder:
    """Used to support building expressions directly off of the base view.

    See the class-level documentation for guidance on use.

    Args:
      name: the name of the FHIR field to start with in the builder.

    Returns:
      A ColumnExpressionBuilder for the field in question
    """
    # If the name is a python keyword, then there will be an extra underscore
    # appended to the name.
    lookup = name[:-1] if name.endswith('_') and keyword.iskeyword(
        name[:-1]) else name

    expression = None
    if self._fields:
      for field in self._fields:
        # View has defined fields, so use them as the base builder expressions.
        if field.column_name == lookup:
          expression = field.builder
    else:
      # View is using the root resource, so look up fields from that
      # structure
      expression = getattr(self._root_resource.builder, lookup)

    if expression is None:
      raise AttributeError(f'No such field {name}')
    return column_expression_builder.ColumnExpressionBuilder.from_fhir_path_builder(
        expression
    )

  def __dir__(self) -> List[str]:
    if self._fields:
      fields: List[str] = [builder.column_name for builder in self._fields]  # pytype: disable=annotation-type-mismatch
    else:
      fields = self._root_resource.fhir_path_fields()

    fields.extend(dir(type(self)))
    return fields

  def get_patient_id_expression(
      self,
  ) -> Optional[column_expression_builder.ColumnExpressionBuilder]:
    """Returns the builder for patient ids of the root builder of the view."""
    if self._root_resource.fhir_path == 'Patient':
      return self._root_resource.id

    structdef = self._context.get_structure_definition(self._structdef_url)
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

    return self._root_resource.__getattr__(patient_ref).getReferenceKey(
        'patient'
    )

  def get_select_expressions(
      self,
  ) -> Tuple[column_expression_builder.ColumnExpressionBuilder, ...]:
    """Returns the fields used in the view and their corresponding expressions.

    Returns:
      An immutable dictionary of selected field names and the expression
      used to populate them.
    """
    return self._fields

  def get_constraint_expressions(
      self,
  ) -> Tuple[column_expression_builder.ColumnExpressionBuilder, ...]:
    """Returns the constraints used to define the view.

    Returns:
      A homogeneous tuple of FHIRPath expressions used to constrain the view.
    """
    return self._constraints

  def _get_select_columns_to_return_type(
      self,
      builders: Tuple[column_expression_builder.ColumnExpressionBuilder, ...],
  ) -> Mapping[str, _fhir_path_data_types.FhirPathDataType]:
    """Returns a mapping from column name to return type."""
    columns = {}
    for builder in builders:
      if builder.column_name:
        return_type = builder.return_type
        if builder.needs_unnest:
          return_type = builder.return_type.with_cardinality(
              _fhir_path_data_types.Cardinality.SCALAR
          )
        columns[builder.column_name] = return_type
      else:
        columns.update(
            self._get_select_columns_to_return_type(tuple(builder.children))
        )
    return columns

  def get_select_columns_to_return_type(
      self,
  ) -> Mapping[str, _fhir_path_data_types.FhirPathDataType]:
    return self._get_select_columns_to_return_type(self._fields)

  def get_structdef_url(self) -> str:
    return self._structdef_url

  def get_fhir_path_context(self) -> context.FhirPathContext:
    return self._context

  def has_unnest_or_sub_select(self) -> bool:
    return self._has_unnest_or_sub_select

  def __str__(self) -> str:
    view_strings = [f'View<{self._structdef_url}']

    select_strings = []
    for builder in self.get_select_expressions():
      select_strings.append(builder._to_string(builder, 1))
    if not select_strings:
      select_strings.append('  *')
    view_strings.append(
        '.select(\n{selects}\n)'.format(selects=',\n'.join(select_strings))
    )

    where_strings = []
    for builder in self.get_constraint_expressions():
      where_strings.append(f'  {builder.fhir_path}')
    if where_strings:
      view_strings.append(
          '.where(\n{constraints}\n)'.format(
              constraints=',\n'.join(where_strings)
          )
      )

    view_strings.append('>')

    return ''.join(view_strings)

  def __repr__(self) -> str:
    return str(self)


class Views:
  """Helper class for creating FHIR views based on some resource definition."""

  def __init__(self, fhir_context: context.FhirPathContext,
               handler: primitive_handler.PrimitiveHandler) -> None:
    self._context = fhir_context
    self._handler = handler

  def view_of(self, structdef_url: str) -> View:
    """Returns a view of the FHIR resource identified by the given string.

    Args:
      structdef_url: URL of the FHIR resource to load. Per the FHIR spec, an
        unqualified URL will be considered to be relative to
        'http://hl7.org/fhir/StructureDefinition/', so for core datatypes or
        resources callers can simply pass in 'Patient' or 'HumanName', for
        example.

    Returns:
      A FHIR View builder for the given structure, typically a FHIR resourcce.
    """
    structdef = self._context.get_structure_definition(structdef_url)
    struct_type = _fhir_path_data_types.StructureDataType.from_proto(structdef)
    builder = (
        column_expression_builder.ColumnExpressionBuilder.from_node_and_handler(
            _evaluation.RootMessageNode(self._context, struct_type),
            self._handler,
        )
    )
    return View(self._context, builder, (), (), self._handler)

  def expression_for(
      self, structdef_url: str
  ) -> column_expression_builder.ColumnExpressionBuilder:
    """Returns a ColumnExpressionBuilder for the given structure definition.

    This can be convenient when building predicates for complicate where() or
    all() FHIRPath expressions. For instance, suppose we want to get LOINC
    codes in for a view of Observations:

    >>> obs = views.view_of('Observation')
    >>> coding = views.expression_for('Coding')
    >>>
    >>> obs.select([
    >>>   obs.code.coding.where(coding.system ==
    >>>                         'http://loinc.org').code.alias('loinc_codes')
    >>>   ])

    Since the `where` expression is a path to a FHIR Coding structure,
    it's convenient to use an expression builder based on Coding to create that.

    Note that for simpler expressions it is often easier to directly access
    the builder on the resource. This expression is exactly equivalent of
    the one above -- it just uses the Coding expression builder that already
    exists as part of the larger resource:

    >>> obs.select([
    >>>   obs.code.coding.where(obs.code.coding.system ==
    >>>                         'http://loinc.org').code.alias('loinc_codes')
    >>>   )

    Args:
      structdef_url: URL of the FHIR resource to load. Per the FHIR spec, an
        unqualified URL will be considered to be relative to
        'http://hl7.org/fhir/StructureDefinition/', so for core datatypes or
        resources callers can simply pass in 'Patient' or 'HumanName', for
        example.

    Returns:
      A ColumnExpressionBuilder for the given structure.
    """
    structdef = self._context.get_structure_definition(structdef_url)
    struct_type = _fhir_path_data_types.StructureDataType.from_proto(structdef)
    return (
        column_expression_builder.ColumnExpressionBuilder.from_node_and_handler(
            _evaluation.StructureBaseNode(self._context, struct_type),
            self._handler,
        )
    )

  def from_view_definition(self, view_definition: Dict[str, Any]) -> View:
    """Returns a view of the FHIR resource according to given view_definition.

    Args:
      view_definition: A JSON format view definition which aligns with the
        specification in
        'https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html'.

    Returns:
      A FHIR View builder for the given view_definition.
    """
    config = _view_config.ViewConfig(
        self._context, self._handler, view_definition
    )
    return (
        self.view_of(config.resource)
        .select(config.column_builders)
        .where(*config.constraint_builders)
    )

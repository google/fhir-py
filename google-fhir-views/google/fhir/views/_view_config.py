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
"""Library for parsing a JSON view definition to a ViewConfig class.

The JSON view definition follows the specification in
https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html.
"""

import abc
from typing import Any, Callable, Dict, List, Optional

from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.r4 import primitive_handler
from google.fhir.views import column_expression_builder


class ViewConfig:
  """Parses a JSON view definition to a config class to use in a View.

  A view definition must contain information of:
   * resource: FHIR Resource for the ViewDefinition.
   * Zero or more select clauses which define the content of columns within the
   view. Currently, we only support the simple alias-path select clause.

  An example of a basic view definition can be seen here:

  >>> view_definition = {
  >>>   "resource": "Patient",
  >>>   "select": [
  >>>     {
  >>>       "alias": "patient_id",
  >>>       "path": "id"
  >>>     },
  >>>     {
  >>>       "alias": "birth_date",
  >>>       "path": "birthDate"
  >>>     }
  >>>   ],
  >>>   "where": [{
  >>>       "path": "birthDate < @1960-01-01"
  >>>   }]
  >>> }

  Users most likely do not need to call this class by themselves. Instead,
  they can use it from the Views class as below:

  >>> views = r4.base_r4()
  >>> view = views.from_view_definition(view_definition)
  """

  def __init__(
      self,
      fhir_context: context.FhirPathContext,
      handler: primitive_handler.PrimitiveHandler,
      view_definition: Dict[str, Any],
  ):
    self._context = fhir_context
    self._handler = handler

    self._resource = view_definition['resource']
    self._select_list = view_definition['select']

    structdef = self._context.get_structure_definition(self._resource)
    self._struct_type = _fhir_path_data_types.StructureDataType.from_proto(
        structdef
    )

    self._column_builders = SelectList(
        self._fhir_path_to_column_builder,
        self._select_list,
    ).column_builders

    self._constraint_builders = []
    if 'where' in view_definition:
      for constraint in view_definition['where']:
        if 'path' not in constraint:
          raise KeyError(
              f'All where clauses must contain `path` fields. Got {constraint}.'
          )
        if not isinstance(constraint['path'], str):
          raise ValueError(
              'The `path` field in a where clause must be strings.'
              f' Got {constraint["path"]}.'
          )
        self._constraint_builders.append(
            self._fhir_path_to_column_builder(constraint['path'])
        )

  @property
  def resource(self) -> str:
    return self._resource

  @property
  def column_builders(
      self,
  ) -> List[column_expression_builder.ColumnExpressionBuilder]:
    return self._column_builders

  @property
  def constraint_builders(
      self,
  ) -> List[column_expression_builder.ColumnExpressionBuilder]:
    return self._constraint_builders

  def _fhir_path_to_column_builder(
      self,
      fhir_path: str,
      root: Optional[column_expression_builder.ColumnExpressionBuilder] = None,
  ) -> column_expression_builder.ColumnExpressionBuilder:
    builder = expressions.from_fhir_path_expression(
        fhir_path, self._context, self._struct_type, self._handler, root
    )
    return column_expression_builder.ColumnExpressionBuilder(builder)


class Select(abc.ABC):
  """Abstract base class to define a `select` clause in the view config."""

  def __init__(
      self,
      fhir_path_to_column_builder: Callable[
          ..., column_expression_builder.ColumnExpressionBuilder
      ],
      select: Dict[str, Any],
      root: Optional[column_expression_builder.ColumnExpressionBuilder] = None,
  ):
    self._fhir_path_to_column_builder = fhir_path_to_column_builder
    self._select = select
    self._root = root

  @property
  @abc.abstractmethod
  def column_builder(
      self,
  ) -> column_expression_builder.ColumnExpressionBuilder:
    """Returns the ColumnExpressionBuilder from the select clause."""


class PathSelect(Select):
  """One type of `select` clause which contains a `alias` and a `path`."""

  @property
  def column_builder(
      self,
  ) -> column_expression_builder.ColumnExpressionBuilder:
    alias = self._select['alias']
    path = self._select['path']
    if not isinstance(alias, str) or not isinstance(path, str):
      raise ValueError(
          'Both `alias` and `path` in a select clause must be strings.'
          f' Got {alias} and {path}.'
      )
    return self._fhir_path_to_column_builder(path, self._root).alias(alias)


class SelectList:
  """A list of `select` clauses."""

  def __init__(
      self,
      fhir_path_to_column_builder: Callable[
          ..., column_expression_builder.ColumnExpressionBuilder
      ],
      select_list: List[Dict[str, Any]],
      root: Optional[column_expression_builder.ColumnExpressionBuilder] = None,
  ):
    self._column_builders: List[
        column_expression_builder.ColumnExpressionBuilder
    ] = []

    for select in select_list:
      if 'alias' in select and 'path' in select:
        self._column_builders.append(
            PathSelect(fhir_path_to_column_builder, select, root).column_builder
        )
      else:
        raise NotImplementedError(
            'Only select clauses containing an `alias` and a `path` are'
            ' supported for now.'
        )

  @property
  def column_builders(
      self,
  ) -> List[column_expression_builder.ColumnExpressionBuilder]:
    """Returns the ColumnExpressionBuilders from the select list."""
    return self._column_builders

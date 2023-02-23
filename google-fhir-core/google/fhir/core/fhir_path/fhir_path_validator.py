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
"""Functionality for validating FHIRPath resources using SQL."""

import dataclasses
from typing import Any, Collection, Dict, List, Optional, Set, cast

from google.protobuf import message
from google.fhir.core.proto import validation_pb2
from google.fhir.core import codes
from google.fhir.core import fhir_errors
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _navigation
from google.fhir.core.fhir_path import _semant
from google.fhir.core.fhir_path import _sql_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import fhir_path
from google.fhir.core.fhir_path import fhir_path_options
from google.fhir.core.utils import fhir_package
from google.fhir.core.utils import proto_utils

# TODO(b/201107372): Update FHIR-agnostic types to a protocol.
StructureDefinition = message.Message
ElementDefinition = message.Message
Constraint = message.Message

# See more at: https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md
_PRIMITIVE_TO_STANDARD_SQL_MAP = {
    'base64Binary': _sql_data_types.String,
    'boolean': _sql_data_types.Boolean,
    'code': _sql_data_types.String,
    'date': _sql_data_types.String,
    'dateTime': _sql_data_types.String,
    'decimal': _sql_data_types.Numeric,
    'id': _sql_data_types.String,
    'instant': _sql_data_types.String,
    'integer': _sql_data_types.Int64,
    'markdown': _sql_data_types.String,
    'oid': _sql_data_types.String,
    'positiveInt': _sql_data_types.Int64,
    'string': _sql_data_types.String,
    'time': _sql_data_types.String,
    'unsignedInt': _sql_data_types.Int64,
    'uri': _sql_data_types.String,
    'xhtml': _sql_data_types.String,
}

# These primitives are excluded from regex encoding because at the point when
# our validation is called, they are already saved as their correct types.
_PRIMITIVES_EXCLUDED_FROM_REGEX_ENCODING = frozenset([
    'base64Binary',
    'boolean',
    'decimal',
    'integer',
    'xhtml',
])

# The `ElementDefinition.type.code` is a URL of the datatype or resource used
# for an element. References are URLs that are relative to:
# http://hl7.org/fhir/StructureDefinition.
#
# `ElementDefinition`s whose type codes overlap with this set will be silently
# skipped during profile traversal.
_SKIP_TYPE_CODES = frozenset([
    # TODO(b/193251325): Add support for traversing `targetProfile`s of a
    # `Reference` type.
    'Reference',
    # Ignore the Resource type. Because it can stand for any resource, it is
    # typed as a string in our protos. Thus we do not need to encode constraints
    # for it.
    'Resource',
])

# A list of fhir path constraint keys to skip.
_SKIP_KEYS = frozenset([
    # TODO(b/203253155): This constraint produces a regex that escapes
    # our string quotes.
    'eld-19',
    # TODO(b/206986228): Remove these keys after we start taking profiles into
    # account when encoding constraints for fields.
    'comparator-matches-code-regex',
    'comparator-memberOf',
    # Ignore this constraint because it is only directed towards primitive
    # fields.
    'ele-1',
    # Ignore these constraints because they require verifying html.
    'txt-1',
    'txt-2',
    # Ignore these constraints because they are directed towards
    # `DomainResource.contained` which is not supported by the SQL-on-FHIR
    # standard. More on why at `semant._UNSUPPORTED_BASE_PATHS`.
    'dom-2',
    'dom-3',
    'dom-4',
    'dom-5',
    # Ignore this constraint because it is directed towards `Extension` fields
    # which are not propagated to our protos or tables.
    'ext-1',
])


@dataclasses.dataclass
class _RegexInfo:
  """A named tuple with information needed to make a regex constraint."""

  regex: str
  type_code: str


def _escape_identifier(identifier_value: str) -> str:
  """Returns the value surrounded by backticks if it is a keyword."""
  # Keywords are case-insensitive
  if identifier_value.upper() in _sql_data_types.STANDARD_SQL_KEYWORDS:
    return f'`{identifier_value}`'
  return identifier_value  # No-op


def _get_analytic_path(element_definition: ElementDefinition) -> str:
  """Returns the identifying dot-separated (`.`) analytic path of the element.

  The `analytic path` is:
  - If the given element is a slice on an extension, it returns the element id
    with the `extension` part discarded.
    (e.g: if slice element id is `Foo.extension:slice`, it returns `Foo.slice`)
  - Else, the element.path attribute.

  Args:
    element_definition: The element definition that we are operating on.
  """
  if _utils.is_slice_on_extension(element_definition):
    initial_path: str = cast(Any, element_definition).id.value
    return initial_path.replace('extension:', '')

  if not proto_utils.field_is_set(element_definition, 'path'):
    raise ValueError(
        f'Required field "path" is not set for {element_definition}.'
    )
  return cast(Any, element_definition).path.value


def _last_path_token(element_definition: ElementDefinition) -> str:
  """Returns `element_definition`'s last path token less the resource type.

  For example:
    * "Foo" returns "" (empty string)
    * "Foo.bar" returns "bar"
    * "Foo.bar.bats" returns "bats"

  Args:
    element_definition: The `ElementDefinition` whose relative path to return.
  """
  path = _get_analytic_path(element_definition)
  components_less_resource = path.split('.')[1:]
  return components_less_resource[-1] if components_less_resource else ''


def _is_type(element_definition: ElementDefinition, type_code: str) -> bool:
  """Returns `True` if `element_definition` is of type, `type_code`."""
  type_codes = _utils.element_type_codes(element_definition)
  if len(type_codes) != 1:
    return False
  return type_codes[0] == type_code


def _path_to_sql_column_name(path: str) -> str:
  """Given a path to an `ElementDefinition`, returns a SQL column name."""
  return path.lower().replace('.', '_')


def _key_to_sql_column_name(key: str) -> str:
  """Given a constraint key, returns a SQL column name."""
  return key.lower().replace('-', '_')


def _is_required(element_definition: ElementDefinition) -> bool:
  """Returns true if the given element_definition is required."""
  return cast(Any, element_definition).min.value > 0


def _is_disabled(element_definition: ElementDefinition) -> bool:
  """Returns true if the given element_definition is a disabled by a profile."""
  return cast(Any, element_definition).max.value == '0'


def _escape_fhir_path_identifier(identifier: str) -> str:
  if identifier in _fhir_path_data_types.RESERVED_FHIR_PATH_KEYWORDS:
    return f'`{identifier}`'
  return identifier


def _escape_fhir_path_invocation(invocation: str) -> str:
  """Returns the given fhir path invocation with reserved words escaped."""
  identifiers = invocation.split('.')
  return '.'.join([_escape_fhir_path_identifier(id_) for id_ in identifiers])


def _get_regex_from_element_type(type_: message.Message):
  """Returns regex from ElementDefinition.type if available."""
  for sub_type in cast(Any, type_):
    for extension in sub_type.extension:
      if extension.url.value == 'http://hl7.org/fhir/StructureDefinition/regex':
        # Escape backslashes from regex.
        primitive_regex = extension.value.string_value.value.replace(
            '\\', '\\\\'
        )
        # Make regex a full match in sql.
        primitive_regex = f'^({primitive_regex})$'
        # If we found the regex we can stop here.
        return primitive_regex

  return None


def _get_regex_from_structure(
    structure_definition: StructureDefinition, type_code: str
) -> Optional[str]:
  """Returns the regex in the given StructureDefinition if it exists."""
  for element in cast(Any, structure_definition).snapshot.element:
    if element.id.value == f'{type_code}.value':
      primitive_regex = _get_regex_from_element_type(element.type)

      if primitive_regex is not None:
        return primitive_regex

  return None


def _is_primitive_typecode(type_code: str) -> bool:
  """Returns True if the given typecode is primitive. False otherwise."""
  return (
      type_code in _PRIMITIVE_TO_STANDARD_SQL_MAP
      or
      # Ids are a special case of primitive that have their type code equal to
      # 'http://hl7.org/fhirpath/System.String'.
      type_code == 'http://hl7.org/fhirpath/System.String'
  )


@dataclasses.dataclass
class State:
  """A named tuple for capturing position within a FHIR resource graph.

  For the root element in the resource graph, `containing_type` will contain the
  structure definition of that root element and `element` will contain the
  element definition (it is usually the first element definition in the
  structure definition) of that element.
  """

  element: ElementDefinition
  containing_type: StructureDefinition


class FhirProfileStandardSqlEncoder:
  """Standard SQL encoding of a `StructureDefinition`'s FHIRPath constraints.

  The encoder performs a pre-order recursive walk of a
  [FHIRProfile](https://www.hl7.org/fhir/profiling.html) represented as a
  [StructureDefinition](http://www.hl7.org/fhir/structuredefinition.html)
  protobuf message and translates its [FHIRPath](http://hl7.org/fhirpath/)
  constraints to a list of equivalent BigQuery Standard SQL expressions.

  Constraints encoded directly on the FHIRProfile as well as "transitory"
  constraints (e.g. constraints defined on types present as fields in the
  FHIRProfile under consideration) are encoded. If a field is un-set in a
  profile, the corresponding transitory constraints are considered vacuously-
  satsified, and the Standard SQL expression translations will produce `NULL` at
  runtime.

  All direct and transitory FHIRPath constraint Standard SQL expression
  encodings are returned as a list by the outer recursive walk over each profle.
  The caller can then join them into a `SELECT` clause, or perform further
  manipulation.
  """

  def __init__(
      self,
      package_manager: fhir_package.FhirPackageManager,
      error_reporter: fhir_errors.ErrorReporter,
      *,
      options: Optional[fhir_path.SqlGenerationOptions] = None,
      validation_options: Optional[
          fhir_path_options.SqlValidationOptions
      ] = None,
  ) -> None:
    """Creates a new instance of `FhirProfileStandardSqlEncoder`.

    Args:
      package_manager: The FHIR resources needed to build a "graph" for
        traversal and encoding of constraints.
      error_reporter: A `fhir_errors.ErrorReporter` delegate for error-handling.
      options: Defines a list of optional settings that can be used to customize
        the behaviour of FhirProfileStandardSqlEncoder.
      validation_options: Optional settings for influencing validation behavior.
    """
    # Persistent state provided during initialization that the profile encoder
    # uses for navigation, error reporting, configuration, etc.
    self._env = _navigation._Environment(
        package_manager.iter_structure_definitions()
    )
    self._error_reporter = error_reporter

    self._options = options or fhir_path.SqlGenerationOptions()
    self._options.value_set_codes_definitions = package_manager

    self._fhir_path_encoder = fhir_path.FhirPathStandardSqlEncoder(
        package_manager.iter_structure_definitions(),
        options=self._options,
        validation_options=validation_options,
    )
    # Add keys that currently cause issues internally.
    self._options.skip_keys.update(_SKIP_KEYS)

    # Ephemeral state that is guaranteed to be cleaned-up between invocations
    # of `encode`.
    self._ctx: List[State] = []
    self._in_progress: Set[str] = set()
    self._requirement_column_names: Set[str] = set()
    self._element_id_to_regex_map: Dict[str, _RegexInfo] = {}
    self._regex_columns_generated = set()

  def _abs_path_invocation(self) -> str:
    """Returns the absolute path invocation given the traversal context."""
    if not self._ctx:
      return ''

    bottom = self._ctx[0]
    root_path = _get_analytic_path(bottom.element)
    path_components = [_last_path_token(s.element) for s in self._ctx[1:]]
    return '.'.join([root_path] + [c for c in path_components if c])

  def _encode_fhir_path_constraint(
      self,
      structure_definition: StructureDefinition,
      element_definition: ElementDefinition,
      fhir_path_expression: str,
  ) -> Optional[str]:
    """Returns a Standard SQL translation of the constraint `fhir_path_expression`.

    If an error is encountered during encoding, the associated error reporter
    will be notified, and this method will return `None`.

    Args:
      structure_definition: The `StructureDefinition` containing the provided
        `element_definition` that the expression is defined with respect to.
      element_definition: The `ElementDefinition` that `fhir_path_expression` is
        defined with respect to.
      fhir_path_expression: The fluent-style dot-delimited ('.') FHIRPath
        expression to encode to Standard SQL.

    Returns:
      A Standard SQL encoding of the constraint `fhir_path_expression` upon
      successful completion. The SQL will evaluate to a single boolean
      indicating whether the constraint is satisfied.
    """
    try:
      sql_expression = self._fhir_path_encoder.encode(
          structure_definition=structure_definition,
          element_definition=element_definition,
          fhir_path_expression=fhir_path_expression,
      )
    # Delegate all FHIRPath encoding errors to the associated `ErrorReporter`
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(),
          fhir_path_expression,
          str(e),
      )
      return None

    # Check to see if `fhir_path_expression` is a top-level constraint or a
    # transitive constraint. If top-level, simply return `sql_expression`.
    # If transitive, we need to add a supporting context query. This is
    # accomplished by a separate call to the `_FhirPathStandardSqlEncoder`,
    # passing the relative path invocation as a synthetic FHIRPath query that
    # should be executed from the `bottom` root element.
    # We determine if this is a top-level constraint by checking if
    # fhir_path_expression` is defined relative to the bottom root element.
    bottom = self._ctx[0]
    bottom_root_element = self._env.get_root_element_for(bottom.containing_type)
    if bottom_root_element is None:
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(),
          fhir_path_expression,
          (
              'No root element definition for: '
              f'{cast(Any, bottom.containing_type).url.value}.'
          ),
      )
      return None

    if bottom_root_element == element_definition:
      return (
          '(SELECT IFNULL(LOGICAL_AND(result_), TRUE)\n'
          f'FROM UNNEST({sql_expression}) AS result_)'
      )

    path_invocation = _escape_fhir_path_invocation(self._abs_path_invocation())
    path_invocation_less_resource = '.'.join(path_invocation.split('.')[1:])
    try:
      root_sql_expression = self._fhir_path_encoder.encode(
          structure_definition=bottom.containing_type,
          element_definition=bottom_root_element,
          fhir_path_expression=path_invocation_less_resource,
      )
    # Delegate all FHIRPath encoding errors to the associated `ErrorReporter`
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(),
          fhir_path_expression,
          str(e),
      )
      return None

    # Bind the two expressions together via a correlated `ARRAY` subquery
    sql_expression = (
        '(SELECT IFNULL(LOGICAL_AND(result_), TRUE)\n'
        f'FROM (SELECT {sql_expression} AS subquery_\n'
        'FROM (SELECT AS VALUE ctx_element_\n'
        f'FROM UNNEST({root_sql_expression}) AS ctx_element_)),\n'
        'UNNEST(subquery_) AS result_)'
    )
    return sql_expression

  def _encode_constraints(
      self,
      structure_definition: StructureDefinition,
      element_definition: ElementDefinition,
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of `SqlRequirement`s for FHIRPath constraints.

    Args:
      structure_definition: The enclosing `StructureDefinition`.
      element_definition: The `ElementDefinition` whose constraints should be
        encoded.

    Returns:
      A list of `SqlRequirement`s expressing FHIRPath constraints defined on the
      `element_definition`.
    """
    result: List[validation_pb2.SqlRequirement] = []
    constraints: List[Constraint] = cast(Any, element_definition).constraint
    for constraint in constraints:
      constraint_key: str = cast(Any, constraint).key.value
      if constraint_key in self._options.skip_keys:
        continue

      # Metadata for the requirement
      fhir_path_expression: str = cast(Any, constraint).expression.value
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(constraint_key)
      column_name_base: str = _path_to_sql_column_name(element_definition_path)
      column_name = f'{column_name_base}_{constraint_key_column_name}'

      if column_name in self._requirement_column_names:
        self._error_reporter.report_fhir_path_error(
            element_definition_path,
            fhir_path_expression,
            f'Duplicate FHIRPath requirement: {column_name}.',
        )
        continue

      if cast(Any, constraint).severity.value == 0:
        self._error_reporter.report_fhir_path_error(
            element_definition_path,
            fhir_path_expression,
            'Constraint severity must be set.',
        )
        continue  # Malformed constraint

      # TODO(b/221470795): Remove this implementation when a better
      # implementation at the FhirPackage level has been added.
      # Replace fhir_path_expression if needed. This functionality is mainly for
      # temporary replacements of invalid expressions defined in the spec while
      # we wait for the spec to be updated.
      if self._options.expr_replace_list:
        for replacement in self._options.expr_replace_list.replacement:
          if (
              not replacement.element_path
              or replacement.element_path == element_definition_path
          ) and replacement.expression_to_replace == fhir_path_expression:
            fhir_path_expression = replacement.replacement_expression

      # Create Standard SQL expression
      sql_expression = self._encode_fhir_path_constraint(
          structure_definition,
          element_definition,
          fhir_path_expression,
      )
      if sql_expression is None:
        continue  # Failure to generate Standard SQL expression

      # Constraint type and severity metadata; default to WARNING
      # TODO(b/199419068): Cleanup validation severity mapping
      type_ = validation_pb2.ValidationType.VALIDATION_TYPE_FHIR_PATH_CONSTRAINT
      severity = cast(Any, constraint).severity
      severity_value_field = severity.DESCRIPTOR.fields_by_name.get('value')
      severity_str = codes.enum_value_descriptor_to_code_string(
          severity_value_field.enum_type.values_by_number[severity.value]
      )
      try:
        validation_severity = validation_pb2.ValidationSeverity.Value(
            f'SEVERITY_{severity_str.upper()}'
        )
      except ValueError:
        self._error_reporter.report_fhir_path_warning(
            element_definition_path,
            fhir_path_expression,
            f'Unknown validation severity conversion: {severity_str}.',
        )
        validation_severity = validation_pb2.ValidationSeverity.SEVERITY_WARNING

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=sql_expression,
          severity=validation_severity,
          type=type_,
          element_path=element_definition_path,
          description=cast(Any, constraint).human.value,
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_expression,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              fhir_path_expression
          ),
      )

      self._requirement_column_names.add(column_name)
      result.append(requirement)

    return result

  # TODO(b/222541838): Handle general cardinality requirements.
  def _encode_required_fields(
      self,
      structure_definition: message.Message,
      element_definition: message.Message,
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns `SqlRequirement`s for all required fields in `ElementDefinition`.

    Args:
      structure_definition: The enclosing `StructureDefinition`.
      element_definition: The element to encode required fields for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      required fields on the element.
    """

    # If this is an extension, we don't want to access its children/fields.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if cast(Any, structure_definition).type.value == 'Extension':
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []
    children = self._env.get_children(structure_definition, element_definition)
    for child in children:
      # This allows us to encode required fields on slices of extensions while
      # filtering out slices on non-extensions.
      # TODO(b/202564733): Properly handle slices that are not slices on
      # extensions.
      if _utils.is_slice_element(child) and not _utils.is_slice_on_extension(
          child
      ):
        continue

      min_size = cast(Any, child).min.value
      max_size = cast(Any, child).max.value
      relative_path = _last_path_token(child)
      element_count = f'{_escape_fhir_path_invocation(relative_path)}.count()'

      query_list = []

      if _utils.is_repeated_element(child) and max_size.isdigit():
        query_list.append(f'{element_count} <= {max_size}')

      if min_size == 1:
        query_list.append(
            f'{_escape_fhir_path_invocation(relative_path)}.exists()'
        )
      elif min_size > 0:
        query_list.append(f'{min_size} <= {element_count}')

      if not query_list:
        continue

      constraint_key = f'{relative_path}-cardinality-is-valid'
      description = (
          f'The length of {relative_path} must be maximum '
          f'{max_size} and minimum {min_size}.'
      )

      fhir_path_expression = ' and '.join(query_list)

      if constraint_key in self._options.skip_keys:
        continue  # Allows users to skip required field constraints.

      # Early-exit if any types overlap with `_SKIP_TYPE_CODES`.
      type_codes = _utils.element_type_codes(child)
      if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
        continue

      required_sql_expression = self._encode_fhir_path_constraint(
          structure_definition, element_definition, fhir_path_expression
      )
      if required_sql_expression is None:
        continue  # Failure to generate Standard SQL expression.

      # Create the `SqlRequirement`.
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(
          _path_to_sql_column_name(constraint_key)
      )
      column_name_base: str = _path_to_sql_column_name(
          self._abs_path_invocation()
      )
      column_name = f'{column_name_base}_{constraint_key_column_name}'

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=required_sql_expression,
          severity=(validation_pb2.ValidationSeverity.SEVERITY_ERROR),
          type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
          element_path=element_definition_path,
          description=description,
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_expression,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              fhir_path_expression
          ),
      )
      encoded_requirements.append(requirement)
    return encoded_requirements

  def get_extension_value_element(
      self,
      structure_definition: StructureDefinition,
      element_definition: ElementDefinition,
  ) -> Optional[ElementDefinition]:
    """Returns the value element of the given extension structure/ element pair.

    Args:
      structure_definition: The structure_definition of that extension.
      element_definition: The root element_definition of that extension.

    Returns:
      The value element of the given structure definition and root element
      pair. If a value element cannot be found, returns None.
    """
    children = self._env.get_children(structure_definition, element_definition)

    for child in children:
      base_path = cast(Any, child).base.path.value
      # Extract value element.
      if base_path == 'Extension.value[x]':
        return child

    return None

  def get_type_codes_from_slice_element(
      self, element_definition: ElementDefinition
  ) -> List[str]:
    """Returns the type codes of slice elements."""

    element_definition_path = _get_analytic_path(element_definition)

    # This function currently only supports getting type codes from slices on
    # extensions.
    if not _utils.is_slice_on_extension(element_definition):
      self._error_reporter.report_conversion_error(
          element_definition_path,
          (
              'Attempted to get type code from slice of non-extension.'
              ' Which is not supported.'
          ),
      )
      return []

    urls = _utils.slice_element_urls(element_definition)
    # TODO(b/190679571): Handle choice types.
    if not urls:
      raise ValueError(
          'Unable to get url for slice on extension with id: '
          f'{_get_analytic_path(element_definition)}'
      )

    if len(urls) > 1:
      raise ValueError(
          'Expected element with only one url but got: '
          f'{urls}, is this a choice type?'
      )

    url = urls[0]
    containing_type = self._env.get_structure_definition_for(url)
    if containing_type is None:
      self._error_reporter.report_conversion_error(
          element_definition_path,
          f'Unable to find `StructureDefinition` for: {url}.',
      )
      return []

    root_element = self._env.get_root_element_for(containing_type)
    if root_element is None:
      self._error_reporter.report_conversion_error(
          element_definition_path,
          f'Unable to find root `ElementDefinition` for: {url}.',
      )
      return []

    value_element = self.get_extension_value_element(
        containing_type, root_element
    )

    if value_element is None or _is_disabled(value_element):
      # At this point, the current element is a slice on an extension that has
      # no valid `Extension.value[x]` element, so we assume it is a complex
      # extension.
      # TODO(b/200575760): Handle complex extensions.
      return []
    else:
      return _utils.element_type_codes(value_element)

  # TODO(b/207690471): Move important ElementDefinition (and other) functions
  # to their respective utility modules and unit test their public facing apis .
  def _get_regex_from_element(
      self, element_definition: ElementDefinition
  ) -> Optional[_RegexInfo]:
    """Returns the regex of this element_definition if available."""

    type_codes = _utils.element_type_codes(element_definition)

    if _utils.is_slice_on_extension(element_definition):
      type_codes = self.get_type_codes_from_slice_element(element_definition)

    if not type_codes:
      return None
    if len(type_codes) > 1:
      self._error_reporter.report_validation_error(
          self._abs_path_invocation(),
          (
              f'Element `{_get_analytic_path(element_definition)}` with type'
              f' codes: {type_codes}, is a choice type which is not currently'
              ' supported.'
          ),
      )
      return None

    current_type_code = type_codes[0]

    element_id: str = cast(Any, element_definition).id.value
    # TODO(b/208620019): Look more into how this section handles multithreading.
    # If we have memoised the regex of this element, then just return it.
    if element_id in self._element_id_to_regex_map:
      return self._element_id_to_regex_map[element_id]

    # Ignore regexes on primitive types that are not represented as strings.
    if current_type_code == 'positiveInt' or current_type_code == 'unsignedInt':
      return _RegexInfo(regex='', type_code=current_type_code)

    # TODO(b/207018908): Remove this after we figure out a better way to encode
    # primitive regex constraints for id fields.
    # If the current element_definition ends with `.id` and it's type_code is
    # `http://hl7.org/fhirpath/System.String`, then assume it is an `id` type.
    # We only care about ids that are direct children of a resource
    # E.g. `Foo.id` and not `Foo.bar.id`. These ids will have a base path of
    # `Resource.id`.
    base_path: str = cast(Any, element_definition).base.path.value
    if (
        base_path == 'Resource.id'
        and current_type_code == 'http://hl7.org/fhirpath/System.String'
    ):
      current_type_code = 'id'

    # If the current_type_code is non primitive we filter it out here.
    if (
        current_type_code in _PRIMITIVE_TO_STANDARD_SQL_MAP
        and current_type_code not in _PRIMITIVES_EXCLUDED_FROM_REGEX_ENCODING
    ):
      primitive_url = _utils.get_absolute_uri_for_structure(current_type_code)

      # If we have not memoised it, then extract it from its
      # `StructureDefinition`.
      type_definition = self._env.get_structure_definition_for(primitive_url)
      regex_value = _get_regex_from_structure(
          type_definition, current_type_code
      )
      if regex_value is None:
        self._error_reporter.report_validation_error(
            self._abs_path_invocation(),
            (
                'Unable to find regex pattern for; '
                f'type_code:`{current_type_code}` '
                f'and url:`{primitive_url}` in environment.'
            ),
        )
      else:
        # Memoise the regex of this element for quick retrieval
        # later.
        regex_info = _RegexInfo(regex_value, current_type_code)
        self._element_id_to_regex_map[element_id] = regex_info
        return regex_info

    return None

  def _encode_primitive_regexes(
      self,
      structure_definition: message.Message,
      element_definition: ElementDefinition,
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns regex `SqlRequirement`s for primitives in `ElementDefinition`.

    This function generates regex `SqlRequirement`s specifically for the direct
    child elements of the given `element_definition`.

    Args:
      structure_definition: The enclosing `StructureDefinition`.
      element_definition: The `ElementDefinition` to encode primitive regexes
        for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      primitive fields on the element that have regexes .
    """

    element_definition_path = self._abs_path_invocation()
    # TODO(b/206986228): Remove this key after we start taking profiles into
    # account when encoding constraints for fields.
    if 'comparator' in element_definition_path.split('.'):
      return []

    # If this is an extension, we don't want to access its children/fields.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if cast(Any, structure_definition).type.value == 'Extension':
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []
    children = self._env.get_children(structure_definition, element_definition)
    for child in children:
      # TODO(b/190679571): Handle choice types, which may have more than one
      # `type.code` value present.
      # If this element is a choice type, a slice (that is not on an extension)
      # or is disabled, then don't encode requirements for it.
      # TODO(b/202564733): Properly handle slices on non-simple extensions.
      if ('[x]' in _get_analytic_path(child) or _is_disabled(child)) or (
          _utils.is_slice_element(child)
          and not _utils.is_slice_on_extension(child)
      ):
        continue

      primitive_regex_info = self._get_regex_from_element(child)
      if primitive_regex_info is None:
        continue  # Unable to find primitive regexes for this child element.

      primitive_regex = primitive_regex_info.regex
      regex_type_code = primitive_regex_info.type_code

      relative_path = _last_path_token(child)
      constraint_key = f'{relative_path}-matches-{regex_type_code}-regex'

      if constraint_key in self._options.skip_keys:
        continue  # Allows users to skip specific regex checks.

      # Early-exit if any types overlap with `_SKIP_TYPE_CODES`.
      type_codes = _utils.element_type_codes(child)
      if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
        continue

      escaped_relative_path = _escape_fhir_path_invocation(relative_path)

      # Generate the FHIR path expression that checks regexes, while also
      # accounting for repeated fields, as FHIR doesn't allow function calls to
      # `matches` where the input collection is repeated.
      # More info here:
      # http://hl7.org/fhirpath/index.html#matchesregex-string-boolean.
      element_is_repeated = _utils.is_repeated_element(child)
      fhir_path_expression = (
          f"{escaped_relative_path}.all( $this.matches('{primitive_regex}') )"
          if element_is_repeated
          else f"{escaped_relative_path}.matches('{primitive_regex}')"
      )

      # Handle special typecode cases, while also accounting for repeated fields
      # , as FHIR doesn't allow direct comparisons involving repeated fields.
      # More info here:
      # http://hl7.org/fhirpath/index.html#comparison.
      if regex_type_code == 'positiveInt':
        fhir_path_expression = (
            f'{escaped_relative_path}.all( $this > 0 )'
            if element_is_repeated
            else f'{escaped_relative_path} > 0'
        )
      if regex_type_code == 'unsignedInt':
        fhir_path_expression = (
            f'{escaped_relative_path}.all( $this >= 0 )'
            if element_is_repeated
            else f'{escaped_relative_path} >= 0'
        )

      required_sql_expression = self._encode_fhir_path_constraint(
          structure_definition, element_definition, fhir_path_expression
      )
      if required_sql_expression is None:
        continue  # Failure to generate Standard SQL expression.

      # Create the `SqlRequirement`.
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(
          _path_to_sql_column_name(constraint_key)
      )
      column_name_base: str = _path_to_sql_column_name(
          self._abs_path_invocation()
      )
      column_name = f'{column_name_base}_{constraint_key_column_name}'
      if column_name in self._regex_columns_generated:
        continue
      self._regex_columns_generated.add(column_name)

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=required_sql_expression,
          severity=(validation_pb2.ValidationSeverity.SEVERITY_ERROR),
          type=validation_pb2.ValidationType.VALIDATION_TYPE_PRIMITIVE_REGEX,
          element_path=element_definition_path,
          description=(
              f'{relative_path} needs to match regex of {regex_type_code}.'
          ),
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_expression,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              fhir_path_expression
          ),
      )
      encoded_requirements.append(requirement)

    return encoded_requirements

  def _encode_element_definition(
      self,
      structure_definition: StructureDefinition,
      element_definition: ElementDefinition,
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of Standard SQL expressions for an `ElementDefinition`."""
    result: List[validation_pb2.SqlRequirement] = []

    # This filters out choice types as they are currently not supported.
    # TODO(b/190679571): Handle choice types, which may have more than one
    # `type.code` value present.
    element_definition_path = (
        f'{self._abs_path_invocation()}.{_last_path_token(element_definition)}'
    )
    if '[x]' in _get_analytic_path(element_definition):
      self._error_reporter.report_conversion_error(
          element_definition_path,
          'The given element is a choice type, which is not yet supported.',
      )
      return result

    # This filters out slices that are not on extensions as they are currently
    # not supported.
    # TODO(b/202564733): Properly handle slices that are not on extensions.
    if _utils.is_slice_element(
        element_definition
    ) and not _utils.is_slice_on_extension(element_definition):
      self._error_reporter.report_conversion_error(
          element_definition_path,
          (
              'The given element is a slice that is not on an extension. This'
              ' is not yet supported.'
          ),
      )
      return result

    type_codes = _utils.element_type_codes(element_definition)
    if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
      return result  # Early-exit if any types overlap with `_SKIP_TYPE_CODES`

    # `ElementDefinition.base.path` is guaranteed to be present for snapshots
    base_path: str = cast(Any, element_definition).base.path.value
    if base_path in _semant.UNSUPPORTED_BASE_PATHS:
      return result  # Early-exit if unsupported `ElementDefinition.base.path`

    # Recurse over the `element_definition`s type
    type_codes = _utils.element_type_codes(element_definition)

    # Mark `(element_definition, structure_definition)` as being visited
    self._ctx.append(State(element_definition, structure_definition))

    # At this point there are no choice types so every element_definition should
    # have at most one type code.
    # Avoid encoding any constraints for the raw `Extension` type, because it's
    # fields are not propagated to the our tables.
    if (
        type_codes
        and not _is_primitive_typecode(type_codes[0])
        and type_codes[0] != 'Extension'
    ):
      type_code = type_codes[0]
      url = _utils.get_absolute_uri_for_structure(type_code)
      parent_structure_definition = self._env.get_structure_definition_for(url)
      if parent_structure_definition is None:
        self._error_reporter.report_conversion_error(
            self._abs_path_invocation(),
            f'Unable to find `StructureDefinition`: `{url}` in environment.',
        )
      else:
        result += self._encode(parent_structure_definition)

    # Encode all relevant FHIRPath expression constraints, prior to recursing on
    # chidren.
    result += self._encode_constraints(structure_definition, element_definition)
    result += self._encode_required_fields(
        structure_definition, element_definition
    )
    if self._options.add_primitive_regexes:
      result += self._encode_primitive_regexes(
          structure_definition, element_definition
      )

    if self._options.add_value_set_bindings:
      result += self._encode_value_set_bindings(element_definition)

    # Ignores the fields inside complex extensions.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if cast(Any, structure_definition).type.value != 'Extension':
      children = self._env.get_children(
          structure_definition, element_definition
      )
      for child in children:
        result += self._encode_element_definition(structure_definition, child)

    # Finish visiting `(element_definition, structure_definition)`
    _ = self._ctx.pop()

    return result

  def _encode_value_set_bindings(
      self, element_definition: ElementDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Encode .memberOf calls implied by elements bound to value sets."""
    # Ensure the element defines a value set binding.
    binding = cast(Any, element_definition).binding
    value_set_uri: str = binding.value_set.value
    if not value_set_uri:
      return []

    # Ensure the binding is required, see
    # https://build.fhir.org/valueset-binding-strength.html#expansion
    required_enum_val: int = (
        binding.strength.DESCRIPTOR.fields_by_name['value']
        .enum_type.values_by_name['REQUIRED']
        .number
    )
    if binding.strength.value != required_enum_val:
      return []

    # Ensure we aren't configured to skip this validation.
    relative_path = _last_path_token(element_definition)
    constraint_key = '%s-memberOf' % relative_path
    if constraint_key in self._options.skip_keys:
      return []

    # Attempt to build SQL for the binding.
    # We always want to build top-level, non-transitive constraints. Breaking
    # the generated SQL expressions into two parts, with one providing the
    # context, and running them together as correlated queries can introduce
    # errors from BigQuery like:
    # "Correlated subqueries that reference other tables are not supported
    # unless they can be de-correlated, such as by transforming them into an
    # efficient JOIN."
    # The SQL generated for memberOf queries handles being called on NULLs by
    # itself. It does not rely on the context returning an empty result set for
    # NULLs.
    path_invocation_less_resource = '.'.join(
        self._abs_path_invocation().split('.')[1:]
    )
    top_level_fhir_path_expression = "%s.memberOf('%s')" % (
        _escape_fhir_path_invocation(path_invocation_less_resource),
        value_set_uri,
    )

    relative_fhir_path_expression = "%s.memberOf('%s')" % (
        _escape_fhir_path_invocation(relative_path),
        value_set_uri,
    )

    # Build the expression against the top-level resource.
    bottom = self._ctx[0]
    bottom_root_element = self._env.get_root_element_for(bottom.containing_type)
    sql_expression = self._encode_fhir_path_constraint(
        bottom.containing_type,
        bottom_root_element,
        top_level_fhir_path_expression,
    )
    if sql_expression is None:
      return []

    # _abs_path_invocation() is the path to the bound code field.
    # Remove the final path item to get the relative path for this bound code.
    element_definition_path = '.'.join(
        self._abs_path_invocation().split('.')[:-1]
    )
    column_name = _key_to_sql_column_name(
        _path_to_sql_column_name('%s-memberOf' % self._abs_path_invocation())
    )
    description = '%s must be a member of %s' % (relative_path, value_set_uri)
    return [
        validation_pb2.SqlRequirement(
            column_name=column_name,
            sql_expression=sql_expression,
            severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
            type=(
                validation_pb2.ValidationType.VALIDATION_TYPE_VALUE_SET_BINDING
            ),
            element_path=element_definition_path,
            description=description,
            fhir_path_key=constraint_key,
            fhir_path_expression=relative_fhir_path_expression,
            fields_referenced_by_expression=_fields_referenced_by_expression(
                relative_fhir_path_expression
            ),
        )
    ]

  def _encode(
      self, structure_definition: StructureDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Recursively encodes the provided resource into Standard SQL."""
    url_value: str = cast(Any, structure_definition).url.value
    if url_value in self._in_progress:
      self._error_reporter.report_conversion_error(
          self._abs_path_invocation(),
          f'Cycle detected when encoding: {url_value}.',
      )
      return []

    root_element = self._env.get_root_element_for(structure_definition)
    if root_element is None:
      self._error_reporter.report_conversion_error(
          self._abs_path_invocation(),
          f'No root element definition found for: {url_value}.',
      )
      return []

    self._in_progress.add(url_value)
    result = self._encode_element_definition(structure_definition, root_element)
    # Removes duplicates (Same SQL Expression) from our list of requirements.
    result = list(
        {
            requirement.sql_expression: requirement for requirement in result
        }.values()
    )
    self._in_progress.remove(url_value)
    return result

  def encode(
      self, structure_definition: StructureDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Encodes the provided resource into a list of Standard SQL expressions."""
    result: List[validation_pb2.SqlRequirement] = []

    try:
      # Call into our protected recursive-helper method to encode the provided
      # `StructureDefinition`. Propagate any exceptions that occur, and always
      # cleanup state prior to returning.
      result = self._encode(structure_definition)
    finally:
      self._ctx.clear()
      self._in_progress.clear()
      self._requirement_column_names.clear()
      self._element_id_to_regex_map.clear()
      self._regex_columns_generated.clear()

    return result


def _fields_referenced_by_expression(
    fhir_path_expression: str,
) -> Collection[str]:
  """Finds paths for fields referenced by the given expression.

  For example, an expression like 'a.b.where(c > d.e)' references fields
  ['a.b', 'c, 'd.e']

  Args:
    fhir_path_expression: The expression to search for field paths.

  Returns:
    A collection of paths for fields referenced in the given expression.
  """
  # Sort the results so they are consistently ordered for the golden tests.
  return sorted(
      _ast.paths_referenced_by(_ast.build_fhir_path_ast(fhir_path_expression))
  )

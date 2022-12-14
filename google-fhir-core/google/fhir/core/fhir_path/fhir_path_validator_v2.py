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
from typing import Any, Collection, List, Optional, Set, cast, Dict

from google.cloud import bigquery

from google.protobuf import message
from google.fhir.core.proto import fhirpath_replacement_list_pb2
from google.fhir.core.proto import validation_pb2
from google.fhir.core import codes
from google.fhir.core import fhir_errors
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _bigquery_interpreter
from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.core.internal import primitive_handler
from google.fhir.core.utils import proto_utils

# TODO(b/201107372): Update FHIR-agnostic types to a protocol.
StructureDefinition = message.Message
ElementDefinition = message.Message
Constraint = message.Message

# The `ElementDefinition.base.path` identifies the base element. This matches
# the `ElementDefinition.path` for that element. Across FHIR, there is only one
# base definition of any element.
#
# `ElementDefinition`s whose `base.path` is present in `_UNSUPPORTED_BASE_PATHS`
# will be silently skipped during profile traversal, and will raise an exception
# during FHIRPath-to-Standard-SQL encoding.
UNSUPPORTED_BASE_PATHS = frozenset([
    # Contained Resources do not map cleanly to SQL and are not supported by
    # the SQL-on-FHIR standard.
    'DomainResource.contained',
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
    # TODO(b/206986228): Remove this key after we start taking profiles into
    # account when encoding constraints for fields.
    'comparator-matches-code-regex',
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

# These primitives are excluded from regex encoding because at the point when
# our validation is called, they are already saved as their correct types.
_PRIMITIVES_EXCLUDED_FROM_REGEX_ENCODING = frozenset([
    'base64Binary',
    'boolean',
    'decimal',
    'integer',
    'xhtml',
])


@dataclasses.dataclass
class _RegexInfo:
  """A named tuple with information needed to make a regex constraint."""
  regex: str
  type_code: str


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
        f'Required field "path" is not set for {element_definition}.')
  return cast(Any, element_definition).path.value


def _last_path_token(builder: expressions.Builder) -> str:
  """Returns `builder`'s last path token less the resource type.

  For example:
    * "Foo" returns "" (empty string)
    * "Foo.bar" returns "bar"
    * "Foo.bar.bats" returns "bats"

  Args:
    builder: The `builder` whose relative path to return.
  """
  if isinstance(builder.get_node(), _evaluation.RootMessageNode):
    return ''
  # The last node will be the last path token.
  return builder.get_node().to_path_token()


def _path_to_sql_column_name(path: str) -> str:
  """Given a path to an `ElementDefinition`, returns a SQL column name."""
  return path.lower().replace('.', '_')


def _key_to_sql_column_name(key: str) -> str:
  """Given a constraint key, returns a SQL column name."""
  return key.lower().replace('-', '_')


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
      if (extension.url.value == 'http://hl7.org/fhir/StructureDefinition/regex'
         ):
        # Escape backslashes from regex.
        primitive_regex = extension.value.string_value.value
        # Make regex a full match in sql.
        primitive_regex = f'^({primitive_regex})$'
        # If we found the regex we can stop here.
        return primitive_regex

  return None


def _get_regex_from_structure(structure_definition: StructureDefinition,
                              type_code: str) -> Optional[str]:
  """Returns the regex in the given StructureDefinition if it exists."""
  for element in cast(Any, structure_definition).snapshot.element:
    if element.id.value == f'{type_code}.value':
      primitive_regex = _get_regex_from_element_type(element.type)

      if primitive_regex is not None:
        return primitive_regex

  return None


def _is_elem_supported(element_definition: ElementDefinition) -> bool:
  """Returns whether the current element is supported by the validator."""
  # This allows us to encode required fields on slices of extensions while
  # filtering out slices on non-extensions.
  # TODO(b/202564733): Properly handle slices that are not slices on
  # extensions.
  if (_utils.is_slice_element(element_definition) and
      not _utils.is_slice_on_extension(element_definition)):
    return False
  elem = cast(Any, element_definition)
  # TODO(b/254866189): Skip links.
  return not elem.content_reference or '#' not in elem.content_reference.value


@dataclasses.dataclass
class SqlGenerationOptions:
  """Used by FhirProfileStandardSqlEncoder to define optional settings.

  Attributes:
    skip_keys: A set of constraint keys that should be skipped during encoding.
    add_primitive_regexes: Whether or not to add constraints requiring primitive
      fields to match their corresponding regex.
    add_value_set_bindings: Whether or not to add constraints enforcing
      membership of codes in the value sets defined by the implementation guide
    expr_replace_list: A list that specifies fhir path expressions to be
      replaced. It also specifies what they should be replaced with.
    value_set_codes_table: The name of the database table containing value set
      code definitions. Used when building SQL for memberOf expressions.
  """
  skip_keys: Set[str] = dataclasses.field(default_factory=set)
  add_primitive_regexes: bool = False
  expr_replace_list: fhirpath_replacement_list_pb2.FHIRPathReplacementList = (
      fhirpath_replacement_list_pb2.FHIRPathReplacementList())
  add_value_set_bindings: bool = False
  value_set_codes_table: bigquery.TableReference = None


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
      structure_definitions: List[StructureDefinition],
      handler: primitive_handler.PrimitiveHandler,
      error_reporter: fhir_errors.ErrorReporter,
      options: Optional[SqlGenerationOptions] = None,
  ) -> None:
    """Creates a new instance of `FhirProfileStandardSqlEncoder`.

    Args:
      structure_definitions: The list of `StructureDefinition`s comprising the
        FHIR resource "graph" for traversal and encoding of constraints.
      handler: Computes primitives with respect to the specification.
      error_reporter: A `fhir_errors.ErrorReporter` delegate for error-handling.
      options: Defines a list of optional settings that can be used to customize
        the behaviour of FhirProfileStandardSqlEncoder.
    """

    self._options = options or SqlGenerationOptions()
    # TODO(b/254866189): Determine whether the mock context is enough for
    # validation.
    self._context = context.MockFhirPathContext(structure_definitions)
    self._primitive_handler = handler
    self._bq_interpreter = _bigquery_interpreter.BigQuerySqlInterpreter()

    self._error_reporter = error_reporter
    self._options.skip_keys.update(_SKIP_KEYS)

    self._ctx: List[expressions.Builder] = []
    self._in_progress: Set[str] = set()
    self._element_id_to_regex_map: Dict[str, _RegexInfo] = {}
    self._regex_columns_generated = set()
    # Used to track duplicate requirements.
    self._requirement_column_names: Set[str] = set()

  def _abs_path_invocation(self) -> str:
    """Returns the absolute path invocation given the traversal context."""
    if not self._ctx:
      return ''

    bottom = self._ctx[0]
    root_path = _get_analytic_path(bottom.return_type.root_element_definition)
    path_components = [_last_path_token(s) for s in self._ctx[1:]]
    return '.'.join([root_path] + [c for c in path_components if c])

  def _encode_fhir_path_constraint(
      self, struct_def: _fhir_path_data_types.StructureDataType,
      fhir_path_expression: str,
      node_context: expressions.Builder) -> Optional[str]:
    """Returns a Standard SQL translation of the constraint `fhir_path_expression`.

    If an error is encountered during encoding, the associated error reporter
    will be notified, and this method will return `None`.

    Args:
      struct_def: The Structure definition that the fhir_path_expression
        originates from.
      fhir_path_expression: The fluent-style dot-delimited ('.') FHIRPath
        expression that encodes to Standard SQL.
      node_context: The root builder of the fhir_path_expression. May be another
        FHIRPath expression.

    Returns:
      A Standard SQL encoding of the constraint `fhir_path_expression` upon
      successful completion. The SQL will evaluate to a single boolean
      indicating whether the constraint is satisfied.
    """
    if node_context.get_root_builder().fhir_path == node_context.fhir_path:
      node_context = None

    try:
      new_builder = expressions.from_fhir_path_expression(
          fhir_path_expression, self._context, struct_def,
          self._primitive_handler, node_context)
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(),
          f'{node_context}.{fhir_path_expression}',
          str(e),
      )
      return None
    return self._encode_fhir_path_builder_constraint(new_builder)

  def _encode_fhir_path_builder_constraint(
      self, builder: expressions.Builder) -> Optional[str]:
    """Returns a Standard SQL translation of the constraint `fhir_path_expression`.

    If an error is encountered during encoding, the associated error reporter
    will be notified, and this method will return `None`.

    Args:
      builder: Builder containing the information to be encoded to Standard SQL.

    Returns:
      A Standard SQL encoding of the constraint `fhir_path_expression` upon
      successful completion. The SQL will evaluate to a single boolean
      indicating whether the constraint is satisfied.
    """
    try:
      sql_expression = self._bq_interpreter.encode(builder)

    # Delegate all FHIRPath encoding errors to the associated `ErrorReporter`
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(),
          str(builder),
          str(e),
      )
      return None

    return ('(SELECT IFNULL(LOGICAL_AND(result_), TRUE)\n'
            f'FROM UNNEST({sql_expression}) AS result_)')

  def _encode_constraints(
      self,
      builder: expressions.Builder) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of `SqlRequirement`s for FHIRPath constraints.

    Args:
      builder: The builder containing the element to encode constraints for.

    Returns:
      A list of `SqlRequirement`s expressing FHIRPath constraints defined on the
      `element_definition`.
    """
    result: List[validation_pb2.SqlRequirement] = []
    element_definition = builder.return_type.root_element_definition
    constraints: List[Constraint] = (cast(Any, element_definition).constraint)
    for constraint in constraints:
      constraint_key: str = cast(Any, constraint).key.value
      if constraint_key in self._options.skip_keys:
        continue

      # Metadata for the requirement
      fhir_path_expression: str = cast(Any, constraint).expression.value
      # TODO(b/254866189): Support specialized identifiers.
      if '%resource' in fhir_path_expression:
        continue

      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(constraint_key)
      column_name_base: str = _path_to_sql_column_name(element_definition_path)
      column_name = f'{column_name_base}_{constraint_key_column_name}'

      if column_name in self._requirement_column_names:
        self._error_reporter.report_fhir_path_error(
            element_definition_path, fhir_path_expression,
            f'Duplicate FHIRPath requirement: {column_name}.')
        continue

      if cast(Any, constraint).severity.value == 0:
        self._error_reporter.report_fhir_path_error(
            element_definition_path, fhir_path_expression,
            'Constraint severity must be set.')
        continue  # Malformed constraint

      # TODO(b/221470795): Remove this implementation when a better
      # implementation at the FhirPackage level has been added.
      # Replace fhir_path_expression if needed. This functionality is mainly for
      # temporary replacements of invalid expressions defined in the spec while
      # we wait for the spec to be updated.
      if self._options.expr_replace_list:
        for replacement in self._options.expr_replace_list.replacement:
          if ((not replacement.element_path or
               replacement.element_path == element_definition_path) and
              replacement.expression_to_replace == fhir_path_expression):
            fhir_path_expression = replacement.replacement_expression

      # Create Standard SQL expression
      struct_def = cast(_fhir_path_data_types.StructureDataType,
                        builder.get_root_builder().return_type)
      sql_expression = self._encode_fhir_path_constraint(
          struct_def, fhir_path_expression, builder)
      if sql_expression is None:
        continue  # Failure to generate Standard SQL expression

      # Constraint type and severity metadata; default to WARNING
      # TODO(b/199419068): Cleanup validation severity mapping
      type_ = validation_pb2.ValidationType.VALIDATION_TYPE_FHIR_PATH_CONSTRAINT
      severity = cast(Any, constraint).severity
      severity_value_field = severity.DESCRIPTOR.fields_by_name.get('value')
      severity_str = codes.enum_value_descriptor_to_code_string(
          severity_value_field.enum_type.values_by_number[severity.value])
      try:
        validation_severity = validation_pb2.ValidationSeverity.Value(
            f'SEVERITY_{severity_str.upper()}')
      except ValueError:
        self._error_reporter.report_fhir_path_warning(
            element_definition_path, fhir_path_expression,
            f'Unknown validation severity conversion: {severity_str}.')
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
              fhir_path_expression))

      self._requirement_column_names.add(column_name)
      result.append(requirement)

    return result

  # TODO(b/222541838): Handle general cardinality requirements.
  def _encode_required_fields(
      self,
      builder: expressions.Builder) -> List[validation_pb2.SqlRequirement]:
    """Returns `SqlRequirement`s for all required fields in `ElementDefinition`.

    Args:
      builder: The builder containing the element to encode required fields for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      required fields on the element.
    """

    # If this is an extension, we don't want to access its children/fields.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if (isinstance(builder.return_type, _fhir_path_data_types.StructureDataType)
        and cast(_fhir_path_data_types.StructureDataType,
                 builder.return_type).base_type == 'Extension'):
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []
    children = builder.return_type.children()
    for name, child_message in children.items():
      child = cast(Any, child_message)
      if not _is_elem_supported(child):
        continue
      child_builder = builder.__getattr__(name)
      min_size = cast(Any, child).min.value
      max_size = cast(Any, child).max.value
      element_count = child_builder.count()

      query_list = []

      if _fhir_path_data_types.is_collection(
          child_builder.return_type) and max_size.isdigit():
        query_list.append(element_count <= int(max_size))

      if min_size == 1:
        query_list.append(child_builder.exists())
      elif min_size > 0:
        query_list.append(element_count >= min_size)

      if not query_list:
        continue

      constraint_key = f'{name}-cardinality-is-valid'
      description = (f'The length of {name} must be maximum '
                     f'{max_size} and minimum {min_size}.')

      fhir_path_builder = query_list[0]
      for query in query_list[1:]:
        fhir_path_builder = fhir_path_builder & query

      if constraint_key in self._options.skip_keys:
        continue  # Allows users to skip required field constraints.

      # Early-exit if any types overlap with `_SKIP_TYPE_CODES`.
      type_codes = _utils.element_type_codes(child)
      if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
        continue

      required_sql_expression = self._encode_fhir_path_builder_constraint(
          fhir_path_builder)
      if required_sql_expression is None:
        continue  # Failure to generate Standard SQL expression.

      # Create the `SqlRequirement`.
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(
          _path_to_sql_column_name(constraint_key))
      column_name_base: str = _path_to_sql_column_name(element_definition_path)
      column_name = f'{column_name_base}_{constraint_key_column_name}'

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=required_sql_expression,
          severity=(validation_pb2.ValidationSeverity.SEVERITY_ERROR),
          type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
          element_path=element_definition_path,
          description=description,
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_builder.fhir_path,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              fhir_path_builder.fhir_path))
      encoded_requirements.append(requirement)
    return encoded_requirements

  # TODO(b/207690471): Move important ElementDefinition (and other) functions
  # to their respective utility modules and unit test their public facing apis .
  def _get_regex_from_element(
      self, builder: expressions.Builder) -> Optional[_RegexInfo]:
    """Returns the regex of this element_definition if available."""
    element_definition = cast(Any, builder.return_type.root_element_definition)
    type_codes = _utils.element_type_codes(element_definition)

    if not _is_elem_supported(element_definition):
      return None

    if not type_codes:
      return None
    if len(type_codes) > 1:
      raise ValueError('Expected element with only one type code but got: '
                       f'{type_codes}, is this a choice type?')
    current_type_code = type_codes[0]

    element_id: str = element_definition.id.value
    # TODO(b/208620019): Look more into how this section handles multithreading.
    # If we have memoised the regex of this element, then just return it.
    if element_id in self._element_id_to_regex_map:
      return self._element_id_to_regex_map[element_id]

    # Ignore regexes on primitive types that are not represented as strings.
    if (current_type_code == 'positiveInt' or
        current_type_code == 'unsignedInt'):
      return _RegexInfo(regex='', type_code=current_type_code)

    # TODO(b/207018908): Remove this after we figure out a better way to encode
    # primitive regex constraints for id fields.
    # If the current element_definition ends with `.id` and it's type_code is
    # `http://hl7.org/fhirpath/System.String`, then assume it is an `id` type.
    # We only care about ids that are direct children of a resource
    # E.g. `Foo.id` and not `Foo.bar.id`. These ids will have a base path of
    # `Resource.id`.
    base_path: str = element_definition.base.path.value
    if (base_path == 'Resource.id' and
        current_type_code == 'http://hl7.org/fhirpath/System.String'):
      current_type_code = 'id'

    if (_fhir_path_data_types.is_primitive(builder.return_type) and
        current_type_code not in _PRIMITIVES_EXCLUDED_FROM_REGEX_ENCODING):
      primitive_url = _utils.get_absolute_uri_for_structure(current_type_code)

      # If we have not memoised it, then extract it from its
      # `StructureDefinition`.
      if primitive_url.endswith('String'):
        return None
      type_definition = self._context.get_structure_definition(primitive_url)
      regex_value = _get_regex_from_structure(type_definition,
                                              current_type_code)
      if regex_value is None:
        self._error_reporter.report_validation_error(
            self._abs_path_invocation(), 'Unable to find regex pattern for; '
            f'type_code:`{current_type_code}` '
            f'and url:`{primitive_url}` in environment.')
      else:
        # Memoise the regex of this element for quick retrieval
        # later.
        regex_info = _RegexInfo(regex_value, current_type_code)
        self._element_id_to_regex_map[element_id] = regex_info
        return regex_info

    return None

  def _encode_primitive_regexes(
      self,
      builder: expressions.Builder) -> List[validation_pb2.SqlRequirement]:
    """Returns regex `SqlRequirement`s for primitives in `ElementDefinition`.

    This function generates regex `SqlRequirement`s specifically for the direct
    child elements of the given `element_definition`.

    Args:
      builder: The current builder to encode regexes for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      primitive fields on the element that have regexes .
    """

    element_definition_path = self._abs_path_invocation()
    # TODO(b/206986228): Remove this key after we start taking profiles into
    # account when encoding constraints for fields.
    if 'comparator' in element_definition_path.split('.'):
      return []

    if not isinstance(builder.return_type,
                      _fhir_path_data_types.StructureDataType):
      return []

    struct_def = cast(_fhir_path_data_types.StructureDataType,
                      builder.return_type).structure_definition

    # If this is an extension, we don't want to access its children/fields.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if cast(Any, struct_def).type.value == 'Extension':
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []
    children = builder.return_type.children()
    for name, child_message in children.items():
      child = cast(Any, child_message)
      # TODO(b/190679571): Handle choice types, which may have more than one
      # `type.code` value present.
      # If this element is a choice type, a slice (that is not on an extension)
      # or is disabled, then don't encode requirements for it.
      # TODO(b/202564733): Properly handle slices on non-simple extensions.
      if ('[x]' in _get_analytic_path(child) or _is_disabled(child)):
        continue

      if not _is_elem_supported(child):
        continue

      child_builder = builder.__getattr__(name)
      primitive_regex_info = self._get_regex_from_element(child_builder)
      if primitive_regex_info is None:
        continue  # Unable to find primitive regexes for this child element.

      primitive_regex = primitive_regex_info.regex
      regex_type_code = primitive_regex_info.type_code

      constraint_key = f'{name}-matches-{regex_type_code}-regex'

      if constraint_key in self._options.skip_keys:
        continue  # Allows users to skip specific regex checks.

      # Early-exit if any types overlap with `_SKIP_TYPE_CODES`.
      type_codes = _utils.element_type_codes(child)
      if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
        continue

      # Generate the FHIR path expression that checks regexes, while also
      # accounting for repeated fields, as FHIR doesn't allow function calls to
      # `matches` where the input collection is repeated.
      # More info here:
      # http://hl7.org/fhirpath/index.html#matchesregex-string-boolean.
      element_is_repeated = _utils.is_repeated_element(child)

      fhir_path_builder = child_builder.matches(f'{primitive_regex}')
      if regex_type_code == 'positiveInt':
        fhir_path_builder = child_builder > 0

      if regex_type_code == 'unsignedInt':
        fhir_path_builder = child_builder >= 0

      # Handle special typecode cases, while also accounting for repeated fields
      # , as FHIR doesn't allow direct comparisons involving repeated fields.
      # More info here:
      # http://hl7.org/fhirpath/index.html#comparison.
      if element_is_repeated:
        fhir_path_builder = child_builder.all(fhir_path_builder)

      required_sql_expression = self._encode_fhir_path_builder_constraint(
          fhir_path_builder)
      if required_sql_expression is None:
        continue  # Failure to generate Standard SQL expression.

      # Create the `SqlRequirement`.
      element_definition_path = self._abs_path_invocation()
      constraint_key_column_name: str = _key_to_sql_column_name(
          _path_to_sql_column_name(constraint_key))
      column_name_base: str = _path_to_sql_column_name(
          self._abs_path_invocation())
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
          description=(f'{name} needs to match regex of '
                       f'{regex_type_code}.'),
          fhir_path_key=constraint_key,
          fhir_path_expression=fhir_path_builder.fhir_path,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              _escape_fhir_path_invocation(fhir_path_builder.fhir_path)))
      encoded_requirements.append(requirement)

    return encoded_requirements

  def _encode_element_definition_of_builder(
      self,
      builder: expressions.Builder) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of Standard SQL expressions for an `ElementDefinition`."""
    result: List[validation_pb2.SqlRequirement] = []

    element_definition = builder.return_type.root_element_definition
    if not _is_elem_supported(element_definition):
      return []

    type_codes = _utils.element_type_codes(element_definition)
    if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
      return result  # Early-exit if any types overlap with `_SKIP_TYPE_CODES`

    # `ElementDefinition.base.path` is guaranteed to be present for snapshots
    base_path: str = cast(Any, element_definition).base.path.value
    if base_path in UNSUPPORTED_BASE_PATHS:
      return result  # Early-exit if unsupported `ElementDefinition.base.path`

    # Encode all relevant FHIRPath expression constraints, prior to recursing on
    # children.

    result += self._encode_constraints(builder)
    result += self._encode_required_fields(builder)

    if self._options.add_primitive_regexes:
      result += self._encode_primitive_regexes(builder)

    if isinstance(builder.return_type, _fhir_path_data_types.StructureDataType):
      struct_type = cast(_fhir_path_data_types.StructureDataType,
                         builder.return_type)
      # Ignores the fields inside complex extensions.
      # TODO(b/200575760): Add support for complex extensions and the fields
      # inside them.
      if struct_type.base_type == 'Extension':
        return result

      for child, elem in struct_type.children().items():
        # TODO(b/200575760): Add support for more complicated fields
        if (child == 'extension' or child == 'link' or
            '#' in cast(Any, elem).content_reference.value):
          continue

        new_builder = builder.__getattr__(child)

        # TODO(b/200575760): Add support polymorphic choice types
        if not new_builder.return_type.root_element_definition:
          self._error_reporter.report_validation_error(
              child, 'Root element definition of child is None.')
          # Early-exit if Root element definition of child is None.
          return result

        if isinstance(new_builder.return_type,
                      _fhir_path_data_types.StructureDataType):
          result += self._encode_structure_definition(new_builder)
        else:
          result += self._encode_element_definition_of_builder(new_builder)

    return result

  def _encode_structure_definition(
      self,
      builder: expressions.Builder) -> List[validation_pb2.SqlRequirement]:
    """Recursively encodes the provided resource into Standard SQL."""

    if builder.return_type.url in self._in_progress:
      self._error_reporter.report_conversion_error(
          self._abs_path_invocation(),
          f'Cycle detected when encoding: {builder.return_type.url}.')
      return []
    self._in_progress.add(builder.return_type.url)
    self._ctx.append(builder)  # save the root.

    result = self._encode_element_definition_of_builder(builder)

    _ = self._ctx.pop()
    self._in_progress.remove(builder.return_type.url)
    return result

  def encode(
      self, structure_definition: StructureDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Encodes the provided resource into a list of Standard SQL expressions."""
    result: List[validation_pb2.SqlRequirement] = []
    try:
      # TODO(b/254866189): Support Extension types.
      if cast(Any, structure_definition).type.value == 'Extension':
        return []
      # Call into our protected recursive-helper method to encode the provided
      # `StructureDefinition`. Propagate any exceptions that occur, and always
      # cleanup state prior to returning.
      struct_def_type = _fhir_path_data_types.StructureDataType(
          structure_definition)

      builder = expressions.Builder(
          _evaluation.RootMessageNode(self._context, struct_def_type),
          self._primitive_handler)
      result = self._encode_structure_definition(builder)
      # Removes duplicates (Same SQL Expression) from our list of requirements.
      result = list({
          requirement.sql_expression: requirement for requirement in result
      }.values())
    finally:
      self._ctx.clear()
      self._in_progress.clear()
      self._requirement_column_names.clear()
      self._element_id_to_regex_map.clear()
      self._regex_columns_generated.clear()
    return result


def _fields_referenced_by_expression(
    fhir_path_expression: str) -> Collection[str]:
  """Finds paths for fields referenced by the given expression.

  For example, an expression like 'a.b.where(c > d.e)' references fields
  ['a.b', 'c, 'd.e']

  Args:
    fhir_path_expression: The expression to search for field paths.

  Returns:
    A collection of paths for fields referenced in the given expression.
  """
  # Sort the results so they are consistently ordered for the golden tests.
  # TODO(b/254866189): Change this to traversal over the builder.
  return sorted(
      _ast.paths_referenced_by(
          _ast.build_fhir_path_ast(
              _escape_fhir_path_invocation(fhir_path_expression))))

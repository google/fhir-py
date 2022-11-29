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

import copy
import dataclasses
from typing import Any, Collection, List, Optional, Set, cast

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


def _path_to_sql_column_name(path: str) -> str:
  """Given a path to an `ElementDefinition`, returns a SQL column name."""
  return path.lower().replace('.', '_')


def _key_to_sql_column_name(key: str) -> str:
  """Given a constraint key, returns a SQL column name."""
  return key.lower().replace('-', '_')


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
      *,
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
    # Used to track duplicate requirements.
    self._requirement_column_names: Set[str] = set()

  def _abs_path_invocation(self) -> str:
    """Returns the absolute path invocation given the traversal context."""
    if not self._ctx:
      return ''

    bottom = self._ctx[0]
    root_path = _get_analytic_path(bottom.return_type.root_element_definition)
    path_components = [
        _last_path_token(s.return_type.root_element_definition)
        for s in self._ctx[1:]
    ]
    return '.'.join([root_path] + [c for c in path_components if c])

  def _encode_fhir_path_constraint(
      self, struct_def: _fhir_path_data_types.StructureDataType,
      fhir_path_expression: str) -> Optional[str]:
    """Returns a Standard SQL translation of the constraint `fhir_path_expression`.

    If an error is encountered during encoding, the associated error reporter
    will be notified, and this method will return `None`.

    Args:
      struct_def: The Structure definition that the fhir_path_expression
        originates from.
      fhir_path_expression: The fluent-style dot-delimited ('.') FHIRPath
        expression that encodes to Standard SQL.

    Returns:
      A Standard SQL encoding of the constraint `fhir_path_expression` upon
      successful completion. The SQL will evaluate to a single boolean
      indicating whether the constraint is satisfied.
    """
    new_builder = expressions.from_fhir_path_expression(fhir_path_expression,
                                                        self._context,
                                                        struct_def,
                                                        self._primitive_handler)
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

    # TODO(b/254866189): Add support for non-root level constraints.
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
          struct_def, fhir_path_expression)
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
      # This allows us to encode required fields on slices of extensions while
      # filtering out slices on non-extensions.
      # TODO(b/202564733): Properly handle slices that are not slices on
      # extensions.
      if (_utils.is_slice_element(child) and
          not _utils.is_slice_on_extension(child)):
        continue

      child_builder = builder.__getattr__(name)
      min_size = cast(Any, child).min.value
      max_size = cast(Any, child).max.value
      relative_path = _last_path_token(child)
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

      constraint_key = f'{relative_path}-cardinality-is-valid'
      description = (f'The length of {relative_path} must be maximum '
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
          fhir_path_expression=str(fhir_path_builder),
          fields_referenced_by_expression=_fields_referenced_by_expression(
              str(fhir_path_builder)))
      encoded_requirements.append(requirement)
    return encoded_requirements

  def _encode_element_definition_of_builder(
      self,
      builder: expressions.Builder) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of Standard SQL expressions for an `ElementDefinition`."""
    if isinstance(builder.return_type, _fhir_path_data_types.StructureDataType):
      if builder.return_type.url in self._in_progress:
        self._error_reporter.report_conversion_error(
            self._abs_path_invocation(),
            f'Cycle detected when encoding: {builder.return_type.url}.')
        return []
      self._in_progress.add(builder.return_type.url)

    result: List[validation_pb2.SqlRequirement] = []

    element_definition = builder.return_type.root_element_definition
    type_codes = _utils.element_type_codes(element_definition)
    if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
      return result  # Early-exit if any types overlap with `_SKIP_TYPE_CODES`

    # `ElementDefinition.base.path` is guaranteed to be present for snapshots
    base_path: str = cast(Any, element_definition).base.path.value
    if base_path in UNSUPPORTED_BASE_PATHS:
      return result  # Early-exit if unsupported `ElementDefinition.base.path`

    self._ctx.append(copy.deepcopy(builder))  # save the root.
    # Encode all relevant FHIRPath expression constraints, prior to recursing on
    # children.
    result += self._encode_constraints(builder)
    result += self._encode_required_fields(builder)

    # Ignores the fields inside complex extensions.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if isinstance(builder.return_type, _fhir_path_data_types.StructureDataType):
      struct_type = cast(_fhir_path_data_types.StructureDataType,
                         builder.return_type)
      for child in struct_type.children().keys():
        new_builder = builder.__getattr__(child)
        result += self._encode_element_definition_of_builder(new_builder)
      self._in_progress.remove(struct_type.url)

    _ = self._ctx.pop()
    return result

  def _encode(
      self, struct_def_type: _fhir_path_data_types.StructureDataType
  ) -> List[validation_pb2.SqlRequirement]:
    """Recursively encodes the provided resource into Standard SQL."""
    builder = expressions.Builder(
        _evaluation.RootMessageNode(self._context, struct_def_type),
        self._primitive_handler)
    result = self._encode_element_definition_of_builder(builder)

    # Removes duplicates (Same SQL Expression) from our list of requirements.
    result = list({
        requirement.sql_expression: requirement for requirement in result
    }.values())
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
      struct_def_type = _fhir_path_data_types.StructureDataType(
          structure_definition)
      result = self._encode(struct_def_type)
    finally:
      self._ctx.clear()
      self._in_progress.clear()
      self._requirement_column_names.clear()

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
      _ast.paths_referenced_by(_ast.build_fhir_path_ast(fhir_path_expression)))

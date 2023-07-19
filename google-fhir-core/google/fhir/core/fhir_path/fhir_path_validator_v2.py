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
import functools
import itertools
import operator
import re
import traceback
from typing import Any, Collection, Iterable, List, Optional, Set, Tuple, cast, Dict

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
from google.fhir.core.utils import fhir_package
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
    # TODO(b/271314399): Handle complex types like SimpleQuantity
    'rng-2',
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


@dataclasses.dataclass
class _BuilderSql:
  """A named tuple with sql generated from the fhir expression."""

  sql: str
  fhir_path_sql: str
  builder: expressions.Builder


@dataclasses.dataclass(frozen=True)
class _PathStep:
  """A step along a step1.step2.step3... FHIRPath expression."""

  field: str
  type_url: str


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


def _last_path_token(builder: expressions.Builder) -> str:
  """Returns `builder`'s last path token less the resource type.

  For example:
    * "Foo" returns "" (empty string)
    * "Foo.bar" returns "bar"
    * "Foo.bar.bats" returns "bats"

  Args:
    builder: The `builder` whose relative path to return.
  """
  if isinstance(builder.node, _evaluation.RootMessageNode):
    return ''
  # The last node will be the last path token.
  return builder.node.to_path_token()


_BAD_SQL_CHARACTERS = re.compile(r'(-|\.|:)')


def _path_to_sql_column_name(path: str) -> str:
  """Given a path to an `ElementDefinition`, returns a SQL column name."""
  return _BAD_SQL_CHARACTERS.sub('_', path.lower())


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
      if extension.url.value == 'http://hl7.org/fhir/StructureDefinition/regex':
        # Escape backslashes from regex.
        primitive_regex = extension.value.string_value.value
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


def _is_elem_supported(element_definition: ElementDefinition) -> bool:
  """Returns whether the current element is supported by the validator."""
  # This allows us to encode required fields on slices of extensions while
  # filtering out slices on non-extensions.
  # TODO(b/202564733): Properly handle slices that are not slices on
  # extensions.
  if _utils.is_slice_element(
      element_definition
  ) and not _utils.is_slice_on_extension(element_definition):
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
    value_set_codes_definitions: A package manager containing value set
      definitions which can be used to build SQL for memberOf expressions. These
      value set definitions can be consulted in favor of using an external
      `value_set_codes_table`.
    verbose_error_reporting: If False, the error report will contain the
      exception message associated with the error. If True, it will contain the
      full stack trace for the exception.
  """

  skip_keys: Set[str] = dataclasses.field(default_factory=set)
  add_primitive_regexes: bool = False
  expr_replace_list: fhirpath_replacement_list_pb2.FHIRPathReplacementList = (
      dataclasses.field(
          default_factory=fhirpath_replacement_list_pb2.FHIRPathReplacementList
      )
  )
  add_value_set_bindings: bool = False
  value_set_codes_table: Optional[bigquery.TableReference] = None
  # TODO(b/269329295): collapse these definitions with the definitions
  # passed to FhirPathStandardSqlEncoder.__init__ in a single package
  # manager.
  value_set_codes_definitions: Optional[fhir_package.FhirPackageManager] = None
  verbose_error_reporting: bool = False


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
  satisfied, and the Standard SQL expression translations will produce `NULL` at
  runtime.

  All direct and transitory FHIRPath constraint Standard SQL expression
  encodings are returned as a list by the outer recursive walk over each profle.
  The caller can then join them into a `SELECT` clause, or perform further
  manipulation.
  """

  def __init__(
      self,
      definitions: fhir_package.FhirPackageManager,
      handler: primitive_handler.PrimitiveHandler,
      error_reporter: fhir_errors.ErrorReporter,
      options: Optional[SqlGenerationOptions] = None,
  ) -> None:
    """Creates a new instance of `FhirProfileStandardSqlEncoder`.

    Args:
      definitions: The FHIR resource "graph" for traversal and encoding of
        constraints.
      handler: Computes primitives with respect to the specification.
      error_reporter: A `fhir_errors.ErrorReporter` delegate for error-handling.
      options: Defines a list of optional settings that can be used to customize
        the behaviour of FhirProfileStandardSqlEncoder.
    """

    self._options = options or SqlGenerationOptions()
    # TODO(b/254866189): Determine whether the mock context is enough for
    # validation.
    self._context = context.MockFhirPathContext(
        definitions.iter_structure_definitions()
    )
    self._primitive_handler = handler
    self._bq_interpreter = _bigquery_interpreter.BigQuerySqlInterpreter(
        value_set_codes_table=self._options.value_set_codes_table,
        value_set_codes_definitions=(
            self._options.value_set_codes_definitions or definitions
        ),
    )

    self._error_reporter = error_reporter
    self._options.skip_keys.update(_SKIP_KEYS)

    self._ctx: List[expressions.Builder] = []
    self._in_progress: Set[_PathStep] = set()
    self._element_id_to_regex_map: Dict[str, _RegexInfo] = {}
    self._regex_columns_generated = set()
    # Used to track duplicate requirements.
    self._requirement_column_names: Set[str] = set()
    # Used to avoid visiting the same element definitions multiple times.
    self._visited_element_definitions: Set[Tuple[str, str]] = set()
    # Likewise for slices.
    self._visited_slices: Set[Tuple[str, str]] = set()

  def _get_new_child_builder(
      self, builder: expressions.Builder, path: str
  ) -> Optional[expressions.Builder]:
    """Creates a new builder by following `path` from `builder`."""
    child_builder = builder
    for path_element in path.split('.'):
      try:
        child_builder = child_builder.__getattr__(path_element)
      except (AttributeError, ValueError) as e:
        self._error_reporter.report_fhir_path_error(
            self._abs_path_invocation(builder),
            f'{child_builder}.{path_element}',
            str(e),
        )
        return None

    return child_builder

  def _translate_fhir_path_expression(
      self, builder: expressions.Builder
  ) -> Tuple[Optional[str], Optional[str]]:
    """Returns a tuple containing both the SQL translation of a FHIRPath expression with array wrapping and the SQL translation without array wrapping.

    If an error is encountered during encoding, the associated error reporter
    will be notified, and this method will return [`None`, `None`].

    Args:
      builder: Builder containing the information to be encoded to Standard SQL.

    Returns:
      A tuple (expression, expression_as_array) where `expression` is the SQL
      translation of the FHIRPath expression without array wrapping and
      `expression_as_array` is the SQL translation with array wrapping.
    """
    try:
      result = self._bq_interpreter.visit(
          builder.node, use_resource_alias=False
      )
      expression = f'{result.as_operand()}'
      expression_as_array = (
          f'ARRAY(SELECT {result.sql_alias}\n'
          f'FROM {result.to_subquery()}\n'
          f'WHERE {result.sql_alias} IS NOT NULL)'
      )
      return expression, expression_as_array
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(builder),
          str(builder),
          self._error_message_for_exception(e),
      )
      return None, None

  def _abs_path_invocation(self, builder: expressions.Builder) -> str:
    """Returns the absolute path invocation given the traversal context."""
    if not builder:
      return ''

    if not self._ctx:
      return builder.fhir_path

    bottom = self._ctx[0]
    if bottom.fhir_path == builder.fhir_path:
      return bottom.fhir_path
    return '.'.join([bottom.fhir_path, builder.fhir_path])

  def _encode_fhir_path_constraint(
      self,
      struct_def: _fhir_path_data_types.StructureDataType,
      fhir_path_expression: str,
      node_context: expressions.Builder,
  ) -> Optional[_BuilderSql]:
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
      indicating whether the constraint is satisfied and the builder that
      created it. May be different from the input builder(s).
    """
    if node_context.get_root_builder().fhir_path == node_context.fhir_path:
      node_context = None

    try:
      new_builder = expressions.from_fhir_path_expression(
          fhir_path_expression,
          self._context,
          struct_def,
          self._primitive_handler,
          node_context,
      )
    except Exception as e:  # pylint: disable=broad-except
      self._error_reporter.report_fhir_path_error(
          self._abs_path_invocation(node_context),
          f'{node_context}.{fhir_path_expression}',
          self._error_message_for_exception(e),
      )
      return None
    return self._encode_fhir_path_builder_constraint(new_builder, node_context)

  def _encode_fhir_path_builder(
      self, builder: expressions.Builder
  ) -> Optional[str]:
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
          self._abs_path_invocation(builder),
          str(builder),
          self._error_message_for_exception(e),
      )
      return None
    return sql_expression

  def _error_message_for_exception(self, exc: Exception) -> str:
    """Renders the given exception as a string for use in error reporting."""
    if self._options.verbose_error_reporting:
      return ''.join(
          traceback.format_exception(type(exc), value=exc, tb=exc.__traceback__)
      )

    return str(exc)

  def _encode_fhir_path_builder_constraint(
      self,
      builder: expressions.Builder,
      top_level_constraint: Optional[expressions.Builder],
  ) -> Optional[_BuilderSql]:
    """Returns a Standard SQL translation of the constraint `fhir_path_expression` relative to its top-level constraint.

    Args:
      builder: Builder containing the information to be encoded to Standard SQL.
      top_level_constraint: Builder containing the constraint that the input
        builder is tied to.

    Returns:
      A Standard SQL encoding of the constraint `fhir_path_expression` upon
      successful completion. The SQL will evaluate to a single boolean
      indicating whether the constraint is satisfied and the builder that
      created it. May be different from the input builder(s).
    """
    # If a top-level constraint is not provided, simply return `sql_expression`.
    # Otherwise, we need to add a supporting context query. This is
    # accomplished by a separate call to the bigquery interpreter,
    # passing a synthetic FHIRPath builder that has replaced the top-level
    # constraint with a dummy node. This is because the supporting context query
    # checks if the top-level constraint exists. If it does not exist, then the
    # query should still return TRUE even if there is a constraint on the
    # absolute constraint.
    # We determine if this is a top-level constraint by checking if
    # the builder contains additional invocations beyond what the context
    # contains.

    if not top_level_constraint or isinstance(
        top_level_constraint.node, _evaluation.RootMessageNode
    ):
      fhir_path_expression_sql, sql_expression = (
          self._translate_fhir_path_expression(builder)
      )
      if sql_expression and fhir_path_expression_sql:
        return _BuilderSql(
            (
                '(SELECT IFNULL(LOGICAL_AND(result_), TRUE)\n'
                f'FROM UNNEST({sql_expression}) AS result_)'
            ),
            fhir_path_expression_sql,
            builder,
        )
      return None

    root_sql_expression = self._encode_fhir_path_builder(top_level_constraint)
    relative_builder = expressions.Builder.replace_with_operand(
        builder,
        old_path=top_level_constraint.fhir_path,
        replacement_node=_evaluation.StructureBaseNode(
            self._context, top_level_constraint.return_type
        ),
    )

    fhir_path_expression_sql, sql_expression = (
        self._translate_fhir_path_expression(relative_builder)
    )

    if (
        not sql_expression
        or not root_sql_expression
        or not fhir_path_expression_sql
    ):
      return None
    # Bind the two expressions together via a correlated `ARRAY` subquery
    return _BuilderSql(
        (
            '(SELECT IFNULL(LOGICAL_AND(result_), TRUE)\n'
            f'FROM (SELECT {sql_expression} AS subquery_\n'
            'FROM (SELECT AS VALUE ctx_element_\n'
            f'FROM UNNEST({root_sql_expression}) AS ctx_element_)),\n'
            'UNNEST(subquery_) AS result_)'
        ),
        fhir_path_expression_sql,
        relative_builder,
    )

  def _encode_constraints(
      self, builder: expressions.Builder, element_definition: ElementDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of `SqlRequirement`s for FHIRPath constraints.

    Args:
      builder: The builder containing the element to encode constraints for.
      element_definition: Element definition passed from the parent.

    Returns:
      A list of `SqlRequirement`s expressing FHIRPath constraints defined on the
      `element_definition` and `builder` if applicable.
    """
    result: List[validation_pb2.SqlRequirement] = []
    constraints: List[Constraint] = cast(Any, element_definition).constraint
    root_constraints: List[Constraint] = []
    if isinstance(builder.return_type, _fhir_path_data_types.StructureDataType):
      root_constraints = cast(
          Any, builder.return_type.root_element_definition
      ).constraint
    dedup_constraint_keys: Set[str] = set()

    for constraint in itertools.chain(constraints, root_constraints):
      constraint_key: str = cast(Any, constraint).key.value

      # Constraints from the builder and the parent might overlap but shouldn't
      # be an error so we continue the loop.
      if constraint_key in dedup_constraint_keys:
        continue
      dedup_constraint_keys.add(constraint_key)

      if constraint_key in self._options.skip_keys:
        continue

      # Metadata for the requirement
      fhir_path_expression: str = cast(Any, constraint).expression.value
      # TODO(b/254866189): Support specialized identifiers.
      # TODO(b/271314399): Handle complex types like SimpleQuantity with its own
      # comparison operators.
      if (
          '%resource' in fhir_path_expression
          or 'comparator' in fhir_path_expression
      ):
        continue

      element_definition_path = self._abs_path_invocation(builder)
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
      struct_def = cast(
          _fhir_path_data_types.StructureDataType,
          builder.get_root_builder().return_type,
      )
      result_constraint = self._encode_fhir_path_constraint(
          struct_def, fhir_path_expression, builder
      )
      if result_constraint is None:
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
          sql_expression=result_constraint.sql,
          fhir_path_sql_expression=result_constraint.fhir_path_sql,
          severity=validation_severity,
          type=type_,
          element_path=element_definition_path,
          description=cast(Any, constraint).human.value,
          fhir_path_key=constraint_key,
          fhir_path_expression=result_constraint.builder.fhir_path,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              result_constraint.builder.fhir_path
          ),
      )

      self._requirement_column_names.add(column_name)
      result.append(requirement)

    return result

  # TODO(b/222541838): Handle general cardinality requirements.
  def _encode_required_fields(
      self, builder: expressions.Builder
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns `SqlRequirement`s for all required fields in `ElementDefinition`.

    Args:
      builder: The builder containing the element to encode required fields for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      required fields on the element.
    """

    if not isinstance(
        builder.return_type, _fhir_path_data_types.StructureDataType
    ):
      return []

    # If this is an extension, we don't want to access its children/fields.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if builder.return_type.element_type == 'Extension':
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []

    # Sometimes a struct_def can specify requirements for their
    # descendants, so we look through all descendant element
    # definitions, not just the direct children.
    for name, desc_message in builder.return_type.iter_all_descendants():
      containing_type_builder = builder
      child_builder = containing_type_builder
      paths = name.split('.')
      for path in paths:
        if isinstance(
            child_builder.return_type,
            _fhir_path_data_types.StructureDataType,
        ):
          containing_type_builder = child_builder

        child_builder = self._get_new_child_builder(child_builder, path)
        if not child_builder:
          break

      if not child_builder:
        continue
      name = paths[-1]
      requirement = self._encode_required_field(
          name, containing_type_builder, child_builder, desc_message
      )
      if requirement:
        encoded_requirements.append(requirement)

    return encoded_requirements

  def _encode_required_field(
      self,
      name: str,
      containing_type_builder: expressions.Builder,
      builder: expressions.Builder,
      element_definition: message.Message,
  ) -> Optional[validation_pb2.SqlRequirement]:
    """Returns `SqlRequirement` for the required field passed.

    Args:
      name: name of the constraint key.
      containing_type_builder: The builder of the Structure definition for the
        required field.
      builder: The builder containing the element to encode required field for.
      element_definition: Element definition of the builder.

    Returns:
      A `SqlRequirement` representing the requirement generated from
      the element.
    """

    element = cast(Any, element_definition)
    if not _is_elem_supported(element):
      return None
    min_size = element.min.value
    max_size = element.max.value
    element_count = builder.count()

    query_list = []

    if (
        _fhir_path_data_types.is_collection(builder.return_type)
        and max_size.isdigit()
    ):
      query_list.append(element_count <= int(max_size))

    if min_size == 1:
      query_list.append(builder.exists())
    elif min_size > 0:
      query_list.append(element_count >= min_size)

    if not query_list:
      return None

    constraint_key = f'{name}-cardinality-is-valid'
    description = (
        f'The length of {name} must be maximum '
        f'{max_size} and minimum {min_size}.'
    )

    fhir_path_builder = query_list[0]
    for query in query_list[1:]:
      fhir_path_builder = fhir_path_builder & query

    if constraint_key in self._options.skip_keys:
      return None  # Allows users to skip required field constraints.

    # Early-exit if any types overlap with `_SKIP_TYPE_CODES`.
    type_codes = _utils.element_type_codes(element)
    if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
      return None

    result = self._encode_fhir_path_builder_constraint(
        fhir_path_builder, containing_type_builder
    )
    if result is None:
      return None  # Failure to generate Standard SQL expression.

    # Create the `SqlRequirement`.
    element_definition_path = self._abs_path_invocation(containing_type_builder)
    constraint_key_column_name: str = _key_to_sql_column_name(
        _path_to_sql_column_name(constraint_key)
    )
    column_name_base: str = _path_to_sql_column_name(element_definition_path)
    column_name = f'{column_name_base}_{constraint_key_column_name}'

    requirement = validation_pb2.SqlRequirement(
        column_name=column_name,
        sql_expression=result.sql,
        fhir_path_sql_expression=result.fhir_path_sql,
        severity=(validation_pb2.ValidationSeverity.SEVERITY_ERROR),
        type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
        element_path=element_definition_path,
        description=description,
        fhir_path_key=constraint_key,
        fhir_path_expression=result.builder.fhir_path,
        fields_referenced_by_expression=_fields_referenced_by_expression(
            result.builder.fhir_path
        ),
    )
    return requirement

  def _encode_choice_type_exclusivity(
      self, builder: expressions.Builder
  ) -> List[validation_pb2.SqlRequirement]:
    """Encodes a constraint ensuring the choice type has only one value set.

    If `builder` represents a choice type, encodes SQL ensuring that at most one
    of the columns representing that choice type's possible data types is not
    null.

    Args:
      builder: The builder representing a path to a choice type.

    Returns:
      An empty sequence if `builder` is not a path to a choice type or the
      constraint can not be encoded for other reasons. Otherwise, a sequence
      containing a single `SqlRequirement` for the choice type.
    """
    # Ensure this is a choice type.
    if not builder.return_type.returns_polymorphic():
      return []

    field_name = _last_path_token(builder)
    constraint_key = f'{field_name}-choice-type-exclusivity'
    if constraint_key in self._options.skip_keys:
      return []

    # A choice type should have at least one choice, but if it doesn't
    # there's no constraint to impose.
    type_codes = _utils.element_type_codes(
        builder.return_type.root_element_definition
    )
    if len(type_codes) <= 1:
      return []

    # Ensure only one of the choice types exist.
    num_choices_exist: expressions.Builder = _num_fields_exist(
        builder.ofType(choice_field) for choice_field in type_codes
    )
    exclusivity_constraint: expressions.Builder = num_choices_exist <= 1
    parent_builder = builder.get_parent_builder()

    result = self._encode_fhir_path_builder_constraint(
        exclusivity_constraint, parent_builder
    )
    if result is None:
      return []

    choice_type_path = self._abs_path_invocation(builder)
    column_name = _path_to_sql_column_name(choice_type_path)
    parent_path = self._abs_path_invocation(parent_builder)
    description = (
        f'Choice type {choice_type_path} has more than one of'
        ' its possible choice data types set.'
    )
    return [
        validation_pb2.SqlRequirement(
            column_name=column_name,
            sql_expression=result.sql,
            fhir_path_sql_expression=result.fhir_path_sql,
            severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
            type=validation_pb2.ValidationType.VALIDATION_TYPE_CHOICE_TYPE,
            element_path=parent_path,
            description=description,
            fhir_path_key=constraint_key,
            fhir_path_expression=result.builder.fhir_path,
            fields_referenced_by_expression=_fields_referenced_by_expression(
                result.builder.fhir_path
            ),
        )
    ]

  def _encode_slice_definition(
      self,
      root_builder: expressions.Builder,
      slice_: _fhir_path_data_types.Slice,
  ) -> List[validation_pb2.SqlRequirement]:
    """Encodes constraints for slices.

    Args:
      root_builder: The builder representing a path to the structure definition
        defining the slice.
      slice_: A slice defined by the structure definition at `root_builder`.

    Returns:
      A constraint enforcing the cardinality of `slice_` if `slice_` imposes a
      non-zero or non-* min or max cardinality. Otherwise, an empty list.
    """
    if slice_.relative_path:
      slice_builder = self._get_new_child_builder(
          root_builder, slice_.relative_path
      )
    else:
      slice_builder = root_builder

    if slice_builder is None:
      return []

    # Find any fixed values set by the slice's element definitions.
    element_constraints = []
    for rule_path, rule_def in slice_.slice_rules:
      constraint = self._constraint_from_slice_element(
          root_builder, rule_path, rule_def
      )
      if constraint is not None:
        element_constraints.append(constraint)

    # If the slice has no fixed values, there's no constraint to build.
    if not element_constraints:
      return []

    # 'and' together each of the fixed value constraints.
    element_predicate = functools.reduce(operator.and_, element_constraints)
    elements_in_slice = slice_builder.where(element_predicate)

    # Ensure the number of elements in the slice is between min and max.
    slice_constraints = []

    min_size = cast(Any, slice_.slice_def).min.value
    if min_size == 1:
      slice_constraints.append(elements_in_slice.exists())
    elif min_size > 1:
      slice_constraints.append(elements_in_slice.count() >= min_size)

    # max_size may be '*' or the string representation of an integer.
    max_size = cast(Any, slice_.slice_def).max.value
    if max_size.isdigit():
      slice_constraints.append(elements_in_slice.count() <= int(max_size))

    # If neither min nor max size are set, there's no constraint to build.
    if not slice_constraints:
      return []

    # 'and' together the min and max size constraints.
    slice_constraint = functools.reduce(operator.and_, slice_constraints)
    constraint_sql = self._encode_fhir_path_builder_constraint(
        slice_constraint, root_builder
    )
    if constraint_sql is None:
      return []

    slice_id = cast(Any, slice_.slice_def).id.value
    slice_path = self._abs_path_invocation(root_builder)
    slice_name = cast(Any, slice_.slice_def).slice_name.value
    column_name = (
        f'{_path_to_sql_column_name(slice_path)}'
        f'_{_path_to_sql_column_name(slice_id)}'
        '_slice_cardinality'
    )
    description = (
        f'Slice {slice_id} requires at least {min_size} and at most'
        f' {max_size} elements in {slice_builder} to conform to slice'
        f' {slice_name}.'
    )
    return [
        validation_pb2.SqlRequirement(
            column_name=column_name,
            sql_expression=constraint_sql.sql,
            fhir_path_sql_expression=constraint_sql.fhir_path_sql,
            severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
            type=validation_pb2.ValidationType.VALIDATION_TYPE_CARDINALITY,
            element_path=slice_path,
            description=description,
            fhir_path_key=column_name.replace('_', '-'),
            fhir_path_expression=slice_constraint.fhir_path,
            fields_referenced_by_expression=_fields_referenced_by_expression(
                slice_constraint.fhir_path
            ),
        )
    ]

  def _constraint_from_slice_element(
      self,
      root_builder: expressions.Builder,
      rule_path: str,
      rule_def: ElementDefinition,
  ) -> Optional[expressions.Builder]:
    """Creates a constraint for the component slice element definition.

    If the element definition contains a 'fixed' value, stating that members of
    the slice must have a value for that field with a given value, returns an
    expression stating that the element definition's field must be that fixed
    value.

    Args:
      root_builder: The builder representing a path to the structure definition
        defining the slice.
      rule_path: The path to the element definition `rule_def` relative to
        `root_builder`.
      rule_def: An element definition representing one rule describing slice
        membership.

    Returns:
      An expression stating the constraint or None if the `rule_def` does not
      contain a constraint.
    """
    rule_def = cast(Any, rule_def)

    # If the slice isn't fixing a value, there's no constraint to generate.
    if not rule_def.HasField('fixed'):
      return None

    rule_builder = self._get_new_child_builder(root_builder, rule_path)
    if rule_builder is None:
      return None

    type_codes = _utils.element_type_codes(rule_def)
    if len(type_codes) > 1:
      self._error_reporter.report_conversion_error(
          self._abs_path_invocation(rule_builder),
          f'Slice {rule_def.id.value} on choice type unsupported.',
      )
      return None

    element_type = type_codes[0]
    if not _fhir_path_data_types.is_type_code_primitive(element_type):
      self._error_reporter.report_fhir_path_warning(
          self._abs_path_invocation(rule_builder),
          rule_def.id.value,
          (
              f'Slice fixing value of type {element_type} not yet supported.'
              ' Only slices fixing primitive types are currently supported.'
          ),
      )
      return None

    fixed_field = _fhir_path_data_types.fixed_field_for_type_code(element_type)
    fixed_value = getattr(rule_def.fixed, fixed_field).value
    expression: expressions.Builder = rule_builder == fixed_value
    return expression

  def _encode_reference_type_constraints(
      self, builder: expressions.Builder, elem: message.Message
  ) -> List[validation_pb2.SqlRequirement]:
    """Generates constraints for reference types.

    Ensures that a reference type only has a value for one of the resourceId
    columns across each of the possible resources the reference can link.

    Args:
      builder: The builder to the reference type for which to encode
        constraints.
      elem: Element definition of the builder.

    Returns:
      A constraint enforcing the above requirements for the given reference
      type.
    """
    field_name = _last_path_token(builder)
    constraint_key = f'{field_name}-resource-type-exclusivity'
    if constraint_key in self._options.skip_keys:
      return []

    element_definition = cast(Any, elem)
    type_codes = _utils.element_type_codes(element_definition)
    if type_codes != ['Reference']:
      return []

    allowed_reference_types = [
        target_profile.value
        for target_profile in element_definition.type[0].target_profile
    ]
    if len(allowed_reference_types) <= 1:
      # If there's only one reference type, there's no exclusivity to enforce.
      return []

    # If there are more than one possible reference types, ensure only
    # one is filled.
    num_references_exist: expressions.Builder = _num_fields_exist(
        builder.idFor(reference_type)
        for reference_type in sorted(allowed_reference_types)
    )

    constraint: expressions.Builder = num_references_exist <= 1

    # If the field is a collection, enforce the constraint over its elements.
    if _fhir_path_data_types.is_collection(builder.return_type):
      constraint: expressions.Builder = builder.all(constraint)

    constraint_sql = self._encode_fhir_path_builder_constraint(
        constraint, builder.get_parent_builder()
    )
    if constraint_sql is None:
      return []

    reference_type_path = self._abs_path_invocation(builder)
    column_name = (
        f'{_path_to_sql_column_name(reference_type_path)}_'
        f'{_key_to_sql_column_name(constraint_key)}'
    )
    parent_path = self._abs_path_invocation(builder.get_parent_builder())
    description = (
        f'Reference type {reference_type_path} links to multiple resources or'
        ' to resources of a type restricted by the profile.'
    )
    return [
        validation_pb2.SqlRequirement(
            column_name=column_name,
            sql_expression=constraint_sql.sql,
            severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
            type=validation_pb2.ValidationType.VALIDATION_TYPE_REFERENCE_TYPE,
            element_path=parent_path,
            description=description,
            fhir_path_key=constraint_key,
            fhir_path_expression=constraint.fhir_path,
            fields_referenced_by_expression=_fields_referenced_by_expression(
                constraint.fhir_path
            ),
        )
    ]

  # TODO(b/207690471): Move important ElementDefinition (and other) functions
  # to their respective utility modules and unit test their public facing apis .
  def _get_regex_from_element(
      self, builder: expressions.Builder, elem: ElementDefinition
  ) -> Optional[_RegexInfo]:
    """Returns the regex of this element_definition if available."""
    element_definition = cast(Any, elem)
    type_codes = _utils.element_type_codes(
        builder.return_type.root_element_definition
    )

    if not type_codes:
      return None
    if len(type_codes) > 1:
      raise ValueError(f'Expected 1 type code, got {type_codes} for {builder}')

    current_type_code = type_codes[0]
    element_id: str = element_definition.id.value
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
    base_path: str = element_definition.base.path.value
    if (
        base_path == 'Resource.id'
        and current_type_code == 'http://hl7.org/fhirpath/System.String'
    ):
      current_type_code = 'id'

    if (
        _fhir_path_data_types.primitive_type_from_type_code(current_type_code)
        and current_type_code not in _PRIMITIVES_EXCLUDED_FROM_REGEX_ENCODING
    ):
      primitive_url = _utils.get_absolute_uri_for_structure(current_type_code)

      # If we have not memoised it, then extract it from its
      # `StructureDefinition`.
      if primitive_url.endswith('String'):
        return None
      type_definition = self._context.get_structure_definition(primitive_url)
      regex_value = _get_regex_from_structure(
          type_definition, current_type_code
      )
      if regex_value is None:
        self._error_reporter.report_validation_error(
            self._abs_path_invocation(builder),
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
      self, builder: expressions.Builder
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns regex `SqlRequirement`s for primitives in `ElementDefinition`.

    This function generates regex `SqlRequirement`s specifically for the direct
    child elements of the given `element_definition`.

    Args:
      builder: The current builder to encode regexes for.

    Returns:
      A list of `SqlRequirement`s representing requirements generated from
      primitive fields on the element that have regexes .
    """
    element_definition_path = self._abs_path_invocation(builder)
    # TODO(b/206986228): Remove this key after we start taking profiles into
    # account when encoding constraints for fields.
    if 'comparator' in element_definition_path.split('.'):
      return []

    if not isinstance(
        builder.return_type, _fhir_path_data_types.StructureDataType
    ):
      return []

    struct_def = cast(
        _fhir_path_data_types.StructureDataType, builder.return_type
    ).structure_definition

    # If this is an extension, we don't want to access its children/fields.
    # TODO(b/200575760): Add support for complex extensions and the fields
    # inside them.
    if cast(Any, struct_def).type.value == 'Extension':
      return []

    encoded_requirements: List[validation_pb2.SqlRequirement] = []
    for name, child_message in builder.return_type.iter_children():
      child = cast(Any, child_message)

      if _is_disabled(child):
        continue

      if not _is_elem_supported(child):
        continue

      child_builder = self._get_new_child_builder(builder, name)
      if not child_builder:
        continue

      # TODO(b/190679571): Handle choice types, which may have more than one
      # `type.code` value present.
      # TODO(b/202564733): Properly handle slices on non-simple extensions.
      if child_builder.return_type.returns_polymorphic():
        self._error_reporter.report_fhir_path_error(
            self._abs_path_invocation(child_builder),
            str(child_builder),
            f'Element `{child_builder}` is a choice type which is not currently'
            ' supported.',
        )
        continue

      primitive_regex_info = self._get_regex_from_element(
          child_builder, child_message
      )

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

      element_definition_path = self._abs_path_invocation(builder)
      result = self._encode_fhir_path_builder_constraint(
          fhir_path_builder, builder
      )
      if result is None:
        continue  # Failure to generate Standard SQL expression.

      # Create the `SqlRequirement`.
      constraint_key_column_name: str = _key_to_sql_column_name(
          _path_to_sql_column_name(constraint_key)
      )
      column_name_base: str = _path_to_sql_column_name(element_definition_path)
      column_name = f'{column_name_base}_{constraint_key_column_name}'
      if column_name in self._regex_columns_generated:
        continue
      self._regex_columns_generated.add(column_name)

      requirement = validation_pb2.SqlRequirement(
          column_name=column_name,
          sql_expression=result.sql,
          fhir_path_sql_expression=result.fhir_path_sql,
          severity=(validation_pb2.ValidationSeverity.SEVERITY_ERROR),
          type=validation_pb2.ValidationType.VALIDATION_TYPE_PRIMITIVE_REGEX,
          element_path=self._abs_path_invocation(builder),
          description=f'{name} needs to match regex of {regex_type_code}.',
          fhir_path_key=constraint_key,
          fhir_path_expression=result.builder.fhir_path,
          fields_referenced_by_expression=_fields_referenced_by_expression(
              _escape_fhir_path_invocation(result.builder.fhir_path)
          ),
      )
      encoded_requirements.append(requirement)

    return encoded_requirements

  def _encode_element_definition_of_builder(
      self,
      builder: expressions.Builder,
      parent_element_definition: ElementDefinition,
  ) -> List[validation_pb2.SqlRequirement]:
    """Returns a list of Standard SQL expressions for an `ElementDefinition`."""
    result: List[validation_pb2.SqlRequirement] = []

    element_definition = builder.return_type.root_element_definition
    if not _is_elem_supported(element_definition):
      return []

    # `ElementDefinition.base.path` is guaranteed to be present for snapshots
    base_path: str = cast(Any, element_definition).base.path.value
    if base_path in UNSUPPORTED_BASE_PATHS:
      return result  # Early-exit if unsupported `ElementDefinition.base.path`

    result += self._encode_reference_type_constraints(
        builder, parent_element_definition
    )

    type_codes = _utils.element_type_codes(element_definition)
    if not _SKIP_TYPE_CODES.isdisjoint(type_codes):
      return result  # Early-exit if any types overlap with `_SKIP_TYPE_CODES`

    # Encode all relevant FHIRPath expression constraints, prior to recursing on
    # children.

    result += self._encode_constraints(builder, parent_element_definition)
    result += self._encode_required_fields(builder)
    result += self._encode_choice_type_exclusivity(builder)

    if self._options.add_primitive_regexes:
      result += self._encode_primitive_regexes(builder)

    if self._options.add_value_set_bindings:
      result += self._encode_value_set_bindings(
          builder, parent_element_definition
      )

    if isinstance(builder.return_type, _fhir_path_data_types.StructureDataType):
      struct_type = cast(
          _fhir_path_data_types.StructureDataType, builder.return_type
      )
      # Ignores the fields inside complex extensions.
      # TODO(b/200575760): Add support for complex extensions and the fields
      # inside them.
      if (
          struct_type.element_type == 'Extension'
          or struct_type.element_type == 'Resource'
      ):
        return result

      for child, elem in struct_type.iter_all_descendants():
        # TODO(b/200575760): Add support for more complicated fields
        if (
            child == 'extension'
            or child == 'link'
            or '#' in cast(Any, elem).content_reference.value
        ):
          continue

        new_builder = self._get_new_child_builder(builder, child)
        if not new_builder:
          continue

        # Ensure we don't visit the same element via the same FHIR
        # path multiple times.
        elem_visit = (self._abs_path_invocation(new_builder), elem.id.value)
        if elem_visit in self._visited_element_definitions:
          continue
        self._visited_element_definitions.add(elem_visit)

        # TODO(b/200575760): Add support polymorphic choice types
        if not new_builder.return_type.root_element_definition:
          self._error_reporter.report_validation_error(
              child, 'Root element definition of child is None.'
          )
          # Early-exit if Root element definition of child is None.
          return result

        is_struct_def = isinstance(
            new_builder.return_type, _fhir_path_data_types.StructureDataType
        )
        if is_struct_def:
          result += self._encode_structure_definition(new_builder, elem)
        else:
          result += self._encode_element_definition_of_builder(
              new_builder, elem
          )

      for slice_def in struct_type.iter_slices():
        # Ensure we don't visit the same slice via the same FHIR path
        # multiple times.
        slice_visit = (
            self._abs_path_invocation(new_builder),
            slice_def.slice_def.id.value,
        )
        if slice_visit in self._visited_slices:
          continue
        self._visited_slices.add(slice_visit)

        result += self._encode_slice_definition(builder, slice_def)

    return result

  def _encode_value_set_bindings(
      self, builder: expressions.Builder, element_definition: ElementDefinition
  ) -> List[validation_pb2.SqlRequirement]:
    """Encode .memberOf calls implied by elements bound to value sets."""
    if isinstance(
        builder.return_type, _fhir_path_data_types.PolymorphicDataType
    ):
      return []

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
    relative_path = _last_path_token(builder)
    constraint_key = '%s-memberOf' % relative_path
    if constraint_key in self._options.skip_keys:
      return []

    fhir_path_builder = builder.memberOf(f'{value_set_uri}')
    # fhir_path_builder produces an absolute path, so for constraint generation
    # sake, we build the equivalent relative path.
    relative_fhir_path = "%s.memberOf('%s')" % (
        _escape_fhir_path_invocation(relative_path),
        value_set_uri,
    )

    # Since the binding is required, we don't have to check the top-level
    # constraints.
    result = self._encode_fhir_path_builder_constraint(
        fhir_path_builder, top_level_constraint=None
    )
    if result is None:
      return []

    element_definition_path = self._abs_path_invocation(
        builder.get_parent_builder()
    )
    column_name = _key_to_sql_column_name(
        _path_to_sql_column_name(
            '%s-memberOf' % self._abs_path_invocation(builder)
        )
    )
    description = '%s must be a member of %s' % (
        fhir_path_builder.fhir_path,
        value_set_uri,
    )
    return [
        validation_pb2.SqlRequirement(
            column_name=column_name,
            sql_expression=result.sql,
            fhir_path_sql_expression=result.fhir_path_sql,
            severity=validation_pb2.ValidationSeverity.SEVERITY_ERROR,
            type=(
                validation_pb2.ValidationType.VALIDATION_TYPE_VALUE_SET_BINDING
            ),
            element_path=element_definition_path,
            description=description,
            fhir_path_key=constraint_key,
            fhir_path_expression=relative_fhir_path,
            fields_referenced_by_expression=_fields_referenced_by_expression(
                relative_fhir_path
            ),
        )
    ]

  def _encode_structure_definition(
      self,
      builder: expressions.Builder,
      parent_element_definition: Optional[ElementDefinition] = None,
  ) -> List[validation_pb2.SqlRequirement]:
    """Recursively encodes the provided resource into Standard SQL."""

    path_step = _PathStep(
        field=builder.node.to_path_token(),
        type_url=builder.return_type.url,
    )
    if path_step in self._in_progress:
      # We've hit a recursive data type, e.g. Identifier which
      # contains an optional Reference which contains an optional
      # Identifier. We currently only generate validation SQL to a
      # recursion depth of 1 (e.g. identifier.reference) and do not
      # attempt to validate deeper resources
      # (e.g. identifier.reference.identifier)
      return []
    self._in_progress.add(path_step)
    self._ctx.append(builder)  # save the root.

    if not parent_element_definition:
      parent_element_definition = builder.return_type.root_element_definition
    result = self._encode_element_definition_of_builder(
        builder, parent_element_definition
    )

    self._ctx.pop()
    self._in_progress.remove(path_step)
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
      struct_def_type = _fhir_path_data_types.StructureDataType.from_proto(
          structure_definition
      )

      builder = expressions.Builder(
          _evaluation.RootMessageNode(self._context, struct_def_type),
          self._primitive_handler,
      )
      result = self._encode_structure_definition(builder)
      # Removes duplicates (Same SQL Expression) from our list of requirements.
      result = list(
          {
              requirement.sql_expression: requirement for requirement in result
          }.values()
      )
      # Sort so the results are consistent for diff tests.
      result.sort(key=operator.attrgetter('column_name'))

    finally:
      self._ctx.clear()
      self._in_progress.clear()
      self._requirement_column_names.clear()
      self._visited_element_definitions.clear()
      self._visited_slices.clear()
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
  # TODO(b/254866189): Change this to traversal over the builder.
  return sorted(
      _ast.paths_referenced_by(
          _ast.build_fhir_path_ast(
              _escape_fhir_path_invocation(fhir_path_expression)
          )
      )
  )


def _num_fields_exist(
    fields: Iterable[expressions.Builder],
) -> expressions.Builder:
  return functools.reduce(
      operator.add, (field.exists().toInteger() for field in fields)
  )

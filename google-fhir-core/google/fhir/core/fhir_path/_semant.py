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
"""Utilities and classes for semantic type checking of FHIRPath expressions."""

import copy
import decimal
from typing import Any, cast, Optional

from google.fhir.core import fhir_errors
from google.fhir.core.fhir_path import _ast
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import _fhir_path_to_sql_functions
from google.fhir.core.fhir_path import _navigation
from google.fhir.core.fhir_path import _utils
from google.fhir.core.fhir_path import fhir_path_options

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


def _set_and_return_type(
    expression: _ast.Expression,
    data_type: _fhir_path_data_types.FhirPathDataType
) -> _fhir_path_data_types.FhirPathDataType:
  """Returns the given data_type after setting expression.data_type to it."""
  expression.data_type = data_type
  return expression.data_type


# TODO(b/203231524): Populate `element_path` in error_reporter once underlying
# FHIR implementation graph state is populated/wired-in.
# TODO(b/204211665): Adapt fhir_errors.py to work with _semant.py.
class FhirPathSemanticAnalyzer(_ast.FhirPathAstBaseVisitor):
  """Adds FHIRPath type information to a FHIRPath AST.

  Callers can attach semantic annotations by calling `add_semantic_annotations`.
  Once the call returns, each node of the provided AST will have the field
  `data_type` populated.

  Note that any type inference to a primitive concrete type is implicitly
  "optional". For example, consider the expression:
  ```
  foo.bar or { }
  ```
  where `foo.bar` is a scalar Boolean field. During semantic analysis,
  we do not know what value this will take on. Instead, we see:
  ```
  <_fhir_path_data_types.Boolean> or <_fhir_path_data_types.Empty>
  ```
  Based on the truth-table for `or`, this may be an empty collection (if
  `foo.bar` evaluates to `false` at runtime), or it may be a Boolean value
  (if `foo.bar` evaluates to `true` at runtime). We infer this type as
  `_fhir_path_data_types.Boolean`. In other words, a primitive type `T`
  *may* also be the empty collection.

  Conversely, if something is typed as the empty collection, it means that it
  will *definitely* be an empty collection at runtime. The exception to this is
  in error handling, where we default to the empty collection as a sentinel type
  to continue type inference throughout the entire AST prior to returning.
  """

  def __init__(
      self,
      env: _navigation._Environment,
      validation_options: Optional[
          fhir_path_options.SqlValidationOptions] = None,
  ) -> None:
    """Creates a new FhirPathSemanticAnalyzer.

    Args:
      env: A reference to the underlying `_Environment` to traverse over.
      validation_options: Optional settings for influencing validation behavior.
    """
    self._env = env
    self._error_reporter: Optional[fhir_errors.ErrorReporter] = None
    self.validation_options = validation_options

  def add_semantic_annotations(
      self,
      ast: _ast.Expression,
      error_reporter: fhir_errors.ErrorReporter,
      structure_definition: _navigation.StructureDefinition,
      element_definition: Optional[_navigation.ElementDefinition] = None
  ) -> None:
    """Performs FHIRPath semantic analysis and adds type information to the AST.

    The entire given AST will be annotated with FHIRPath type information
    inferred during static analysis. Any errors encountered during semantic
    analysis will be logged to the `error_reporter`.

    Args:
      ast: The Abstract Syntax Tree to perform semantic analysis on and annotate
        with type information.
      error_reporter: Used to report semantic errors.
      structure_definition: The initial `StructureDefinition`.
      element_definition: The initial `ElementDefinition`. If `None`, the root
        element of `structure_definition` is chosen. Defaults to `None`.

    Raises:
      ValueError: In the event that the provided `input_str` was syntactically
        invalid FHIRPath that failed during lexing/parsing.
    """
    self._error_reporter = error_reporter

    walker = _navigation.FhirStructureDefinitionWalker(
        self._env,
        structure_definition,
        element_definition,
    )
    _ = self.visit(ast, walker=walker)

  def visit_literal(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, literal: _ast.Literal,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    if literal.value is None:
      result = _fhir_path_data_types.Empty
    elif isinstance(literal.value, bool):
      result = _fhir_path_data_types.Boolean
    elif isinstance(literal.value, str):
      if literal.is_date_type:
        if 'T' in literal.value:
          result = _fhir_path_data_types.DateTime
        else:
          result = _fhir_path_data_types.Date
      else:
        result = _fhir_path_data_types.String
    elif isinstance(literal.value, _ast.Quantity):
      result = _fhir_path_data_types.Quantity
    elif isinstance(literal.value, int):
      result = _fhir_path_data_types.Integer
    elif isinstance(literal.value, decimal.Decimal):
      result = _fhir_path_data_types.Decimal
    else:
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis', f'Unsupported literal value: {literal}.')
      result = _fhir_path_data_types.Empty

    return _set_and_return_type(literal, result)

  # TODO(b/190679571): Handle choice types, which may have more than one
  # `type.code` value present.
  def visit_identifier(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, identifier: _ast.Identifier,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:

    # If the identifier is `$this`, then we don't have to advance the
    # message context because `$this` is just a reference to the current
    # identifier.
    if identifier.value != '$this':
      walker.step(str(identifier))

    base_path: str = cast(Any, walker.element).base.path.value
    if base_path in UNSUPPORTED_BASE_PATHS:
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          'Unable to encode constraint that traverses element '
          f'with unsupported `base.path` of: {base_path}.')
      return _set_and_return_type(identifier, _fhir_path_data_types.Empty)

    type_codes = _utils.element_type_codes(walker.element)
    type_code = type_codes[0] if len(type_codes) == 1 else None
    primitive_type = _fhir_path_data_types.primitive_type_from_type_code(
        type_code) if type_code else None

    if primitive_type:
      data_type = primitive_type
    elif walker.current_type:
      # TODO(b/186792939): Use a protocol when structural typing is added.
      # Until then cast to Any to get the structdef url and type fields.
      struct_def = cast(Any, walker.current_type)
      data_type = _fhir_path_data_types.StructureDataType.from_proto(struct_def)
    else:
      data_type = _fhir_path_data_types.Empty

    # If the identifier is $this, then we are referencing individual scalar
    # values of the element. So we don't want to wrap it in a collection.
    if (_utils.is_repeated_element(walker.element) and
        identifier.value != '$this'):
      data_type = _fhir_path_data_types.Collection(types={data_type})
    return _set_and_return_type(identifier, data_type)

  # TODO(b/215533268): Add support in semant.py's visit_indexer for multiple
  # types.
  def visit_indexer(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, indexer: _ast.Indexer,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on `_ast.Indexer`.

    This function returns the datatype of an indexed item in the collection.
    If the subject of the indexer is instead a single item, then it returns its
    datatype.

    If the index used is not the Integer type, it reports an error and returns
    the Empty type.

    Currently only supports indexing collections with a sinlge type. If a
    collection contains multiple types, then it returns the Empty type and
    reports an error to the given error reporter.

    Args:
      indexer: The Indexer operator `[]` that should be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    subject_type = self.visit(indexer.collection, walker=copy.copy(walker))
    index_data_type = self.visit(indexer.index, walker=copy.copy(walker))

    if index_data_type != _fhir_path_data_types.Integer:
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected index argument of indexer to be Integer type, '
          f'but got: {index_data_type}.')
      return _set_and_return_type(indexer, _fhir_path_data_types.Empty)

    if isinstance(subject_type, _fhir_path_data_types.Collection):

      if len(subject_type.types) == 1:
        return _set_and_return_type(indexer, list(subject_type.types)[0])

      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected input collection to have single type, but got:'
          f'... {subject_type.types}.')
      return _set_and_return_type(indexer, _fhir_path_data_types.Empty)

    return _set_and_return_type(indexer, subject_type)

  def visit_arithmetic(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, arithmetic: _ast.Arithmetic,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on `_ast.Arithmetic`.

    This function returns the datatype of the result of this Arithmetic
    operation.

    Args:
      arithmetic: The Arithmetic binary operator (e.g. *, /, +, -, div, mod, &)
        that should be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    lhs = self.visit(arithmetic.lhs, walker=copy.copy(walker))
    rhs = self.visit(arithmetic.rhs, walker=copy.copy(walker))

    # Both arguments must be single-element collections (or empty)
    if (isinstance(lhs, _fhir_path_data_types.Collection) or
        isinstance(rhs, _fhir_path_data_types.Collection)):
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected scalar operands but got: {lhs} {arithmetic.op} {rhs}.')
      return _set_and_return_type(arithmetic, _fhir_path_data_types.Empty)

    # If either operand is an empty collection, the entire expression is empty
    if lhs == _fhir_path_data_types.Empty or rhs == _fhir_path_data_types.Empty:
      return _set_and_return_type(arithmetic, _fhir_path_data_types.Empty)

    # Both LHS and RHS must be the same type or implicitly convertible to the
    # same type, otherwise we must notify the error reporter.
    if _fhir_path_data_types.is_coercible(lhs, rhs):
      type_ = _fhir_path_data_types.coerce(lhs, rhs)
      return _set_and_return_type(arithmetic, type_)

    self._error_reporter.report_fhir_path_error(
        '', 'Semantic Analysis',
        f'Expected operands of compatible type for arithmetic operation but got'
        f': {lhs} {arithmetic.op} {rhs}.')
    return _set_and_return_type(arithmetic, _fhir_path_data_types.Empty)

  # TODO(b/210038841): Add support for non-primitive types such as Quantity.
  def visit_type_expression(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, type_expression: _ast.TypeExpression,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on `_ast.TypeExpression`.

    Right now, we only support operations where the lhs (type_specifier) is a
    string reperesenting a Primitive type:

    E.g. `bar is integer`. In this case, `integer` is a string that represents
    the primitive type `Integer`.

    If the `type_specifier` does not resolve to a primitive type, log an error
    and return the empty type.

    If the operand is `is`, return the `Boolean type`.

    If the operand is `as`, return the datatype specified by the
    `type_specifier`.

    Args:
      type_expression: The Type Expression operator (e.g. is, as) that should be
        type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    type_str = str(type_expression.type_specifier)
    primitive_type = _fhir_path_data_types.primitive_type_from_type_code(
        type_str)

    if primitive_type is None:
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected rhs of type expression to describe a type but got:'
          f'... {type_expression.op} {type_str}.')
      return _set_and_return_type(type_expression, _fhir_path_data_types.Empty)

    if type_expression.op == _ast.TypeExpression.Op.IS:
      return _set_and_return_type(type_expression,
                                  _fhir_path_data_types.Boolean)
    elif type_expression.op == _ast.TypeExpression.Op.AS:
      return _set_and_return_type(type_expression, primitive_type)
    else:
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected operand to be one of `is` or `as`, but got:'
          f'... {type_expression.op} ...')
      return _set_and_return_type(type_expression, _fhir_path_data_types.Empty)

  def visit_equality(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, equality: _ast.EqualityRelation,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on `_ast.EqualityRelation`.

    This function either returns an `Empty type` or `Boolean type`.

    If the operator is the `=` or `!=` sign (Equality) then:
      * If either operand is the `Empty type`, then the result is the
        `Empty type`.

      * If both operands are collections with a single item, they must be of the
        same type (or be implicitly convertible to the same type). If they are
        not of the same type / implicitly convertable to the same type, then
        the result is `Empty type` and an error is logged.

      * Otherwise the result is the `Boolean type`.

    If the operator is the `~` or `!~` sign (Equivalent) then:
      * The result is always the `Boolean type`.

    More information here: http://hl7.org/fhirpath/index.html#equality

    Args:
      equality: The Equality binary operator (e.g. =, !=, ~, !~) that should be
        type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    lhs = self.visit(equality.lhs, walker=copy.copy(walker))
    rhs = self.visit(equality.rhs, walker=copy.copy(walker))

    is_equality = (
        equality.op == _ast.EqualityRelation.Op.EQUAL or
        equality.op == _ast.EqualityRelation.Op.NOT_EQUAL)

    # Handle Equality & Equivalence.
    if (lhs == _fhir_path_data_types.Empty or
        rhs == _fhir_path_data_types.Empty):
      if is_equality:
        return _set_and_return_type(equality, _fhir_path_data_types.Empty)
      else:
        return _set_and_return_type(equality, _fhir_path_data_types.Boolean)

    if _fhir_path_data_types.is_coercible(lhs, rhs):
      return _set_and_return_type(equality, _fhir_path_data_types.Boolean)

    self._error_reporter.report_fhir_path_error(
        '', 'Semantic Analysis',
        f'Expected operands of compatible type but got: {lhs} '
        f'{equality.op} {rhs}.')
    return _set_and_return_type(equality, _fhir_path_data_types.Empty)

  def visit_comparison(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, comparison: _ast.Comparison,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on an instance of `_ast.Comparison`.

    For comparison, the following rules apply:
      * Comparsion operations are defined for strings, integers, decimals,
        quantities, dates, datetimes, and times.

      * If one or both arguments is an empty collection, a comparison operator
        will return an empty collection.

      * Both arguments must be collections with single values.

    See https://hl7.org/fhirpath/#comparison for more details.

    Note that when comparing quantities, the dimensions of each quantity must be
    the same, but not necessarily the unit. Since this will be dependent upon
    the values assigned at runtime in all cases but literals, we infer any
    comparison between quantities as returning a Boolean result. See the class
    docstring for more details.

    Args:
      comparison: The Comparison binary operator (e.g. <, <=, >, >=) that should
        be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    lhs = self.visit(comparison.lhs, walker=copy.copy(walker))
    rhs = self.visit(comparison.rhs, walker=copy.copy(walker))

    # Both arguments must be single-element collections (or empty)
    if (isinstance(lhs, _fhir_path_data_types.Collection) or
        isinstance(rhs, _fhir_path_data_types.Collection)):
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected scalar operands but got: {lhs} {comparison.op} {rhs}.')
      return _set_and_return_type(comparison, _fhir_path_data_types.Empty)

    # If either operand is an empty collection, the entire expression is empty
    if lhs == _fhir_path_data_types.Empty or rhs == _fhir_path_data_types.Empty:
      return _set_and_return_type(comparison, _fhir_path_data_types.Empty)

    # Both LHS and RHS must be the same type or implicitly convertible to the
    # same type, otherwise we must notify the error reporter.
    if _fhir_path_data_types.is_coercible(lhs, rhs):
      # The resulting type must be one of the supported comparable types
      type_ = _fhir_path_data_types.coerce(lhs, rhs)
      if type_.comparable:
        return _set_and_return_type(comparison, _fhir_path_data_types.Boolean)

      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected comparable operands but got: {lhs}'
          f' {comparison.op} {rhs}.')
      return _set_and_return_type(comparison, _fhir_path_data_types.Empty)

    self._error_reporter.report_fhir_path_error(
        '', 'Semantic Analysis',
        f'Expected operands of compatible type but got: {lhs} '
        f' {comparison.op} {rhs}.')
    return _set_and_return_type(comparison, _fhir_path_data_types.Empty)

  def visit_boolean_logic(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, boolean_logic: _ast.BooleanLogic,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on an instance of `_ast.BooleanLogic`.

    For all Boolean operators, the collections passed as operands are first
    evaluated as Booleans as described in:
    https://hl7.org/fhirpath/#singleton-evaluation-of-collections.

    Operands are expected to be single-element collections. Empty operands may
    affect the inferred type based on the operation (e.g. `xor` will collapse to
    the empty collection, if either operand is empty).

    Args:
      boolean_logic: The Boolean logical binary operator (e.g. and, or, implies,
        xor) that should be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    lhs = self.visit(boolean_logic.lhs, walker=copy.copy(walker))
    rhs = self.visit(boolean_logic.rhs, walker=copy.copy(walker))

    # Operands are evaluated per FHIRPath "singleton evaluation of collections".
    # Both operands are expected to be single-element collections (or empty
    # collections).
    # See: https://hl7.org/fhirpath/#singleton-evaluation-of-collections.
    if (isinstance(lhs, _fhir_path_data_types.Collection) or
        isinstance(rhs, _fhir_path_data_types.Collection)):
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected scalar operands but got: {lhs} {boolean_logic.op} {rhs}.')
      return _set_and_return_type(boolean_logic, _fhir_path_data_types.Empty)

    if ((lhs == _fhir_path_data_types.Empty or
         rhs == _fhir_path_data_types.Empty) and
        boolean_logic.op == _ast.BooleanLogic.Op.XOR):
      # XOR is the only operator that we know will evaluate to strictly the
      # empty collection if either operand is empty. Otherwise, the returned
      # type will be either a Boolean *or* the empty collection, based on values
      # at runtime.
      return _set_and_return_type(boolean_logic, _fhir_path_data_types.Empty)

    if (lhs == _fhir_path_data_types.Empty and
        rhs == _fhir_path_data_types.Empty):
      # All binary logical operators resolve to empty if both operands are empty
      return _set_and_return_type(boolean_logic, _fhir_path_data_types.Empty)

    # All operands are of scalar type or empty, and therefore will be evaluated
    # according to the Boolean truth tables outlined at:
    # https://hl7.org/fhirpath/#boolean-logic at runtime.
    return _set_and_return_type(boolean_logic, _fhir_path_data_types.Boolean)

  def visit_membership(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, membership: _ast.MembershipRelation,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on instance of `_ast.MembershipRelation`.

    This includes the `in` and `contains` operators.

    For the `in` operator:
      * If the left operand has multiple items, an exception is thrown.
      * If the left operand is empty, the result is the `Empty type`.
      * Otherwise the result is the `Boolean type`.

    The `contains` operator is the converse of the `in` operator so:
      * If the right operand has multiple items, an exception is thrown.
      * If the right operand is empty, the result is the `Empty type`.
      * Otherwise the result is the `Boolean type`.

    More information here: https://hl7.org/fhirpath/#in-membership

    Args:
      membership: The Membership binary operator (`in` or `contains`) that
        should be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    lhs_type = self.visit(membership.lhs, walker=copy.copy(walker))
    rhs_type = self.visit(membership.rhs, walker=copy.copy(walker))

    operand_to_check, operand_side = ((lhs_type, 'left') if membership.op
                                      == _ast.MembershipRelation.Op.IN else
                                      (rhs_type, 'right'))

    if isinstance(operand_to_check, _fhir_path_data_types.Collection):
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis',
          f'Expected operand with single item for operator `{membership.op}` on'
          f' {operand_side}-hand side, but got Collection: {operand_to_check}.')
      return _set_and_return_type(membership, _fhir_path_data_types.Empty)

    if operand_to_check == _fhir_path_data_types.Empty:
      return _set_and_return_type(membership, _fhir_path_data_types.Empty)

    return _set_and_return_type(membership, _fhir_path_data_types.Boolean)

  def visit_union(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, union: _ast.UnionOp,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on an instance of `_ast.UnionOp`.

    The resultant datatype of UnionOp is the union of the datatypes of the `rhs`
    and `lhs`.

    Args:
      union: The binary union operator (`union`) that should be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    lhs_type = self.visit(union.lhs, walker=copy.copy(walker))
    rhs_type = self.visit(union.rhs, walker=copy.copy(walker))

    # If either side is the `Empty type`, return the other side.
    if (lhs_type == _fhir_path_data_types.Empty or
        rhs_type == _fhir_path_data_types.Empty):
      return _set_and_return_type(
          union,
          (lhs_type if rhs_type == _fhir_path_data_types.Empty else rhs_type))

    # Extract the types of both sides.
    lhs_type_set = (
        set(lhs_type.types)
        if isinstance(lhs_type, _fhir_path_data_types.Collection)
        else {lhs_type}
    )

    rhs_type_set = (
        set(rhs_type.types)
        if isinstance(rhs_type, _fhir_path_data_types.Collection)
        else {rhs_type}
    )

    final_type_set = lhs_type_set.union(rhs_type_set)
    return _set_and_return_type(
        union, _fhir_path_data_types.Collection(types=final_type_set)
    )

  def visit_polarity(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, polarity: _ast.Polarity,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on an instance of `_ast.Polarity`.

    Polarity expects that its operand is a numeric type:
    `_fhir_path_data_types.Integer` or `_fhir_path_data_types.Decimal`.

    Args:
      polarity: The unarity polarity operator (+/-) that should be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    operand = self.visit(polarity.operand, walker=copy.copy(walker))
    if (operand == _fhir_path_data_types.Integer or
        operand == _fhir_path_data_types.Decimal):
      return _set_and_return_type(polarity, operand)

    self._error_reporter.report_fhir_path_error(
        '', 'Semantic Analysis',
        f'Expected numeric operand but got: {polarity.op} {operand}.')
    return _set_and_return_type(polarity, _fhir_path_data_types.Empty)

  def visit_invocation(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self, invocation: _ast.Invocation,
      walker: _navigation.FhirStructureDefinitionWalker
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on an instance of `_ast.Invocation`.

    The resultant datatype of this invocation can be gotten from datatype of
    `invocation.rhs`.

    If the chain of invocations is not supported, it reports an error and
    returns the Empty type.
    Currently, only the following are supported:
    1. <expression>.<function>
    2. <identifier|invocation>.<invocation>

    Args:
      invocation: The Invocation binary operator (`.`) that should be type
        checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.

    Returns:
      The inferred static type of the operation.
    """
    lhs_result = self.visit(invocation.lhs, walker=walker)

    # Ignore semantic type checking if lhs is `Any`.
    condition = (
        lhs_result == _fhir_path_data_types.Any_ or
        (isinstance(lhs_result, _fhir_path_data_types.Collection) and
         list(lhs_result.types) == [_fhir_path_data_types.Any_]))

    if condition:
      return _set_and_return_type(invocation, _fhir_path_data_types.Any_)

    if (isinstance(invocation.lhs, _ast.Expression) and
        isinstance(invocation.rhs, _ast.Function)):
      return _set_and_return_type(
          invocation,
          self.visit_function(invocation.rhs, walker, operand=invocation.lhs))

    # We check if the rhs matches both Identifier and Invocation here
    # (instead of just Invocation) because according to the FHIRPath Grammar
    # an Invocation can be an Identifier.
    #
    # For example in `Foo.bar`, `.bar` is an Invocation that resolves to an
    # Identifier. But because `.bar` doesn't have any more arguments on its RHS
    # it is treated as an Identifier in the _ast.
    #
    # If the this check is true, we return the datatype of the rhs.
    #
    # More info here: http://hl7.org/fhirpath/grammar.html.
    if (isinstance(invocation.lhs, (_ast.Identifier, _ast.Invocation)) and
        isinstance(invocation.rhs, (_ast.Identifier, _ast.Invocation))):
      rhs_result = self.visit(invocation.rhs, walker=walker)

      # If the lhs is a collection, then the rhs should also be a collection
      # (if it is not a collection already).
      if (isinstance(lhs_result, _fhir_path_data_types.Collection) and
          not isinstance(rhs_result, _fhir_path_data_types.Collection)):
        rhs_result = _fhir_path_data_types.Collection(types=(rhs_result,))

      return _set_and_return_type(invocation, rhs_result)

    # TODO(b/193046163): Add support for arbitrary leading expressions.
    self._error_reporter.report_fhir_path_error(
        '', 'Semantic Analysis', f'Unable to resolve {invocation.rhs!r}'
        f' in the context of leading expression: {invocation.lhs!r}.')
    return _set_and_return_type(invocation, _fhir_path_data_types.Empty)

  def visit_function(  # pytype: disable=signature-mismatch  # overriding-parameter-count-checks
      self,
      function: _ast.Function,
      walker: _navigation.FhirStructureDefinitionWalker,
      operand: Optional[_ast.Expression] = None,
  ) -> _fhir_path_data_types.FhirPathDataType:
    """Performs semantic type checking on an instance of `_ast.Function`.

    If the operand is None, then the resultant datatype of `function` is Empty
    type. Otherwise, the resultant datatype of `function` is equivalent to the
    given function's return type.

    The operand (lhs of this function) should have already been semantically
    validated before this function call.

    Args:
      function: The function `Expression` that should be type checked.
      walker: A `FhirStructureDefinitionWalker` for traversing the underlying
        FHIR implementation graph.
      operand: The `Expression` that this function is applied on.

    Returns:
      The inferred static type of the operation.
    """

    function_name = function.identifier.value
    if function_name not in _fhir_path_to_sql_functions.FUNCTION_MAP:
      self._error_reporter.report_fhir_path_error(
          '', 'Semantic Analysis', f'Unsupported function: `{function_name}`.')
      return _set_and_return_type(function, _fhir_path_data_types.Empty)

    if operand is None:
      return _set_and_return_type(function, _fhir_path_data_types.Empty)

    # Perform semantic type checking on the params with a shallow-copy of `ctx`.
    for param in function.params:
      _ = self.visit(param, walker=copy.copy(walker))

    error = (
        _fhir_path_to_sql_functions.FUNCTION_MAP[function_name]
        .validate_and_get_error(
            function,
            operand,
            copy.copy(walker),
            options=self.validation_options))
    if error:
      # Delegate all FHIRPath function errors to the given `ErrorReporter`
      self._error_reporter.report_fhir_path_error(
          '',
          'Semantic Analysis',
          error,
      )
      return _set_and_return_type(function, _fhir_path_data_types.Empty)

    function_data_type = _fhir_path_to_sql_functions.FUNCTION_MAP[
        function_name].return_type(function, operand, walker)
    return _set_and_return_type(function, function_data_type)

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
"""Utilities and classes for manipulating FHIRPath abstract syntax trees."""

import abc
import dataclasses
import decimal
import enum
import sys
from typing import Any, Collection, List, Optional, Tuple, Union
import weakref

import logging
import antlr4
from antlr4.error import ErrorListener

from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path.FhirPathLexer import FhirPathLexer
from google.fhir.core.fhir_path.FhirPathParser import FhirPathParser
from google.fhir.core.fhir_path.FhirPathVisitor import FhirPathVisitor

Number = Union[int, decimal.Decimal]


_StrEnum = (
    (enum.StrEnum,) if sys.version_info[:2] >= (3, 11) else (str, enum.Enum)
)


@dataclasses.dataclass
class Quantity:
  """A FHIRPath Quantity literal."""

  value: Number
  unit: Optional[str] = None

  def __str__(self) -> str:
    return (
        f'{self.value}' if self.unit is None else f"{self.value} '{self.unit}'"
    )


# Python native datatypes for FHIRPath literal values.
LiteralType = Union[None, bool, str, Number, Quantity]


# TODO(b/244184211): Could override property setters and store as a dictionary
# to avoid the extra pointer storage.
class AbstractSyntaxTree(abc.ABC):
  """An abstract syntax tree (AST) base class.

  In an abstract syntax tree (AST), each interior node represents a programming
  construct. Nodes are aware of their children, and their parent, if any exist.
  A node has a strong relationship to its children, and a weak relationship to
  its parent.
  """

  @property
  def has_parent(self) -> bool:
    """Returns `true` if the node has a parent node."""
    return self.parent is not None

  @property
  def children(self) -> Optional[List['AbstractSyntaxTree']]:
    """The list of child nodes, if one exists.

    The returned list of children should be treated as immutable by the caller.
    """
    return self._children

  def __init__(
      self, children: Optional[List['AbstractSyntaxTree']] = None
  ) -> None:
    """Initializes an `AbstractSyntaxTree` with an optional list of children.

    Note that the `parent` property is set for children at the time their parent
    is initialized. It is set as a weak reference to avoid retain cycles.

    Args:
      children: The optional list of children belonging to this node.
    """
    self.data_type: Optional[_fhir_path_data_types.FhirPathDataType] = None
    self.parent = None
    self._children = children
    for c in self._children or []:
      c.parent = weakref.proxy(self)

  def num_children(self) -> int:
    """Returns the number of children of the node."""
    return len(self._children) if self._children is not None else 0

  def has_children(self) -> bool:
    """Returns `true` if the node has child nodes."""
    return self.num_children() > 0

  def add_child(self, c: 'AbstractSyntaxTree') -> None:
    """Adds a child to the node and sets the child's parent to `self`."""
    if self._children is None:
      self._children = []
    self._children.append(c)
    c.parent = weakref.proxy(self)

  def remove_child(self, c: 'AbstractSyntaxTree') -> None:
    """Removes a child from the reciever and sets its parent to `None`.

    Args:
      c: The child to remove. By default, compared using pointer equality.

    Raises:
      ValueError in the event that the child does not being to the underlying
      list of children.
    """
    if self._children is None:
      raise ValueError(f'No children belonging to {self!r}.')
    self._children.remove(c)
    c.parent = None

  @abc.abstractmethod
  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    """Delegates a 'visit' to the `AbstractSyntaxTree` node.

    The provided `visitor` can be used to perform arbitrary operations on the
    visited node. Arbitrary key/value arguments can be leveraged to provide
    additional stateful information.

    Args:
      visitor: A visitor that traverses the FHIRPath abstract syntax tree.
      **kwargs: Arbitrary key/value arguments accompanying the visitor.

    Returns:
      `Any` value that is a byproduct of the visit.
    """

  def debug_string(self, indent: int = 0) -> str:
    """Returns a debug string for the tree rooted at this node.

    Args:
      indent: The level of indentation to begin printing the tree.

    Returns:
      A string representing this node and its descendant nodes.
    """
    self_repr = f'{"| " * indent}{self.__class__.__name__}<{repr(self)}>'
    if self._children:
      child_repr = '\n'.join(
          child.debug_string(indent=indent + 1) for child in self._children
      )
      self_repr = f'{self_repr}\n{child_repr}'
    return self_repr


class Expression(AbstractSyntaxTree):
  """An abstract FHIRPath expression base class."""

  def __init__(
      self, children: Optional[List[AbstractSyntaxTree]] = None
  ) -> None:
    super(Expression, self).__init__(children)

  def __repr__(self) -> str:
    """Simple debug representation of the AST node name."""
    return type(self).__name__.lower()

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    raise ValueError('Unable to visit Expression node.')


class Literal(Expression):
  """A FHIRPath literal value."""

  def __init__(self, value: LiteralType, is_date_type: bool = False) -> None:
    super(Literal, self).__init__()
    self.value = value
    self.is_date_type = is_date_type

  def __str__(self) -> str:
    return '{ }' if self.value is None else f'{self.value}'

  def __repr__(self) -> str:
    return f'{self.value!r}'

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_literal(self, **kwargs)


# TODO(b/244184211): Specially handle $this, $index, $total.
class Identifier(Expression):
  """FHIRPath labels such as type names and property names."""

  def __init__(self, value: str) -> None:
    super(Identifier, self).__init__()
    self.value = value

  def __str__(self) -> str:
    return self.value

  def __repr__(self) -> str:
    return self.value

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_identifier(self, **kwargs)


class UnaryOperator(Expression):
  """A base operation applied to a single operand."""

  def __init__(self, op: str, operand: Expression) -> None:
    super(UnaryOperator, self).__init__([operand])
    self.op = op
    self.operand = operand

  def __str__(self) -> str:
    return f'{self.op}{self.operand}'

  def __repr__(self) -> str:
    return self.op

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    raise ValueError('Unable to visit UnaryOperator node.')


class Indexer(Expression):
  """Indexing of an expression representing a collection."""

  def __init__(self, collection: Expression, index: Expression) -> None:
    super(Indexer, self).__init__([collection, index])
    self.collection = collection
    self.index = index

  def __str__(self) -> str:
    return f'{self.collection}[{self.index}]'

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_indexer(self, **kwargs)


class Polarity(UnaryOperator):
  """Positive/negative representation of some numeric operand."""

  @enum.unique
  class Op(*_StrEnum):
    """Polarity operators."""

    NEGATIVE = '-'
    POSITIVE = '+'

  def __init__(self, op: Op, operand: Expression) -> None:
    super(Polarity, self).__init__(op, operand)

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_polarity(self, **kwargs)


class BinaryOperator(Expression):
  """A base operation applied to two operands."""

  def __init__(self, op: str, lhs: Expression, rhs: Expression) -> None:
    super(BinaryOperator, self).__init__([lhs, rhs])
    self.op = op
    self.lhs = lhs
    self.rhs = rhs

  def __str__(self) -> str:
    return f'{self.lhs} {self.op} {self.rhs}'

  def __repr__(self) -> str:
    return self.op

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    raise ValueError('Unable to visit BinaryOperator.')


class Arithmetic(BinaryOperator):
  """Arithmetic FHIRPath expressions."""

  @enum.unique
  class Op(*_StrEnum):
    """Arithmetic operators."""

    ADDITION = '+'
    DIVISION = '/'
    MODULO = 'mod'
    MULTIPLICATION = '*'
    STRING_CONCATENATION = '&'
    SUBTRACTION = '-'
    TRUNCATED_DIVISION = 'div'

  def __init__(self, op: Op, lhs: Expression, rhs: Expression) -> None:
    super(Arithmetic, self).__init__(op, lhs, rhs)

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_arithmetic(self, **kwargs)


class TypeExpression(Expression):
  """A FHIRPath type expression."""

  @enum.unique
  class Op(*_StrEnum):
    """Type operators."""

    AS = 'as'
    IS = 'is'

  def __init__(
      self, op: Op, expression: Expression, type_specifier: Identifier
  ) -> None:
    super(TypeExpression, self).__init__([expression, type_specifier])
    self.op = op
    self.expression = expression
    self.type_specifier = type_specifier

  def __str__(self) -> str:
    return f'{self.expression} {self.op} {self.type_specifier}'

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_type_expression(self, **kwargs)


class UnionOp(BinaryOperator):
  """The union between two FHIRPath expressions evaluating to collections."""

  def __init__(self, lhs: Expression, rhs: Expression) -> None:
    super(UnionOp, self).__init__('|', lhs, rhs)

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_union(self, **kwargs)


class EqualityRelation(BinaryOperator):
  """Equality comparisons between collections."""

  @enum.unique
  class Op(*_StrEnum):
    """Equality operators."""

    EQUAL = '='
    EQUIVALENT = '~'
    NOT_EQUAL = '!='
    NOT_EQUIVALENT = '!~'

  def __init__(self, op: Op, lhs: Expression, rhs: Expression) -> None:
    super(EqualityRelation, self).__init__(op, lhs, rhs)

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_equality(self, **kwargs)


class Comparison(BinaryOperator):
  """A logical comparison between two operands.

  Comparison operators are defined for: strings, integers, decimals, quantities,
  dates, datetimes, and times. Both operands should be scalar collections of
  the same type, or of types that are implicitly convertible to the same type.

  See more at: https://hl7.org/fhirpath/#comparison.
  """

  @enum.unique
  class Op(*_StrEnum):
    """Comparison operators."""

    GREATER_THAN = '>'
    GREATER_THAN_OR_EQUAL = '>='
    LESS_THAN = '<'
    LESS_THAN_OR_EQUAL = '<='

  def __init__(self, op: Op, lhs: Expression, rhs: Expression) -> None:
    super(Comparison, self).__init__(op, lhs, rhs)

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_comparison(self, **kwargs)


class BooleanLogic(BinaryOperator):
  """Performs standard Boolean logic between two operators.

  Note that the collections passed as operands are first evaluated as Booleans
  per: https://hl7.org/fhirpath/#singleton-evaluation-of-collections.

  At runtime, operators use three-valued logic to propagate empty operands. Both
  operands must be collections of a single value (or less, in the event of an
  empty collection).

  See more at: https://hl7.org/fhirpath/#boolean-logic.
  """

  @enum.unique
  class Op(*_StrEnum):
    """Boolean logic operators."""

    AND = 'and'
    IMPLIES = 'implies'
    OR = 'or'
    XOR = 'xor'

  def __init__(self, op: Op, lhs: Expression, rhs: Expression) -> None:
    super(BooleanLogic, self).__init__(op, lhs, rhs)

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_boolean_logic(self, **kwargs)


class MembershipRelation(BinaryOperator):
  """Membership relations between a singular operand and a collection."""

  @enum.unique
  class Op(*_StrEnum):
    """Membership operators."""

    CONTAINS = 'contains'
    IN = 'in'

  def __init__(self, op: Op, lhs: Expression, rhs: Expression) -> None:
    super(MembershipRelation, self).__init__(op, lhs, rhs)

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_membership(self, **kwargs)


class Invocation(BinaryOperator):
  """FHIRPath 'fluent' path navigation."""

  def __init__(self, lhs: Expression, rhs: Expression) -> None:
    super(Invocation, self).__init__('.', lhs, rhs)

  def __str__(self) -> str:
    return f'{self.lhs}{self.op}{self.rhs}'

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_invocation(self, **kwargs)


# TODO(b/244184211): Add additional functions supported by FHIR. See more at:
# https://www.hl7.org/fhir/fhirpath.html#functions.
# TODO(b/244184211): Add supported "operations". See more at:
# http://hl7.org/fhirpath/#operations.
class Function(Expression):
  """A FHIRPath built-in function invoked on some evaluated expression.

  The FHIRPath specification details several categories of functions which are
  supported here (shown in the order specified by HL7 documentation):
    * Existence
    * Filtering and Projection
    * Subsetting
    * Combining
    * Conversion
    * String
    * Math
    * Tree Navigation
    * Utility Functions
    * Operations
    * Additional Functions

  Note that "Operations" aren't explicitly listed as functions, but their
  function-form (e.g. `not()`) is treated as a function according to the CFG.

  See more at:
    * http://hl7.org/fhirpath/#functions
    * http://hl7.org/fhirpath/grammar.html
    * https://www.hl7.org/fhir/fhirpath.html#functions
  """

  @enum.unique
  class Name(*_StrEnum):
    """Supported FHIRPath function names.

    Functions are broken into categories to mirror HL7-specified categories.
    Within each category they are alphabetized A-Z. See more at:
    http://hl7.org/fhirpath/#functions.
    """

    # Existence
    ALL = 'all'
    ALL_FALSE = 'allFalse'
    ALL_TRUE = 'allTrue'
    ANY_FALSE = 'anyFalse'
    ANY_TRUE = 'anyTrue'
    COUNT = 'count'
    DISTINCT = 'distinct'
    EMPTY = 'empty'
    EXISTS = 'exists'
    IS_DISTINCT = 'isDistinct'
    SUBSET_OF = 'subsetOf'
    SUPERSET_OF = 'supersetOf'

    # Filtering and Projection
    OF_TYPE = 'ofType'
    REPEAT = 'repeat'
    SELECT = 'select'
    WHERE = 'where'

    # Subsetting
    EXCLUDE = 'exclude'
    FIRST = 'first'
    INTERSECT = 'intersect'
    LAST = 'last'
    SINGLE = 'single'
    SKIP = 'skip'
    TAIL = 'tail'
    TAKE = 'take'

    # Combining
    COMBINE = 'combine'
    UNION = 'union'  # Synonymous with the union operator: a | b

    # Conversion
    CONVERTS_TO_BOOLEAN = 'convertsToBoolean'
    CONVERTS_TO_DATE = 'convertsToDate'
    CONVERTS_TO_DATE_TIME = 'convertsToDateTime'
    CONVERTS_TO_DECIMAL = 'convertsToDecimal'
    CONVERTS_TO_INTEGER = 'convertsToInteger'
    CONVERTS_TO_QUANTITY = 'convertsToQuantity'
    CONVERTS_TO_STRING = 'convertsToString'
    CONVERTS_TO_TIME = 'convertsToTime'
    IIF = 'iif'
    TO_BOOLEAN = 'toBoolean'
    TO_DATE = 'toDate'
    TO_DATE_TIME = 'toDateTime'
    TO_DECIMAL = 'toDecimal'
    TO_INTEGER = 'toInteger'
    TO_QUANTITY = 'toQuantity'
    TO_STRING = 'toString'
    TO_TIME = 'toTime'

    # String
    CONTAINS = 'contains'
    ENDS_WITH = 'ends_with'
    INDEX_OF = 'indexOf'
    LENGTH = 'length'
    LOWER = 'lower'
    MATCHES = 'matches'
    REPLACE = 'replace'
    REPLACE_MATCHES = 'replaceMatches'
    STARTS_WITH = 'starts_with'
    SUBSTRING = 'substring'
    TO_CHARS = 'toChars'
    UPPER = 'upper'

    # Math
    ABS = 'abs'
    CEILING = 'ceiling'
    EXP = 'exp'
    FLOOR = 'floor'
    LN = 'ln'
    LOG = 'log'
    POWER = 'power'
    ROUND = 'round'
    SQRT = 'sqrt'

    # Tree Navigation
    CHILDREN = 'children'
    DESCENDENTS = 'descendents'

    # Utility Functions
    NOW = 'now'
    TIME_OF_DAY = 'timeOfDay'
    TODAY = 'today'
    TRACE = 'trace'

    # Operations
    # Boolean
    NOT = 'not'

    # Additional functions
    CHECK_MODIFIERS = 'checkModifiers'
    CONFORMS_TO = 'conformsTo'
    ELEMENT_DEFINITION = 'elementDefinition'
    EXTENSION = 'extension'
    GET_VALUE = 'getValue'
    HAS_VALUE = 'hasValue'
    HTML_CHECKS = 'htmlChecks'
    MEMBER_OF = 'memberOf'
    RESOLVE = 'resolve'
    SLICE = 'slice'
    SUBSUMES = 'subsumes'
    SUBSUMED_BY = 'subsumedBy'

    # Non-FHIRPath Functions to simplify data analysis workloads.
    # TODO(b/221322122): Consider separating these to avoid tight coupling
    # of FHIRPath and additional functions.
    ID_FOR = 'idFor'

  def __init__(
      self, identifier: Identifier, params: Optional[List[Expression]] = None
  ) -> None:
    super(Function, self).__init__(
        [identifier] + (params if params is not None else [])
    )
    self.identifier = identifier
    self.params = params if params is not None else []

  def __str__(self) -> str:
    params_str = str(self.params) if self.params else ''
    return f'{self.identifier}({params_str})'

  def accept(self, visitor: 'FhirPathAstBaseVisitor', **kwargs: Any) -> Any:
    return visitor.visit_function(self, **kwargs)


_ArithmeticContext = Union[
    FhirPathParser.AdditiveExpressionContext,
    FhirPathParser.MultiplicativeExpressionContext,
]
_LogicalExpressionContext = Union[
    FhirPathParser.AndExpressionContext,
    FhirPathParser.OrExpressionContext,
    FhirPathParser.ImpliesExpressionContext,
]


class _FhirPathErrorListener(ErrorListener.ErrorListener):
  """Manages FHIRPath errors."""

  def __init__(self) -> None:
    self.errors: List[str] = []

  def syntaxError(
      self,
      recognizer: FhirPathParser,
      offending_symbol: Any,
      line: int,
      column: int,
      msg: str,
      e: Exception,
  ) -> None:
    formatted_err_msg = 'line: %d:%d %s' % (line, column, msg)
    logging.error(formatted_err_msg)
    self.errors.append(formatted_err_msg)


class _FhirPathCstVisitor(FhirPathVisitor):
  """A parse tree visitor which constructs a FHIRPath AST representation."""

  def _build_arithmetic(self, ctx: _ArithmeticContext) -> Arithmetic:
    op = Arithmetic.Op(ctx.getChild(1).getText())
    lhs: Expression = self.visit(ctx.expression(0))
    rhs: Expression = self.visit(ctx.expression(1))
    return Arithmetic(op, lhs, rhs)

  def _build_boolean_logic(
      self, ctx: _LogicalExpressionContext
  ) -> BooleanLogic:
    op = BooleanLogic.Op(ctx.getChild(1).getText())
    lhs: Expression = self.visit(ctx.expression(0))
    rhs: Expression = self.visit(ctx.expression(1))
    return BooleanLogic(op, lhs, rhs)

  def visitIndexerExpression(
      self, ctx: FhirPathParser.IndexerExpressionContext
  ) -> Indexer:
    lhs: Expression = self.visit(ctx.expression(0))
    rhs: Expression = self.visit(ctx.expression(1))
    return Indexer(lhs, rhs)

  def visitPolarityExpression(
      self, ctx: FhirPathParser.PolarityExpressionContext
  ) -> Polarity:
    op = Polarity.Op(ctx.getChild(0).getText())
    operand: Expression = self.visit(ctx.expression())
    return Polarity(op, operand)

  def visitAdditiveExpression(
      self, ctx: FhirPathParser.AdditiveExpressionContext
  ) -> Arithmetic:
    return self._build_arithmetic(ctx)

  def visitMultiplicativeExpression(
      self, ctx: FhirPathParser.MultiplicativeExpressionContext
  ) -> Arithmetic:
    return self._build_arithmetic(ctx)

  def visitUnionExpression(
      self, ctx: FhirPathParser.UnionExpressionContext
  ) -> UnionOp:
    lhs: Expression = self.visit(ctx.expression(0))
    rhs: Expression = self.visit(ctx.expression(1))
    return UnionOp(lhs, rhs)

  def visitOrExpression(
      self, ctx: FhirPathParser.OrExpressionContext
  ) -> BooleanLogic:
    return self._build_boolean_logic(ctx)

  def visitAndExpression(
      self, ctx: FhirPathParser.AndExpressionContext
  ) -> BooleanLogic:
    return self._build_boolean_logic(ctx)

  def visitMembershipExpression(
      self, ctx: FhirPathParser.MembershipExpressionContext
  ) -> MembershipRelation:
    op = MembershipRelation.Op(ctx.getChild(1).getText())
    lhs: Expression = self.visit(ctx.expression(0))
    rhs: Expression = self.visit(ctx.expression(1))
    return MembershipRelation(op, lhs, rhs)

  def visitInequalityExpression(
      self, ctx: FhirPathParser.InequalityExpressionContext
  ) -> Comparison:
    op = Comparison.Op(ctx.getChild(1).getText())
    lhs: Expression = self.visit(ctx.expression(0))
    rhs: Expression = self.visit(ctx.expression(1))
    return Comparison(op, lhs, rhs)

  def visitInvocationExpression(
      self, ctx: FhirPathParser.InvocationExpressionContext
  ) -> Union[Invocation, Function]:
    lhs: Expression = self.visit(ctx.expression())
    rhs: Union[Identifier, Function] = self.visit(ctx.invocation())
    return Invocation(lhs, rhs)

  def visitEqualityExpression(
      self, ctx: FhirPathParser.EqualityExpressionContext
  ) -> EqualityRelation:
    op = EqualityRelation.Op(ctx.getChild(1).getText())
    lhs: Expression = self.visit(ctx.expression(0))
    rhs: Expression = self.visit(ctx.expression(1))
    return EqualityRelation(op, lhs, rhs)

  def visitImpliesExpression(
      self, ctx: FhirPathParser.ImpliesExpressionContext
  ) -> BooleanLogic:
    return self._build_boolean_logic(ctx)

  def visitTermExpression(
      self, ctx: FhirPathParser.TermExpressionContext
  ) -> Expression:
    # Singular child of type: invocation, literal, externalConstant, or
    # '(' expression ')'; propagate.
    return self.visit(ctx.getChild(0))

  def visitTypeExpression(
      self, ctx: FhirPathParser.TypeExpressionContext
  ) -> TypeExpression:
    op = TypeExpression.Op(ctx.getChild(1).getText())
    expression: Expression = self.visit(ctx.expression())
    type_specifier: Identifier = self.visit(ctx.typeSpecifier())
    return TypeExpression(op, expression, type_specifier)

  def visitInvocationTerm(
      self, ctx: FhirPathParser.InvocationTermContext
  ) -> Union[Identifier, Function]:
    # Singular non-termal of type: invocation; propagate
    return self.visit(ctx.invocation())

  def visitLiteralTerm(self, ctx: FhirPathParser.LiteralTermContext) -> Literal:
    value: LiteralType = self.visit(ctx.getChild(0))
    # Preserve whether the literal is a date type for evaluation needs.
    if isinstance(value, str) and value.startswith('@'):
      return Literal(value[1:], is_date_type=True)
    else:
      return Literal(value)

  def visitExternalConstantTerm(
      self, ctx: FhirPathParser.ExternalConstantTermContext
  ) -> Union[Identifier, str]:
    # A value of externalConstant; propagate.
    return self.visit(ctx.getChild(0))

  def visitParenthesizedTerm(
      self, ctx: FhirPathParser.ParenthesizedTermContext
  ) -> Expression:
    # A value of '(' expression ')'; propagate.
    return self.visit(ctx.getChild(1))

  def visitNullLiteral(self, ctx: FhirPathParser.NullLiteralContext) -> None:
    # Note that in FHIRPath there is no representation for "NULL". This means
    # that when, in an underlying data object, a member is "null" or "missing",
    # there will be no correspondence for the node in the tree. E.g.
    # `Patient.name` will return an empty collection {}.
    return None

  def visitBooleanLiteral(
      self, ctx: FhirPathParser.BooleanLiteralContext
  ) -> bool:
    return bool(ctx.getChild(0).getText() == 'true')

  def visitStringLiteral(self, ctx: FhirPathParser.StringLiteralContext) -> str:
    # Remove leading and trailing single quotations.
    return str(ctx.getChild(0).getText())[1:-1]

  def visitNumberLiteral(
      self, ctx: FhirPathParser.NumberLiteralContext
  ) -> Number:
    # Note that Python3 decimal.Decimal defaults to 28 digits of *exact*
    # precision, which is sufficient to represent a FHIRPath Decimal:
    # http://hl7.org/fhirpath/#decimal.
    raw_str: str = ctx.getChild(0).getText()
    return decimal.Decimal(raw_str) if '.' in raw_str else int(raw_str)

  def visitDateLiteral(self, ctx: FhirPathParser.DateLiteralContext) -> str:
    raw_str: str = ctx.getChild(0).getText()
    return raw_str

  def visitDateTimeLiteral(
      self, ctx: FhirPathParser.DateTimeLiteralContext
  ) -> str:
    raw_str: str = ctx.getChild(0).getText()
    return raw_str

  def visitTimeLiteral(self, ctx: FhirPathParser.TimeLiteralContext) -> str:
    # Returns the ISO-8610-compliant substring, trimming the leading '@T'.
    raw_str: str = ctx.getChild(0).getText()
    return raw_str[2:]

  def visitQuantityLiteral(
      self, ctx: FhirPathParser.QuantityLiteralContext
  ) -> Quantity:
    # Singular `quantity` non-terminal; propagate.
    return self.visit(ctx.getChild(0))

  def visitExternalConstant(
      self, ctx: FhirPathParser.ExternalConstantContext
  ) -> Union[Identifier, str]:
    # A value of '%' (identifier | STRING); propagate on non-terminal.
    if ctx.STRING() is not None:
      return str(ctx.getChild(1).getText())[1:-1]
    return self.visit(ctx.getChild(1))

  def visitMemberInvocation(
      self, ctx: FhirPathParser.MemberInvocationContext
  ) -> Identifier:
    # A value of identifier; propagate
    return self.visit(ctx.identifier())

  def visitFunctionInvocation(
      self, ctx: FhirPathParser.FunctionInvocationContext
  ) -> Function:
    # A value of function; propagate
    return self.visit(ctx.getChild(0))

  def visitThisInvocation(
      self, ctx: FhirPathParser.ThisInvocationContext
  ) -> Identifier:
    # $this terminal
    return Identifier(ctx.getChild(0).getText())

  def visitIndexInvocation(
      self, ctx: FhirPathParser.IndexInvocationContext
  ) -> Identifier:
    # $index terminal
    return Identifier(ctx.getChild(0).getText())

  def visitTotalInvocation(
      self, ctx: FhirPathParser.TotalInvocationContext
  ) -> Identifier:
    # $total terminal
    return Identifier(ctx.getChild(0).getText())

  def visitFunction(self, ctx: FhirPathParser.FunctionContext) -> Function:
    if ctx.paramList() is None:
      return Function(self.visit(ctx.identifier()))
    return Function(
        self.visit(ctx.identifier()), params=self.visit(ctx.paramList())
    )

  def visitParamList(
      self, ctx: FhirPathParser.ParamListContext
  ) -> List[Expression]:
    # A value of expression (, expression)*; propagate, collect, and return
    result: List[Expression] = []
    for i in range(ctx.getChildCount()):
      result.append(self.visit(ctx.getChild(i)))
    return result

  def visitQuantity(self, ctx: FhirPathParser.QuantityContext) -> Quantity:
    # Note: This is an ambiguity in the FHIRPath CFG. Both quantity *and*
    # NUMBER are visited within the same literal production. Therefore, a
    # `quantity` production will always have a unit.
    return Quantity(ctx.getChild(0).getText(), self.visit(ctx.unit()))

  def visitUnit(self, ctx: FhirPathParser.UnitContext) -> str:
    # A value of dateTimePrecision | pluralDateTimePrecision | STRING. Propagate
    # on non-terminals, and strip leading/trailing quotes and return if string
    # literal.
    if ctx.STRING() is not None:
      return str(ctx.getChild(0).getText())[1:-1]
    return self.visit(ctx.getChild(0))

  def visitDateTimePrecision(
      self, ctx: FhirPathParser.DateTimePrecisionContext
  ) -> str:
    # Remove leading and trailing single quotations.
    return str(ctx.getChild(0).getText())[1:-1]

  def visitPluralDateTimePrecision(
      self, ctx: FhirPathParser.PluralDateTimePrecisionContext
  ) -> str:
    # Remove leading and trailing single quotations.
    return str(ctx.getChild(0).getText())[1:-1]

  def visitTypeSpecifier(
      self, ctx: FhirPathParser.TypeSpecifierContext
  ) -> Identifier:
    # A value of qualifiedIdentifier; propagate
    return self.visit(ctx.qualifiedIdentifier())

  def visitQualifiedIdentifier(
      self, ctx: FhirPathParser.QualifiedIdentifierContext
  ) -> Identifier:
    # A value of: identifier ('.' identifier)* expressing a fully-qualified
    # FHIRPath type for exclusive use within a `typeExpression`. Treat as a
    # single dot delimited ('.') identifier.
    result: List[str] = []
    for i in range(ctx.getChildCount()):
      id_: Identifier = self.visit(ctx.getChild(i))
      result.append(id_.value)
    return Identifier('.'.join(result))

  def visitIdentifier(
      self, ctx: FhirPathParser.IdentifierContext
  ) -> Identifier:
    # Note: This is an ambiguity in the FHIRPath CFG. The highlighted keywords
    # 'as', 'contains', 'in', and 'is' collide with the IDENTIFIER token.
    # Assuming leading terminals are consumed first in the event of a tie,
    # IDENTIFIER will always be chosen.
    if ctx.DELIMITEDIDENTIFIER() is not None:
      return Identifier(ctx.DELIMITEDIDENTIFIER().getText().strip('`'))
    if ctx.IDENTIFIER() is None:
      raise ValueError(
          'Unsupported FHIRPath expression. Expected an identifier'
          ' but got None.'
      )
    return Identifier(ctx.IDENTIFIER().getText())


class FhirPathAstBaseVisitor(abc.ABC):
  """An abstract base class for AST visitation.

  Implementers are responsible for overriding the `visit*` methods with a
  concrete implementation. The responsibility is on the implementation to
  visit children of any node.
  """

  def visit(self, node: AbstractSyntaxTree, **kwargs: Any) -> Any:
    """Calls `node.accept`, passing the caller as a visitor."""
    return node.accept(self, **kwargs)

  def visit_children(
      self, node: AbstractSyntaxTree, **kwargs: Any
  ) -> List[Any]:
    """Calls `accept` on each child node, passing the caller as a visitor."""
    result: List[Any] = []
    for c in node.children:
      result.append(c.accept(self, **kwargs))
    return result

  @abc.abstractmethod
  def visit_literal(self, literal: Literal, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_identifier(self, identifier: Identifier, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_indexer(self, indexer: Indexer, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_arithmetic(self, arithmetic: Arithmetic, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_type_expression(
      self, type_expression: TypeExpression, **kwargs: Any
  ) -> Any:
    pass

  @abc.abstractmethod
  def visit_equality(self, equality: EqualityRelation, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_comparison(self, comparison: Comparison, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_boolean_logic(
      self, boolean_logic: BooleanLogic, **kwargs: Any
  ) -> Any:
    pass

  @abc.abstractmethod
  def visit_membership(
      self, membership: MembershipRelation, **kwargs: Any
  ) -> Any:
    pass

  @abc.abstractmethod
  def visit_union(self, union: UnionOp, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_polarity(self, polarity: Polarity, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_invocation(self, invocation: Invocation, **kwargs: Any) -> Any:
    pass

  @abc.abstractmethod
  def visit_function(self, function: Function, **kwargs: Any) -> Any:
    pass


def build_fhir_path_ast(input_str: str) -> Expression:
  """Given a FHIRPath query, constructs an AST and returns the root node.

  Args:
    input_str: The FHIRPath string to translate.

  Returns:
    A FHIRPath `Expression` instance, representing the root AST node.

  Raises:
    ValueError: In the event that the provided `input_str` was syntactically
    invalid FHIRPath that failed during lexing/parsing.
  """
  error_listener = _FhirPathErrorListener()

  # Lex
  lexer = FhirPathLexer(antlr4.InputStream(input_str))
  lexer.removeErrorListeners()
  lexer.addErrorListener(error_listener)
  token_stream = antlr4.CommonTokenStream(lexer)

  # Parse
  parser = FhirPathParser(token_stream)
  parser.removeErrorListeners()
  parser.addErrorListener(error_listener)

  # Generate parse tree (CST) and build AST representation
  cst_visitor = _FhirPathCstVisitor()
  cst = parser.expression()

  # Raise all lexical/parsing errors
  if error_listener.errors:
    raise ValueError('\n'.join(error_listener.errors))

  ast = cst_visitor.visit(cst)
  return ast


def _ast_to_string_helper(t: AbstractSyntaxTree, result: List[str]) -> None:
  """Recursively formats a LISP string representation of an AST."""
  repr_ = repr(t)
  if t.children is None:
    result.append(repr_)
  else:
    result.append('(')
    result.append(repr_)
    for c in t.children:
      result.append(' ')
      _ast_to_string_helper(c, result)
    result.append(')')


def ast_to_string(t: AbstractSyntaxTree) -> str:
  """Given a FHIRPath AST, returns a LISP `repr` of the tree structure."""
  result: List[str] = []
  _ast_to_string_helper(t, result)
  return ''.join(result)


def paths_referenced_by(node: AbstractSyntaxTree) -> Collection[str]:
  """Finds paths for any fields referenced in the given tree.

  For example, given the expression 'a.b.where(c > d.e)' returns paths
  ['a.b', 'a.b.c', 'a.b.d.e']

  Args:
    node: The abstract syntax tree to search for paths.

  Returns:
    A collections of paths referenced in the AST.
  """
  context, paths = _paths_referenced_by(node)
  paths = set(paths)

  if context is not None:
    paths.add(context)

  return paths


def _paths_referenced_by(
    node: AbstractSyntaxTree,
) -> Tuple[Optional[str], Collection[str]]:
  """Finds paths for any fields referenced in the given tree.

  Recursively builds paths by visitng the trees nodes depth-first in-order.
  Returns a tuple of (context, paths) where `context` is an identifier which may
  be part of a dotted path completed by its parent and `paths` are the full
  dotted paths found so far.

  Callers are responsible for attempting to either continue chaining successive
  identifiers from invocations to the context or acknowledging it as completed
  and adding it to `paths` if the caller has no identifiers to add to the chain.

  Args:
    node: The abstract syntax tree to search for paths.

  Returns:
    A tuple of (context, paths) as described above.
  """
  if isinstance(node, Identifier):
    # Return the identifier as the first element in a possible chain. The caller
    # can use this identifier as context to build longer dotted paths.
    # Identifiers have no children, so there are no other nodes to visit.
    if node.value in ('$this', '$index', '$total'):
      # We can ignore special values like $this because they will refer to other
      # existing identifiers in the AST.
      return None, ()
    else:
      return node.value, ()

  # The node does not start an identifier chain and has no children to search.
  if not node.children:
    return None, ()

  # Start from the left-most child. The left-most child will be the beginning
  # of the first identifier of a dotted path.
  context, paths = _paths_referenced_by(node.children[0])
  if isinstance(node, Function):
    # The identifier found by visiting a function's left-most child will be the
    # name of the function. This isn't an identifier we are interested in, so we
    # proceed without using it as a context to build other paths.
    context = None

  if isinstance(node, Invocation) and isinstance(node.rhs, Identifier):
    # Continue the dotted path chain.
    # Identifiers have no children, so there are no other nodes to visit.
    context = _append_path_to_context(context, node.rhs.value)
    return context, paths

  # The dotted path chain has ended. Add it as a discovered path.
  if context is not None:
    paths = paths + (context,)  # pytype: disable=unsupported-operands  # always-use-return-annotations

  if isinstance(node, Invocation):
    # The rhs is a function and the lhs is the identifier on which the function
    # is being called, available as the context from the above call.

    # Find any identifiers referenced in the function call's parameters.
    child_paths = _get_paths_from_children_except_first(node)

    # We understand function arguments as relative to their operand.
    # e.g. a.where(b > c.d) is understood as a.where(a.b > a.c.d)
    child_paths = tuple(
        _append_path_to_context(context, child_path)
        for child_path in child_paths
    )

    return context, paths + child_paths

  # Find any identifiers referenced in any child nodes, besides the first which
  # we already visited above.
  child_paths = _get_paths_from_children_except_first(node)
  return context, paths + child_paths


def _get_paths_from_children_except_first(
    node: AbstractSyntaxTree,
) -> Tuple[str, ...]:
  """Finds paths referenced by any child nodes except the first."""
  paths = []
  children = node.children or ()
  for child in children[1:]:
    child_context, child_paths = _paths_referenced_by(child)
    paths.extend(child_paths)
    if child_context is not None:
      paths.append(child_context)

  return tuple(paths)


def _append_path_to_context(context: Optional[str], path: str) -> str:
  """Extends a dotted path by appending `path` to the `context`."""
  if context is None:
    return path
  else:
    return f'{context}.{path}'


def contains_reference_without_id_for(node: AbstractSyntaxTree) -> bool:
  """Checks if the AST contains a reference without a corresponding idFor call.

  Args:
    node: The root node of the abstract syntax tree to search.

  Returns:
    True if the abstract syntax tree contains an identifier to a reference
    without an idFor call against it. False otherwise.
  """
  # Check if the node is an identifier of a reference type.
  if (
      isinstance(node, Identifier)
      and isinstance(node.data_type, _fhir_path_data_types.StructureDataType)
      and node.data_type.element_type == 'Reference'
  ):
    # Try to find a parent invocation with the reference node on the
    # left and an idFor function on the right.
    if not node.has_parent or not isinstance(node.parent, Invocation):
      return True

    # For a path like 'reference.idFor' the reference would be on the
    # left of an invocation with the function call on the right. For a
    # path like 'foo.reference.idFor' the reference would be on the
    # right with another parent invocation containing the function
    # call on its right.
    if node.parent.lhs == node:
      parent_invocation = node.parent
    else:
      if not node.parent.has_parent or not isinstance(
          node.parent.parent, Invocation
      ):
        return True
      parent_invocation = node.parent.parent

    # Ensure the reference has an idFor call against it.
    if (
        not isinstance(parent_invocation.rhs, Function)
        or parent_invocation.rhs.identifier.value != 'idFor'
    ):
      return True

  # Recursively check each child node.
  for child in node.children or ():
    if contains_reference_without_id_for(child):
      return True

  return False

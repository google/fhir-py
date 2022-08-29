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
"""Functionality for Standard SQL datatype inference and validation.

Scalar types are declared as module-level constants that can be imported by
dependent modules. Parameterized types such as `Array` and `Struct` should be
instantiated directly, so that the relevant contained type information can be
provided by the caller.
"""

from __future__ import annotations

import abc
import dataclasses
from typing import Any, List, Optional, Sequence, Set, Union

# TODO: Consolidate with `_fhir_path_data_types.py` functionality.

# Keywords are a group of tokens that have special meaning in the BigQuery
# language, and have the following characteristics:
# * Keywords cannot be used as identifiers unless enclosed by backtick (`)
#   characters.
# * Keywords are case insensitive.
STANDARD_SQL_KEYWORDS = frozenset([
    'ALL',
    'AND',
    'ANY',
    'ARRAY',
    'AS',
    'ASC',
    'ASSERT_ROWS_MODIFIED',
    'AT',
    'BETWEEN',
    'BY',
    'CASE',
    'CAST',
    'COLLATE',
    'CONTAINS',
    'CREATE',
    'CROSS',
    'CUBE',
    'CURRENT',
    'DEFAULT',
    'DEFINE',
    'DESC',
    'DISTINCT',
    'ELSE',
    'END',
    'ENUM',
    'ESCAPE',
    'EXCEPT',
    'EXCLUDE',
    'EXISTS',
    'EXTRACT',
    'FALSE',
    'FETCH',
    'FOLLOWING',
    'FOR',
    'FROM',
    'FULL',
    'GROUP',
    'GROUPING',
    'GROUPS',
    'HASH',
    'HAVING',
    'IF',
    'IGNORE',
    'IN',
    'INNER',
    'INTERSECT',
    'INTERVAL',
    'INTO',
    'IS',
    'JOIN',
    'LATERAL',
    'LEFT',
    'LIKE',
    'LIMIT',
    'LOOKUP',
    'MERGE',
    'NATURAL',
    'NEW',
    'NO',
    'NOT',
    'NULL',
    'NULLS',
    'OF',
    'ON',
    'OR',
    'ORDER',
    'OUTER',
    'OVER',
    'PARTITION',
    'PRECEDING',
    'PROTO',
    'RANGE',
    'RECURSIVE',
    'RESPECT',
    'RIGHT',
    'ROLLUP',
    'ROWS',
    'SELECT',
    'SET',
    'SOME',
    'STRUCT',
    'TABLESAMPLE',
    'THEN',
    'TO',
    'TREAT',
    'TRUE',
    'UNBOUNDED',
    'UNION',
    'UNNEST',
    'USING',
    'WHEN',
    'WHERE',
    'WINDOW',
    'WITH',
    'WITHIN',
])


# TODO: Add a consistent string representation to all the
# data_types.
class StandardSqlDataType(metaclass=abc.ABCMeta):
  """Describes Standard SQL datatypes and their attributes.

  See more at:
  https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types.

  Attributes:
    nullable: `NULL` is a valid value.
    orderable: Can be used in an `ORDER BY` clause.
    groupable: Can generally appear in an expression following `GROUP BY`,
      `DISTINCT`, and `PARTITION BY`. However, `PARTITION BY` expressions cannot
      include floating point types.
    comparable: Values of the same type can be compared to each other.
    supported_coercion: A set of `StandardSqlDataType`s depicting allowable
      implicit conversion.
  """

  def __init__(
      self,
      *,
      nullable: bool,
      orderable: bool,
      groupable: bool,
      comparable: bool,
  ) -> None:
    self._nullable = nullable
    self._orderable = orderable
    self._groupable = groupable
    self._comparable = comparable

  @property
  def nullable(self) -> bool:
    return self._nullable

  @property
  def orderable(self) -> bool:
    return self._orderable

  @property
  def groupable(self) -> bool:
    return self._groupable

  @property
  def comparable(self) -> bool:
    return self._comparable

  @property
  @abc.abstractmethod
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    pass


class Array(StandardSqlDataType):
  """An ordered list of zero or more elements of non-`ARRAY` values.

  `ARRAY`s of `ARRAY`s are not allowed. Instead a `STRUCT` must be inserted
  between nested `ARRAY`s.

  Attributes:
    contained_type: The `StandardSqlDataType` contained within the `ARRAY`. If
      absent, then this is considered an "opaque ARRAY", and type information
      about the contained type is unknown.
  """

  def __init__(self,
               contained_type: Optional[StandardSqlDataType] = None) -> None:
    super(Array, self).__init__(
        nullable=False,
        orderable=False,
        groupable=False,
        comparable=False,
    )
    if isinstance(contained_type, Array):
      raise ValueError('`ARRAY`s of `ARRAY`s are not supported.')
    self._contained_type = contained_type

  def __eq__(self, other: Any) -> bool:
    if isinstance(other, Array):
      return self.contained_type == other.contained_type
    return False

  @property
  def contained_type(self) -> Optional[StandardSqlDataType]:
    return self._contained_type

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return f'<ArraySqlDataType(contained_type: {self.contained_type})>'


class _Boolean(StandardSqlDataType):
  """Boolean values are represented by the keywords `TRUE` and `FALSE`."""

  def __init__(self) -> None:
    super(_Boolean, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<BooleanSqlDataType>'


class _Bytes(StandardSqlDataType):
  """Variable-length binary data."""

  def __init__(self) -> None:
    super(_Bytes, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<BytesSqlDataType>'


class _Date(StandardSqlDataType):
  """A logical calendar date, independent of timezone."""

  def __init__(self) -> None:
    super(_Date, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set([Datetime])

  def __str__(self):
    return '<DateSqlDataType>'


class _Datetime(StandardSqlDataType):
  """A date and time, as they might be displayed on a calendar or clock."""

  def __init__(self) -> None:
    super(_Datetime, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<DateTimeSqlDataType>'


class _Geography(StandardSqlDataType):
  """A collection of points, lines, and polygons.

  The collection is represented as a point set, or a subset of the surface of
  the Earth.
  """

  def __init__(self) -> None:
    super(_Geography, self).__init__(
        nullable=True,
        orderable=False,
        groupable=False,
        comparable=False,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<GeographySqlDataType>'


class _Int64(StandardSqlDataType):
  """A 64-bit integer."""

  def __init__(self) -> None:
    super(_Int64, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set([
        Numeric,
        BigNumeric,
        Float64,
    ])

  def __str__(self):
    return '<Int64SqlDataType>'


class _Numeric(StandardSqlDataType):
  """A numeric value with a fixed precision (38) and scale (9).

  Note that `NUMERIC` is an alias for `DECIMAL`.
  """

  def __init__(self) -> None:
    super(_Numeric, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set([
        BigNumeric,
        Float64,
    ])

  def __str__(self):
    return '<NumericSqlDataType>'


class _BigNumeric(StandardSqlDataType):
  """A numeric value with a fixed precision (76.76) and scale (38).

  Note that `BIGNUMERIC` is an alias for `BIGDECIMAL`.
  """

  def __init__(self) -> None:
    super(_BigNumeric, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set([
        Float64,
    ])

  def __str__(self):
    return '<BigNumericSqlDataType>'


class _Float64(StandardSqlDataType):
  """Approximate numeric values with fractional components."""

  def __init__(self) -> None:
    super(_Float64, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<Float64SqlDataType>'


class _String(StandardSqlDataType):
  """Variable-length character (Unicode) data."""

  def __init__(self) -> None:
    super(_String, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<StringSqlDataType>'


class Struct(StandardSqlDataType):
  """Container of ordered fields each with a type.

  Note that equality comparisons for `STRUCT`s are supported field by field, in
  field order. Field names are ignored. Less-than and greater-than comparisons
  are not supported.

  Attributes:
    fields: An optional ordered-list of type information of each of the STRUCT's
      members. If absent, then this is considered an "opaque STRUCT", and type
      information about its members is unknown.
  """

  def __init__(self,
               fields: Optional[List[StandardSqlDataType]] = None) -> None:
    super(Struct, self).__init__(
        nullable=True,
        orderable=False,
        groupable=False,
        comparable=True,
    )
    self.fields = fields

  def __eq__(self, other: Any) -> bool:
    if isinstance(other, Struct):
      return self.fields == other.fields
    return False

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return f'<StructSqlDataType(fields: {self.fields})>'


class _Time(StandardSqlDataType):
  """Represents a time, as might be displayed on a watch.

  `TIME` values represented are independent of a specific date and timezone.
  """

  def __init__(self) -> None:
    super(_Time, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<TimeSqlDataType>'


class _Timestamp(StandardSqlDataType):
  """Represents an absolute point in time.

  `TIMESTAMP` values are independent of any time zone or convention such as
  Daylight Savings Time with microsecond precision.
  """

  def __init__(self) -> None:
    super(_Timestamp, self).__init__(
        nullable=True,
        orderable=True,
        groupable=True,
        comparable=True,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<TimestampSqlDataType>'


class _Undefined(StandardSqlDataType):
  """Represents datatypes for which no type information is available.

  For an exmaple of when this type might be used, consider a scenario where an
  expression resolves to a `NULL` state, but we have no type information.
  """

  def __init__(self) -> None:
    super(_Undefined, self).__init__(
        nullable=False,
        orderable=False,
        groupable=False,
        comparable=False,
    )

  @property
  def supported_coercion(self) -> Set[StandardSqlDataType]:
    return set()  # No supported coercion

  def __str__(self):
    return '<UndefinedSqlDataType>'


# Module-level instances for import+type inference. Note that parameterized
# types (such as `Array` and `Struct`) should be instantiated.
Boolean = _Boolean()
Bytes = _Bytes()
Date = _Date()
Datetime = _Datetime()
Geography = _Geography()
Int64 = _Int64()
Numeric = _Numeric()
BigNumeric = _BigNumeric()
Float64 = _Float64()
String = _String()
Time = _Time()
Timestamp = _Timestamp()
Undefined = _Undefined()

# Empty parameterized types. Useful for type inference when the parameterized-
# type is inconsequential.
OpaqueArray = Array()
OpaqueStruct = Struct()

NUMERIC_TYPES = frozenset([
    Int64,
    Numeric,
    BigNumeric,
    Float64,
])


def is_coercible(lhs: StandardSqlDataType, rhs: StandardSqlDataType) -> bool:
  """Returns `True` if coercion can occur between `lhs` and `rhs`.

  See more at:
  https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules

  Args:
    lhs: The left operand.
    rhs: The right operand.

  Raises:
    ValueError: In the event that a coercion cycle is detected.

  Returns:
    `True` if coercion can occur, otherwise `False.`
  """
  if rhs == lhs:
    return True  # Early-exit if same type

  # TODO: Remove this special case with a common SQL flow.
  if isinstance(lhs, type(Undefined)):
    return True  # Assume unknown types can be coerced, like pytype and Any.

  if isinstance(rhs, (Array, Struct)) or isinstance(lhs, (Array, Struct)):
    return False  # Early-exit if either operand is a complex type

  if rhs in lhs.supported_coercion and lhs in rhs.supported_coercion:
    raise ValueError(f'Coercion cycle between: {lhs} and {rhs}.')

  return rhs in lhs.supported_coercion or lhs in rhs.supported_coercion


def coerce(lhs: StandardSqlDataType,
           rhs: StandardSqlDataType) -> StandardSqlDataType:
  """Performs implicit Standard SQL coercion between two datatypes.

  See more at:
  https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules

  Args:
    lhs: The left operand.
    rhs: The right operand.

  Returns:
    The resulting coerced datatype, if successful.

  Raises:
    TypeError: In the event that coercion is not supported.
    ValueError: In the event that a coercion cycle is detected.
  """
  if not is_coercible(lhs, rhs):
    raise TypeError(
        f'Unsupported Standard SQL coercion between {lhs} and {rhs}.')

  if rhs in lhs.supported_coercion:
    return rhs
  else:  # lhs in rhs.supported_coercion
    return lhs


class StandardSqlExpression(metaclass=abc.ABCMeta):
  """ABC for classes which render Standard SQL statements.

  Implementers must provide the following methods: __str__, sql_data_type,
  sql_alias.
  """

  @abc.abstractmethod
  def __str__(self) -> str:
    """Returns the raw SQL represented by the class."""

  @property
  @abc.abstractmethod
  def sql_alias(self) -> str:
    """Returns the alias of the resulting SQL expression.

    This will be used to generate 'SELECT AS' statements matching the alias.
    """

  @property
  @abc.abstractmethod
  def sql_data_type(self) -> StandardSqlDataType:
    """Returns the Standard SQL datatype of the expression."""

  def to_subquery(self) -> StandardSqlExpression:
    """Renders the expression as a subquery.

    Builds a SELECT statement for the expression and returns it as a subquery.
    Expressions which already render a SELECT (such as the Select and
    UnionExpression classes) should overide this to remove the extra SELECT.

    Returns:
      A SubQuery expression for this expression.
    """
    return SubQuery(Select(select_part=self, from_part=None))

  def matches_alias(self, alias: str) -> bool:
    """Indicates whether the expression will be selected as the given alias.

    Intended to be over-ridden by sub-classes which can safely implement it.
    Given an expression and an alias, indicates whether the expression will be
    SELECT'd as the given alias. For example, an expression like `SELECT a.b`
    matches the alias 'b', making it equivalent to the expression
    `SELECT a.b AS b`.

    Args:
      alias: The alias to compare the expression against.

    Returns:
      True when the expression evaluates to the same name as the alias and False
      otherwise.
    """
    del self  # Sub-classes implementing this method will need self.
    del alias  # Sub-classes implementing this method will need alias.
    return False

  def is_null(self, **kwargs) -> StandardSqlExpression:
    """Builds an IS NULL expression from this expression."""
    return IsNullOperator(self, **kwargs)

  def is_not_null(self, **kwargs) -> StandardSqlExpression:
    """Builds an IS NOT NULL expression from this expression."""
    return IsNotNullOperator(self, **kwargs)

  def as_operand(self) -> str:
    """Returns the simplest possible str of this expression.

    For use in other operations.
    """
    return str(self)


@dataclasses.dataclass
class RawExpression(StandardSqlExpression):
  """A raw SQL expression.

  Attributes:
    sql_expr: The raw Standard SQL expression string.
  """
  sql_expr: str
  _sql_data_type: StandardSqlDataType
  _sql_alias: str = 'f0_'

  def __str__(self) -> str:
    return self.sql_expr

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    return self._sql_data_type

  @property
  def sql_alias(self) -> str:
    return self._sql_alias


class Identifier(StandardSqlExpression):
  """Represents an identifier in a SELECT statement.

  Attributes:
    dotted_path: Successive identifier names representing a dotted path. A
      sequence like ('a', 'b') will result in SQL like 'SELECT a.b'.
  """
  dotted_path: Sequence[str]
  _sql_data_type: StandardSqlDataType
  _sql_alias: Optional[str] = None

  def __init__(self,
               name: Union[str, Sequence[str]],
               _sql_data_type: StandardSqlDataType,
               _sql_alias: Optional[str] = None) -> None:
    """Builds an identifier.

    Args:
      name: Either a single name or a sequence of names representing a dotted
        path. A sequence like ('a', 'b') will result in SQL like 'SELECT a.b'.
      _sql_data_type: The type of the values behind the identifier.
      _sql_alias: The alias of the identifier. Defaults to the last element in
        the dotted identifier path.
    """
    if isinstance(name, str):
      self.dotted_path = (name,)
    else:
      self.dotted_path = name

    self._sql_data_type = _sql_data_type
    self._sql_alias = _sql_alias

  @property
  def sql_alias(self) -> str:
    return self._sql_alias or self.dotted_path[-1]

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    return self._sql_data_type

  def dot(self,
          attribute: str,
          sql_data_type: StandardSqlDataType,
          sql_alias: Optional[str] = None) -> Identifier:
    """Builds an identifier for the attribute of this identifier.

    For example, Identifier('a').dot('b') renders SQL like 'SELECT a.b'.

    Args:
      attribute: The attribute on this identifier to select.
      sql_data_type: The type of the attribute being selected.
      sql_alias: The alias of the identifier.

    Returns:
      A new identifier representing the given path.
    """
    return Identifier((*self.dotted_path, attribute),
                      sql_data_type,
                      _sql_alias=sql_alias)

  def matches_alias(self, alias: str) -> bool:
    """Indicates whether an alias matches the identifier."""
    last_col = self.dotted_path[-1]
    return last_col == alias or f'`{last_col}`' == alias

  def __str__(self) -> str:
    return '.'.join(self.dotted_path)


@dataclasses.dataclass
class IsNullOperator(StandardSqlExpression):
  """Representation of an IS NULL statement.

  Renders SQL like 'SELECT `operand` IS NULL'.

  Attributes:
    operand: The expression being IS NULL'd.
  """
  operand: StandardSqlExpression
  _sql_alias: str = 'empty_'

  def __str__(self) -> str:
    return f'{self.operand} IS NULL'

  @property
  def sql_alias(self) -> str:
    return self._sql_alias

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    del self  # Needed to match StandardSqlExpression interface.
    return Boolean

  def as_operand(self) -> str:
    return f'({self})'


@dataclasses.dataclass
class IsNotNullOperator(StandardSqlExpression):
  """Representation of an IS NOT NULL statement.

  Renders SQL like 'SELECT `operand` IS NOT NULL'.

  Attributes:
    operand: The expression being IS NOT NULL'd.
  """
  operand: StandardSqlExpression
  _sql_alias: str = 'has_value_'

  def __str__(self) -> str:
    return f'{self.operand} IS NOT NULL'

  @property
  def sql_alias(self) -> str:
    return self._sql_alias

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    del self  # Needed to match StandardSqlExpression interface.
    return Boolean

  def as_operand(self) -> str:
    return f'({self})'


@dataclasses.dataclass
class SubQuery(StandardSqlExpression):
  """Representation of a subquery.

  Renders SQL like 'SELECT (`sql_expr`)'.

  Attributes:
    sql_expr: The expression being treated as a subquery.
  """
  sql_expr: StandardSqlExpression

  def __str__(self) -> str:
    return f'({self.sql_expr})'

  @property
  def sql_alias(self) -> str:
    return self.sql_expr.sql_alias

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    return self.sql_expr.sql_data_type


@dataclasses.dataclass
class FunctionCall(StandardSqlExpression):
  """Representation of a SQL function call.

  Attributes:
    name: The name of the function to call.
    params: The arguments to pass to the function.
  """
  name: str
  params: Sequence[StandardSqlExpression]
  _sql_data_type: StandardSqlDataType
  _sql_alias: Optional[str] = None

  def __str__(self) -> str:
    rendered_params = ', '.join(str(param) for param in self.params)
    return f'{self.name}(\n{rendered_params})'

  @property
  def sql_alias(self) -> str:
    return self._sql_alias or self.name.lower() + '_'

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    return self._sql_data_type


class CountCall(FunctionCall):
  """Provides short-hand for COUNT calls."""

  def __init__(self, params: Sequence[StandardSqlExpression]) -> None:
    super().__init__(
        name='COUNT', params=params, _sql_alias='count_', _sql_data_type=Int64)


class RegexpContainsCall(FunctionCall):
  """Provides short-hand for REGEXP_CONTAINS calls."""

  def __init__(self, params: Sequence[StandardSqlExpression]) -> None:
    super().__init__(
        name='REGEXP_CONTAINS',
        params=params,
        _sql_alias='matches_',
        _sql_data_type=Boolean)


@dataclasses.dataclass
class UnionExpression(StandardSqlExpression):
  """Represents a UNION Of two SELECT statements."""
  lhs: Select
  rhs: Select
  distinct: bool
  _sql_alias: str = 'union_'

  @property
  def sql_alias(self) -> str:
    return self._sql_alias

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    return coerce(self.lhs.sql_data_type, self.rhs.sql_data_type)

  def to_subquery(self) -> StandardSqlExpression:
    """Renders the expression as a subquery."""
    return SubQuery(self)

  def __str__(self):
    return (f'{self.lhs}\nUNION {"DISTINCT" if self.distinct else ""}\n'
            f'{self.rhs}')

  def as_operand(self) -> str:
    """Returns the simplest possible str of this expression.

    For use in other operations.
    """
    return str(self.to_subquery())


@dataclasses.dataclass
class Select(StandardSqlExpression):
  """Representation of a Standard SQL SELECT expression.

  Attributes:
    select_part: The expression being SELECT'd
    from_part: The body of the FROM clause. Optional to support subquery
      expressions taking their FROM from a parent query.
    where_part: The body of the WHERE clause.
    limit_part: The body of the LIMIT clause.
  """
  select_part: StandardSqlExpression
  from_part: Optional[str]
  where_part: Optional[str] = None
  limit_part: Optional[int] = None

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    return self.select_part.sql_data_type

  @property
  def sql_alias(self) -> str:
    return self.select_part.sql_alias

  def union(self, rhs: Select, distinct: bool) -> UnionExpression:
    """Builds a UNION with this and the given Select."""
    return UnionExpression(self, rhs, distinct)

  def to_subquery(self) -> StandardSqlExpression:
    """Renders the expression as a subquery."""
    return SubQuery(self)

  def __str__(self) -> str:
    """Builds the SQL expression from its given components."""
    query_parts = ['SELECT ']

    query_parts.append(str(self.select_part))
    # Add an AS statement to match sql_alias if necessary.
    if not self.select_part.matches_alias(self.sql_alias):
      query_parts.extend((' AS ', self.sql_alias))

    if self.from_part:
      query_parts.extend(('\n', 'FROM ', self.from_part))

    if self.where_part:
      query_parts.extend(('\n', 'WHERE ', self.where_part))

    if self.limit_part:
      query_parts.extend(('\n', 'LIMIT ', str(self.limit_part)))

    return ''.join(query_parts)

  def as_operand(self) -> str:
    """Returns select_part.as_operand() if this expression has no other parts.

    Otherwise it just returns the expression's __str__ representation in a
    subquery.

    Also excludes the inital `SELECT`.
    """
    # Wrap expressions that cannot stand on thier own in brackets.
    if self.from_part or self.where_part:
      return str(self.to_subquery())

    return self.select_part.as_operand()


@dataclasses.dataclass
class IdentifierSelect(Select):
  """Representation of a SELECT of an identifier.

  Identical to the Select class, but the select_part is restricted to
  identifiers rather than arbitrary expressions. This allows callers to access
  Identifier-specific behavior on the `select_part`.
  """
  select_part: Identifier

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
import enum
from typing import Any, List, Optional, Sequence, Set, Union, cast
from google.fhir.core.fhir_path import _fhir_path_data_types

# TODO(b/202892821): Consolidate with `_fhir_path_data_types.py` functionality.

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


class SqlDialect(enum.Enum):
  BIGQUERY = 'GoogleSQL'
  SPARK = 'SparkSQL'


# TODO(b/218912393): Add a consistent string representation to all the
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

  @property
  @abc.abstractmethod
  def big_query_type_name(self) -> str:
    """The name of the type as it appears in BigQuery DDL."""


class Array(StandardSqlDataType):
  """An ordered list of zero or more elements of non-`ARRAY` values.

  `ARRAY`s of `ARRAY`s are not allowed. Instead a `STRUCT` must be inserted
  between nested `ARRAY`s.

  Attributes:
    contained_type: The `StandardSqlDataType` contained within the `ARRAY`. If
      absent, then this is considered an "opaque ARRAY", and type information
      about the contained type is unknown.
  """

  def __init__(
      self, contained_type: Optional[StandardSqlDataType] = None
  ) -> None:
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

  @property
  def big_query_type_name(self) -> str:
    if self.contained_type is None:
      return 'ARRAY'

    return f'ARRAY<{self.contained_type.big_query_type_name}>'

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

  @property
  def big_query_type_name(self) -> str:
    return 'BOOL'

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

  @property
  def big_query_type_name(self) -> str:
    return 'BYTES'

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
    return set([Timestamp])

  @property
  def big_query_type_name(self) -> str:
    return 'DATE'

  def __str__(self):
    return '<DateSqlDataType>'


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

  @property
  def big_query_type_name(self) -> str:
    return 'GEOGRAPHY'

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

  @property
  def big_query_type_name(self) -> str:
    return 'INT64'

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

  @property
  def big_query_type_name(self) -> str:
    return 'NUMERIC'

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

  @property
  def big_query_type_name(self) -> str:
    return 'BIGNUMERIC'

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

  @property
  def big_query_type_name(self) -> str:
    return 'FLOAT64'

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

  @property
  def big_query_type_name(self) -> str:
    return 'STRING'

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

  def __init__(
      self, fields: Optional[List[StandardSqlDataType]] = None
  ) -> None:
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

  @property
  def big_query_type_name(self) -> str:
    if not self.fields:
      return 'STRUCT'

    struct_def = ', '.join(field.big_query_type_name for field in self.fields)
    return f'STRUCT<{struct_def}>'

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

  @property
  def big_query_type_name(self) -> str:
    return 'TIME'

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

  @property
  def big_query_type_name(self) -> str:
    return 'TIMESTAMP'

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

  @property
  def big_query_type_name(self) -> str:
    raise NotImplementedError('Not defined for Undefined type.')

  def __str__(self):
    return '<UndefinedSqlDataType>'


# Module-level instances for import+type inference. Note that parameterized
# types (such as `Array` and `Struct`) should be instantiated.
Boolean = _Boolean()
Bytes = _Bytes()
Date = _Date()
Timestamp = _Timestamp()
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

_FHIR_PATH_URL_TO_STANDARD_SQL_TYPE = {
    _fhir_path_data_types.Boolean.url: Boolean,
    _fhir_path_data_types.Integer.url: Int64,
    _fhir_path_data_types.Decimal.url: Numeric,
    _fhir_path_data_types.String.url: String,
    _fhir_path_data_types.Quantity.url: OpaqueStruct,
    _fhir_path_data_types.DateTime.url: Timestamp,
    _fhir_path_data_types.Date.url: Date,
    _fhir_path_data_types.Time.url: Time,
}


def get_standard_sql_data_type(
    fhir_type: _fhir_path_data_types.FhirPathDataType,
) -> StandardSqlDataType:
  """Gets the equivalent StandardSQL type for a fhir type."""
  if not fhir_type:
    return Undefined
  return_type = Undefined
  if isinstance(fhir_type, _fhir_path_data_types.StructureDataType):
    return_type = OpaqueStruct
  else:
    return_type = _FHIR_PATH_URL_TO_STANDARD_SQL_TYPE.get(fhir_type.url)
    return_type = return_type if return_type else Undefined

  if _fhir_path_data_types.is_collection(fhir_type):
    return_type = Array(contained_type=return_type)
  return return_type


def wrap_time_types(
    raw_sql: str,
    sql_type: StandardSqlDataType,
    sql_dialect: SqlDialect = SqlDialect.BIGQUERY,
) -> str:
  """If the type is a date/timestamp type, wrap the SQL statement with a CAST."""
  if isinstance(sql_type, Array):
    sql_type = cast(Array, sql_type).contained_type

  if raw_sql.startswith('TO_TIMESTAMP'):
    return raw_sql

  if raw_sql.startswith('CAST'):
    return raw_sql

  if raw_sql.startswith('SAFE_CAST'):
    return raw_sql

  if (
      isinstance(sql_type, (_Timestamp, _Date))
      and sql_dialect is SqlDialect.BIGQUERY
  ):
    return f'SAFE_CAST({raw_sql} AS TIMESTAMP)'

  if (
      isinstance(sql_type, (_Timestamp, _Date))
      and sql_dialect is SqlDialect.SPARK
  ):
    return f'CAST({raw_sql} AS TIMESTAMP)'

  return raw_sql


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

  # TODO(b/221322122): Remove this special case with a common SQL flow.
  if isinstance(lhs, type(Undefined)):
    return True  # Assume unknown types can be coerced, like pytype and Any.

  if isinstance(rhs, (Array, Struct)) or isinstance(lhs, (Array, Struct)):
    return False  # Early-exit if either operand is a complex type

  if rhs in lhs.supported_coercion and lhs in rhs.supported_coercion:
    raise ValueError(f'Coercion cycle between: {lhs} and {rhs}.')

  return rhs in lhs.supported_coercion or lhs in rhs.supported_coercion


def coerce(
    lhs: StandardSqlDataType, rhs: StandardSqlDataType
) -> StandardSqlDataType:
  """Performs implicit Standard SQL coercion between two datatypes.

  See more at:
  https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules

  Args:
    lhs: The left operand.
    rhs: The right operand.

  Returns:
    The resulting coerced datatype, if successful.

  Raises:
    ValueError: In the event that a coercion cycle is detected or coercion is
      not supported.
  """
  if not is_coercible(lhs, rhs):
    raise ValueError(
        f'Unsupported Standard SQL coercion between {lhs} and {rhs}.'
    )

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

  def or_(self, rhs: StandardSqlExpression, **kwargs) -> OrExpression:
    """Builds an OR expression for `self` OR `rhs`."""
    return OrExpression(self, rhs, **kwargs)

  def and_(self, rhs: StandardSqlExpression, **kwargs) -> AndExpression:
    """Builds an AND expression for `self` AND `rhs`."""
    return AndExpression(self, rhs, **kwargs)

  def eq_(self, rhs: StandardSqlExpression, **kwargs) -> EqualsExpression:
    """Builds an equals expression for `self` = `rhs`."""
    return EqualsExpression(self, rhs, **kwargs)

  def in_(
      self, params: Sequence[StandardSqlExpression], **kwargs
  ) -> InOperator:
    """Builds an IN expression for `self` IN `params`."""
    return InOperator(self, params, **kwargs)

  def cast(
      self, cast_to: StandardSqlDataType, **kwargs
  ) -> StandardSqlExpression:
    """Builds a CAST call for this expression as type `cast_to`."""
    return CastFunction(self, cast_to, **kwargs)

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

  def __init__(
      self,
      name: Union[str, Sequence[str]],
      _sql_data_type: StandardSqlDataType,  # pylint:disable=invalid-name matches names on other classes
      _sql_alias: Optional[str] = None,  # pylint:disable=invalid-name matches names on other classes
  ) -> None:
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

  def dot(
      self,
      attribute: str,
      sql_data_type: StandardSqlDataType,
      sql_alias: Optional[str] = None,
  ) -> Identifier:
    """Builds an identifier for the attribute of this identifier.

    For example, Identifier('a').dot('b') renders SQL like 'SELECT a.b'.

    Args:
      attribute: The attribute on this identifier to select.
      sql_data_type: The type of the attribute being selected.
      sql_alias: The alias of the identifier.

    Returns:
      A new identifier representing the given path.
    """
    return Identifier(
        (*self.dotted_path, attribute), sql_data_type, _sql_alias=sql_alias
    )

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
    operand_str = str(self.operand)
    if 'FROM' in operand_str or 'WHERE' in operand_str:
      return f'SELECT ({self.operand}) IS NOT NULL'
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
class InOperator(StandardSqlExpression):
  """Representation of an IN statement.

  Renders SQL like 'SELECT `operand` IN (x, y...)'.

  Attributes:
    operand: The expression on the left hand side of the IN.
    parameters: The set of expressions on the right hand side of the IN.
  """

  operand: StandardSqlExpression
  parameters: Sequence[StandardSqlExpression]
  _sql_alias: str = 'in_'

  def __str__(self) -> str:
    param_str = ', '.join(str(param) for param in self.parameters)
    return f'{self.operand} IN ({param_str})'

  @property
  def sql_alias(self) -> str:
    return self._sql_alias

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    del self
    return Boolean

  def as_operand(self) -> str:
    return f'({self})'


@dataclasses.dataclass
class BindaryBooleanExpression(StandardSqlExpression):
  """Base class for OR, =, AND, etc expressions.

  Attributes:
    lhs: The expression on the left hand side of the expression.
    rhs: The expression on the right hand side of the expression.
  """

  lhs: StandardSqlExpression
  rhs: StandardSqlExpression
  _operator_name: str
  _sql_alias: str

  def __str__(self) -> str:
    return (
        f'{self.lhs.as_operand()} {self._operator_name} {self.rhs.as_operand()}'
    )

  @property
  def sql_alias(self) -> str:
    return self._sql_alias

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    del self
    return Boolean

  def as_operand(self) -> str:
    return f'({self})'


class OrExpression(BindaryBooleanExpression):
  """Representation of an OR expression."""

  def __init__(
      self,
      lhs: StandardSqlExpression,
      rhs: StandardSqlExpression,
      _sql_alias: str = 'or_',
  ):
    super().__init__(lhs, rhs, 'OR', _sql_alias)


@dataclasses.dataclass
class AndExpression(BindaryBooleanExpression):
  """Representation of an AND expression."""

  def __init__(
      self,
      lhs: StandardSqlExpression,
      rhs: StandardSqlExpression,
      _sql_alias: str = 'and_',
  ):
    super().__init__(lhs, rhs, 'AND', _sql_alias)


@dataclasses.dataclass
class EqualsExpression(BindaryBooleanExpression):
  """Representation of an equals expression."""

  def __init__(
      self,
      lhs: StandardSqlExpression,
      rhs: StandardSqlExpression,
      _sql_alias: str = 'eq_',
  ):
    super().__init__(lhs, rhs, '=', _sql_alias)


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
class CastFunction(StandardSqlExpression):
  """Representation of a SQL cast.

  Attributes:
    expression: The expression being cast.
    cast_to: The type the expression is being cast to.
  """

  expression: StandardSqlExpression
  cast_to: StandardSqlDataType
  _sql_alias: Optional[str] = None

  def __str__(self) -> str:
    return f'CAST(\n{self.expression} AS {self.cast_to.big_query_type_name})'

  @property
  def sql_alias(self) -> str:
    return self._sql_alias or 'cast_'

  @property
  def sql_data_type(self) -> StandardSqlDataType:
    return self.cast_to


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
        name='COUNT', params=params, _sql_alias='count_', _sql_data_type=Int64
    )


class RegexpContainsCall(FunctionCall):
  """Provides short-hand for REGEXP_CONTAINS calls."""

  def __init__(self, params: Sequence[StandardSqlExpression]) -> None:
    super().__init__(
        name='REGEXP_CONTAINS',
        params=params,
        _sql_alias='matches_',
        _sql_data_type=Boolean,
    )


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
    return (
        f'{self.lhs}\nUNION {"DISTINCT" if self.distinct else ""}\n{self.rhs}'
    )

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
    sql_dialect: The SQL dialect to use. Defaults to BigQuery
  """

  select_part: StandardSqlExpression
  from_part: Optional[str]
  where_part: Optional[str] = None
  limit_part: Optional[int] = None
  sql_dialect: Optional[SqlDialect] = SqlDialect.BIGQUERY

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

    select_part = wrap_time_types(
        str(self.select_part), self.sql_data_type, self.sql_dialect
    )
    query_parts.append(select_part)
    # Add an AS statement to match sql_alias if necessary.
    if select_part != str(
        self.select_part
    ) or not self.select_part.matches_alias(self.sql_alias):
      query_parts.extend((' AS ', str(self.sql_alias)))

    if self.from_part:
      query_parts.extend(f'\nFROM {self.from_part}')

    if self.where_part:
      query_parts.extend(('\n', 'WHERE ', str(self.where_part)))

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

    return wrap_time_types(
        self.select_part.as_operand(), self.sql_data_type, self.sql_dialect
    )


@dataclasses.dataclass
class IdentifierSelect(Select):
  """Representation of a SELECT of an identifier.

  Identical to the Select class, but the select_part is restricted to
  identifiers rather than arbitrary expressions. This allows callers to access
  Identifier-specific behavior on the `select_part`.
  """

  select_part: Identifier

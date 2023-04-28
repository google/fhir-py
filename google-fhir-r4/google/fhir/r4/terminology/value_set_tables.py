#
# Copyright 2022 Google LLC
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
"""Utilities for maintaining value set codes database tables.

Read more about value set codes tables here:
https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support
"""

import itertools
from typing import Collection, Dict, Iterable, Tuple

import logging
import sqlalchemy

from google.fhir.r4.proto.core.resources import value_set_pb2


def valueset_codes_insert_statement_for(
    expanded_value_sets: Iterable[value_set_pb2.ValueSet],
    table: sqlalchemy.sql.expression.TableClause,
    batch_size: int = 500,
) -> Iterable[sqlalchemy.sql.dml.Insert]:
  """Builds INSERT statements for placing value sets' codes into a given table.

  The INSERT may be used to build a valueset_codes table as described by:
  https://github.com/FHIR/sql-on-fhir/blob/master/sql-on-fhir.md#valueset-support

  Returns an sqlalchemy insert expression which inserts all of the value set's
  expanded codes into the given table which do not already exist in the table.
  The query will avoid inserting duplicate rows if some of the codes are already
  present in the given table. It will not attempt to perform an 'upsert' or
  modify any existing rows.

  Args:
    expanded_value_sets: The expanded value sets with codes to insert into the
      given table. The value sets should have already been expanded, for
      instance by a ValueSetResolver or TerminologyServiceClient's
      expand_value_set_url method.
    table: The SqlAlchemy table to receive the INSERT. May be an sqlalchemy
      Table or TableClause object. The table is assumed to have the columns
      'valueseturi', 'valuesetversion', 'system', 'code.'
    batch_size: The maximum number of rows to insert in a single query.

  Yields:
    The sqlalchemy insert expressions which you may execute to perform the
    actual database writes. Each yielded insert expression will insert at most
    batch_size number of rows.
  Raises:
    ValueError: If the given table does not have the expected columns.
  """
  expected_cols = ('valueseturi', 'valuesetversion', 'system', 'code')
  missing_cols = [col for col in expected_cols if col not in table.columns]
  if missing_cols:
    raise ValueError(
        'Table %s missing expected columns: %s'
        % (table, ', '.join(missing_cols))
    )

  def value_set_codes() -> (
      Iterable[
          Tuple[
              value_set_pb2.ValueSet, value_set_pb2.ValueSet.Expansion.Contains
          ]
      ]
  ):
    """Yields (value_set, code) tuples for each code in each value set."""
    for value_set in expanded_value_sets:
      if not value_set.expansion.contains:
        logging.warning(
            'Value set: %s version: %s has no expanded codes',
            value_set.url.value,
            value_set.version.value,
        )
      for code in value_set.expansion.contains:
        yield value_set, code

  # Break the value set codes into batches.
  batch_iterables = [iter(value_set_codes())] * batch_size
  batches = itertools.zip_longest(*batch_iterables)

  for batch in batches:
    # Build a SELECT statement for each code.
    code_literals = []
    for pair in batch:
      # The last batch will have `None`s padding it out to `batch_size`.
      if pair is not None:
        value_set, code = pair
        code_literals.append(_code_as_select_literal(value_set, code))

    # UNION each SELECT to build a single select subquery for all codes.
    codes = sqlalchemy.union_all(*code_literals).alias('codes')
    # Filter the codes to those not already present in `table` with a LEFT JOIN.
    new_codes = (
        sqlalchemy.select(codes)
        .select_from(
            codes.outerjoin(
                table,
                sqlalchemy.and_(
                    codes.c.valueseturi == table.c.valueseturi,
                    codes.c.valuesetversion == table.c.valuesetversion,
                    codes.c.system == table.c.system,
                    codes.c.code == table.c.code,
                ),
            )
        )
        .where(
            sqlalchemy.and_(
                table.c.valueseturi.is_(None),
                table.c.valuesetversion.is_(None),
                table.c.system.is_(None),
                table.c.code.is_(None),
            )
        )
    )
    yield table.insert().from_select(new_codes.subquery().columns, new_codes)


def get_num_code_systems_per_value_set(
    engine: sqlalchemy.engine.base.Engine,
    table: sqlalchemy.sql.expression.TableClause,
) -> Dict[str, int]:
  """Queries `table` for the code systems referenced by each value set.

  Looks up the code systems referenced by each value set decribed in the
  valueset_codes `table`. Returns counts for the number of code systems
  referenced by each value set.

  If the value sets' URL contains a "|version" suffix, reports the number of
  code systems referenced by that value set and version.
  If the url does not contain a "|version" suffix, the number of code systems
  across all versions of the value set will be reported.

  Args:
    engine: The SqlAlachemy engine to use when performing queries.
    table: The SqlAlchemy table to query.

  Returns:
    A CodeSystemCounts object for accessing code systems information.
  """
  query = sqlalchemy.select([
      table.c.valueseturi,
      table.c.valuesetversion,
      sqlalchemy.func.array_agg(sqlalchemy.distinct(table.c.system)).label(
          'systems'
      ),
  ]).group_by(table.c.valueseturi, table.c.valuesetversion)
  with engine.connect() as connection:
    systems_per_value_set = connection.execute(query)
    return _query_results_to_code_system_counts(
        systems_per_value_set.fetchall()
    )


def _query_results_to_code_system_counts(
    query_results
) -> Dict[str, int]:
  """Converts query results to a map of value set to code system counts."""
  # Build a map of {value_set_url: {value_set_version: [code_systems]}}
  systems_per_value_set: Dict[str, Dict[str, Collection[str]]] = {}
  for row in query_results:
    value_set_version = row.valuesetversion or ''
    systems_per_value_set.setdefault(row.valueseturi, {})[
        value_set_version
    ] = row.systems

  # Convert the above map to {value_set_url|version: num_codes_systems}
  # Rows without versions inherit code systems from all value sets with the same
  # base URL.
  num_systems_per_url: Dict[str, int] = {}
  for value_set_url, systems_per_version in systems_per_value_set.items():
    for version, systems in systems_per_version.items():
      if version:
        num_systems_per_url['%s|%s' % (value_set_url, version)] = len(systems)
      # Also add a lookup for the version-less URL.
      num_systems_per_url[value_set_url] = len(
          set(itertools.chain.from_iterable(systems_per_version.values()))
      )
  return num_systems_per_url


def _code_as_select_literal(
    value_set: value_set_pb2.ValueSet,
    code: value_set_pb2.ValueSet.Expansion.Contains,
) -> sqlalchemy.select:
  """Builds a SELECT statement for the literals in the given code."""
  return sqlalchemy.select(
      _literal_or_null(value_set.url.value).label('valueseturi'),
      _literal_or_null(value_set.version.value).label('valuesetversion'),
      _literal_or_null(code.system.value).label('system'),
      _literal_or_null(code.code.value).label('code'),
  )


def _literal_or_null(val: str) -> sqlalchemy.sql.elements.ColumnElement:
  """Returns a literal for the given string or NULL for an empty string."""
  if val:
    return sqlalchemy.sql.expression.literal(val)
  else:
    return sqlalchemy.null()

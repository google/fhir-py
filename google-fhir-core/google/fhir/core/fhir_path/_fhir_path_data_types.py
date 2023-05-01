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
"""Functionality for FHIRPath datatype inference and validation.

Scalar types are declared as module-level constants that can be imported by
dependent modules. Parameterized types such as `Collection` should be
instantiated directly, so that the relevant contained type information can be
provided by the caller.
"""

import abc
import collections
import copy
import dataclasses
import enum
import itertools
import re

from typing import Any, Dict, Iterable, List, Mapping, Optional, Set, Sequence, Tuple, cast, Collection as CollectionType
from google.protobuf import message

# Tokens that are keywords in FHIRPath.
# If used as identifier tokens in FHIRPath expressions, they must be escaped
# with backticks (`).
RESERVED_FHIR_PATH_KEYWORDS = frozenset([
    '$index',
    '$this',
    '$total',
    'and',
    'as',
    'contains',
    'day',
    'days',
    'div',
    'false',
    'hour',
    'hours',
    'implies',
    'in',
    'is',
    'millisecond',
    'milliseconds',
    'minute',
    'minutes',
    'mod',
    'month',
    'months',
    'or',
    'seconds',
    'true',
    'week',
    'weeks',
    'xor',
    'year',
    'years',
    'second',
])


@enum.unique
class Cardinality(enum.Enum):
  """Defines the cardinality of a FhirPathDataType.

  There are three different cardinalities that need to be captured:
    1. A FHIRPath expression always returns one value (e.g., exists() or
       count()).
    2. A FHIRPath expression can return one or more values (e.g., an invoke
       expression on a repeated field or where(...) function).
    3. A FHIRPath expression that returns the same number of values as its
       parent. (e.g., an invoke expression on a single-valued field that is a
       child of a multi-valued parent) -- so it could return a single value if
       the parent is single-value, or multi-value if the parent is as well.
  """

  SCALAR = 'scalar'
  COLLECTION = 'collection'
  CHILD_OF_COLLECTION = 'child_of_collection'


# TODO(b/202892821): Consolidate with `_sql_data_types.py` functionality.


@dataclasses.dataclass(frozen=True)
class Slice:
  """A container for all element definitions describing a slice.

  https://build.fhir.org/profiling.html#slicing

  Attributes:
    slice_def: The element definition describing the slice itself.
    relative_path: The path to the sliced collection relative to the structure
      definition defining the slice.
    slice_rules: Tuples of (relative_path, element_definition) for the element
      definitions describing the contents of the slice and the path to them
      relaitve to the structure definition defining the slice.
  """

  slice_def: message.Message
  relative_path: str
  slice_rules: Sequence[Tuple[str, message.Message]]


@dataclasses.dataclass
class _SliceBuilder:
  """An internal class used to incerementally build Slice instances.

  Its attributes are Optional because the _SliceBuilder class is built
  incrementally. These attributes are not always available
  initially. A _SliceBuilder instance should be converted to a Slice
  instance and then provided to callers.
  """

  slice_def: Optional[message.Message]
  relative_path: Optional[str]
  slice_rules: List[Tuple[str, message.Message]]

  def to_slice(self) -> Slice:
    assert self.slice_def is not None, 'slice_def is unexpectedly None'
    assert self.relative_path is not None, 'relative_path is unexpectedly None'
    return Slice(self.slice_def, self.relative_path, self.slice_rules)


class FhirPathDataType(metaclass=abc.ABCMeta):
  """An abstract base class defining a FHIRPath system primitive.

  Traversal implicitly converts FHIR types representing primitives to instances
  of `FhirPathDataType` types according to the logic at:
  https://www.hl7.org/fhir/fhirpath.html#types.

  Attributes:
    cardinality: Determines how many values are returned for the type.
    root_element_definition: The root element definition that contains
      constraints and restrictions of the particular type.
    comparable: Values of the same type can be compared to each other.
    supported_coercion: A set of `FhirPathDataType`s depicting allowable
      implicit conversion.
    url: The canonical URL reference to the data type.
    child_defs: A mapping of name to element definition for each child element
      of the structure definition, including slices on extension. Empty for
      other data types.
  """

  _cardinality: Cardinality = Cardinality.SCALAR
  _root_element_definition: message.Message = None

  @property
  @abc.abstractmethod
  def supported_coercion(self) -> Set['FhirPathDataType']:
    pass

  @property
  @abc.abstractmethod
  def url(self) -> str:
    pass

  @property
  def comparable(self) -> bool:
    return self._comparable

  @property
  def cardinality(self) -> Cardinality:
    return self._cardinality

  def get_new_cardinality_type(
      self, cardinality: Cardinality
  ) -> 'FhirPathDataType':
    obj_copy = copy.deepcopy(self)
    # pylint: disable=protected-access
    obj_copy._cardinality = cardinality
    # pylint: enable=protected-access
    return obj_copy

  def copy_fhir_type_with_root_element_definition(
      self, root_element_definition: message.Message
  ) -> 'FhirPathDataType':
    """Copies the type and sets the root_element_definition.

    Args:
      root_element_definition: Element definition to set for the type.

    Returns:
      A copy of the original type with the root_element_definition set.
    """
    obj_copy = copy.deepcopy(self)
    # pylint: disable=protected-access
    obj_copy._root_element_definition = root_element_definition
    # pylint: enable=protected-access
    return obj_copy

  def returns_collection(self) -> bool:
    return (
        self._cardinality == Cardinality.COLLECTION
        or self._cardinality == Cardinality.CHILD_OF_COLLECTION
    )

  def returns_polymorphic(self) -> bool:
    """Indicates if the type returns a polymorphic choice type."""
    return False

  def fields(self) -> Set[str]:
    return set()

  @property
  def child_defs(self) -> Mapping[str, message.Message]:
    return {}

  @property
  def root_element_definition(self) -> Optional[message.Message]:
    return self._root_element_definition

  def __init__(self, *, comparable: bool = False) -> None:
    self._comparable = comparable

  def __eq__(self, o) -> bool:
    if isinstance(o, FhirPathDataType):
      return cast(FhirPathDataType, o).url == self.url
    return False

  def __hash__(self) -> int:
    return hash(self.url)

  def _wrap_collection(self, name: str) -> str:
    return f'[{name}]' if self.returns_collection() else name

  @abc.abstractmethod
  def _class_name(self) -> str:
    pass

  def __str__(self) -> str:
    return self._wrap_collection(self._class_name())


class _Boolean(FhirPathDataType):
  """Represents the logical Boolean values `true` and `false`.

  These values are used as the result of comparisons, and can be combined using
  logical operators such as `and` and `or`.

  See more at: https://hl7.org/fhirpath/#boolean.
  """

  _URL = 'http://hl7.org/fhirpath/System.Boolean'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self) -> None:
    super().__init__(comparable=False)

  def _class_name(self) -> str:
    return '<BooleanFhirPathDataType>'


class _Date(FhirPathDataType):
  """Represents date and partial date values.

  Values are within the range @0001-01-01 to @9999-12-31 with a 1 day step size.

  See more at: https://hl7.org/fhirpath/#date.
  """

  _URL = 'http://hl7.org/fhirpath/System.Date'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([DateTime])

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self) -> None:
    super().__init__(comparable=True)

  def _class_name(self) -> str:
    return '<DateFhirPathDataType>'


class _Time(FhirPathDataType):
  """Represents time-of-day and partial time-of-day values.

  Values are in the range @T00:00:00.000 to @T23:59:59.999 with a step size of
  1 millisecond. The Time literal uses a subset of [ISO8601].

  See more at: https://hl7.org/fhirpath/#time.
  """

  _URL = 'http://hl7.org/fhirpath/System.Time'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self) -> None:
    super().__init__(comparable=True)

  def _class_name(self) -> str:
    return '<TimeFhirPathDataType>'


class _DateTime(FhirPathDataType):
  """Represents date/time and partial date/time values.

  Values are within the range @0001-01-01T00:00:00.000 to
  @9999-12-31T23:59:59.999 with a 1 millisecond step size. The `DateTime`
  literal combines the `Date` and `Time` literals and is a subset of ISO8601.

  See more at: https://hl7.org/fhirpath/#datetime.
  """

  _URL = 'http://hl7.org/fhirpath/System.DateTime'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self) -> None:
    super().__init__(comparable=True)

  def _class_name(self) -> str:
    return '<DateTimeFhirPathDataType>'


class _Decimal(FhirPathDataType):
  """Represents real values.

  Values are within the range (-10^28 + 1)/10^8 to (10^28-1)/10^8 with a step
  size of 10^(-8).

  See more at: https://hl7.org/fhirpath/#decimal.
  """

  _URL = 'http://hl7.org/fhirpath/System.Decimal'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Quantity])

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self) -> None:
    super().__init__(comparable=True)

  def _class_name(self) -> str:
    return '<DecimalFhirPathDataType>'


class _Integer(FhirPathDataType):
  """Represents whole numbers in the range -2^31 to 2^31 - 1.

  Note that an integer's polarity is part of a FHIRPath expression, and not the
  type itself.

  See more at: https://hl7.org/fhirpath/#integer.
  """

  _URL = 'http://hl7.org/fhirpath/System.Integer'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Decimal, Quantity])

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self) -> None:
    super().__init__(comparable=True)

  def _class_name(self) -> str:
    return '<IntegerFhirPathDataType>'


class _Quantity(FhirPathDataType):
  """Represents quantities with a specified unit.

  The `value` component is defined as a decimal, and the `unit` element is
  represented as a `String` that is required to be either a valid Unified Code
  for Units of Measure unit or one of the calendar duration keywords, singular
  or plural.

  See more at: https://hl7.org/fhirpath/#quantity.
  """

  _URL = 'http://hl7.org/fhirpath/System.Quantity'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self, comparable: bool = True, **kwargs) -> None:
    super().__init__(comparable=comparable, **kwargs)

  def _class_name(self) -> str:
    return '<QuantityFhirPathDataType>'


class _Reference(FhirPathDataType):
  """Represents a FHIR Reference.

  See more at: https://build.fhir.org/references.html
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Reference'

  def __init__(self) -> None:
    super().__init__(comparable=False)

  def _class_name(self) -> str:
    return '<ReferenceFhirPathDataType>'


class _String(FhirPathDataType):
  r"""Represents string values up to 2^31 - 1 characters in length.

  String literals are surrounded by single-quotes and may use '\'-escapes to
  escape quotes and represent Unicode characters.

  See more at: https://hl7.org/fhirpath/#string.
  """

  _URL = 'http://hl7.org/fhirpath/System.String'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return self._URL

  def __init__(self) -> None:
    super().__init__(comparable=True)

  def _class_name(self) -> str:
    return '<StringFhirPathDataType>'


class _Empty(FhirPathDataType):
  """Represents the absence of a value in FHIRPath.

  There is no literal representation for `null` in FHIRPath. This means that,
  in an underlying data object (i.e. the physical data on which the
  implementation is operating) a member is null or missing, there will simply be
  no corresponding node for that member in the tree.

  See more at: https://hl7.org/fhirpath/#null-and-empty.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self):
    return None

  def __init__(self) -> None:
    super().__init__(comparable=False)

  def __eq__(self, o: Any) -> bool:
    return isinstance(o, _Empty)

  def _class_name(self) -> str:
    return '<EmptyFhirPathDataType>'

  def __hash__(self) -> int:
    return hash(self._class_name())


class Collection(FhirPathDataType):
  """A heterogeneous ordered group of FHIRPath primitive datatypes.

  In FHIRPath, the result of every expression is a collection, even if that
  expression only results in a single element.

  This Collection type holds a set containing all the types currently in it.

  Note: It is not a concrete implementation of `Collection`, so it only stores
  one instance of every type that is present.
  """

  def __init__(self, types: Set[FhirPathDataType]) -> None:
    super().__init__(comparable=False)
    self._types: Set[FhirPathDataType] = types

  def __eq__(self, o: Any) -> bool:
    if not isinstance(o, Collection):
      return False

    return self._types == cast(Collection, o).types

  def __len__(self) -> int:
    return len(self._types)

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()

  @property
  def url(self):
    return None

  @property
  def types(self):
    return self._types

  def fields(self) -> Set[str]:
    result = []
    for t in self._types:
      result += t.fields()
    return set(result)

  def _class_name(self) -> str:
    return (
        '<CollectionFhirPathDataType(types='
        f'{[str(types) for types in self._types]})>'
    )


class StructureDataType(FhirPathDataType):
  """The FHIR specification data types used in the resource elements.

  Their definitions are typically provided by FHIR StructureDefinitions.
  See https://www.hl7.org/fhir/datatypes.html
  """

  @property
  def supported_coercion(self) -> Set['FhirPathDataType']:
    return set()

  @property
  def url(self) -> str:
    return (
        '.'.join([self._url, self.backbone_element_path])
        if self.backbone_element_path
        else self._url
    )

  @property
  def base_type(self) -> str:
    return self._base_type

  @property
  def element_type(self) -> str:
    return self._element_type

  @property
  def structure_definition(self) -> message.Message:
    return self._struct_def

  @property
  def backbone_element_path(self) -> Optional[str]:
    """Optional path to non-root backbone element to use."""
    return self._backbone_element_path

  def __init__(
      self,
      struct_def_proto: message.Message,
      backbone_element_path: Optional[str] = None,
      comparable: bool = False,
      element_type: Optional[str] = None,
  ) -> None:
    super().__init__(comparable=comparable)
    self._struct_def = cast(Any, struct_def_proto)
    self._url = self._struct_def.url.value
    self._base_type = self._struct_def.type.value
    # For some custom types, the element type differs from the base type.
    self._element_type = element_type if element_type else self._base_type
    self._backbone_element_path = backbone_element_path

    # For backbone elements, prepend their paths with the path to the
    # root of the backbone element.
    qualified_path = (
        f'{self._element_type}.{self._backbone_element_path}'
        if self._backbone_element_path
        else self._element_type
    )

    self._child_defs = {}
    self._direct_children = []
    self._other_descendants = []
    # A map of slice ID (e.g. some.path:SomeSlice) to the _SliceBuilder
    # object representing that slice.
    slices: dict[str, _SliceBuilder] = collections.defaultdict(
        lambda: _SliceBuilder(None, None, [])
    )

    for elem in self._struct_def.snapshot.element:
      path = _get_analytic_path(elem.path.value, elem.id.value)

      if path == qualified_path:
        self._root_element_definition = elem
        continue

      if re.search(rf'^{qualified_path}\.\w+', path):
        relative_path = path[len(qualified_path) + 1 :]

        # One path may have multiple element definitions due to
        # slices. The path will have one element definition providing
        # its base definition and additional element definitions for
        # each slice of that path. We only place the base element
        # definition in the children dictionary, as callers currently
        # expect to be able to find these definitions in this
        # dictionary.

        # Check to see if the element id contains a slice ID
        # (e.g. some.path:SomeSlice) but not a slice on extension.
        # Capture the slice ID, e.g. 'some.path:SomeSlice' given an id
        # like 'some.path:SomeSlice.field' so we can later group all
        # elements describing the same slice together. We use the
        # closest_slice_ancestor below to aggregate element
        # definitions describing the same slice. We do not need to do
        # this for slices on extensions, as they have a unique field
        # in the analytic schema, and thus are treated as fields
        # rather than slices.
        closest_slice_ancestor = re.search(
            rf'^{qualified_path}\.(.+(?<!.extension):\w+)(?:$|\.)',
            elem.id.value,
        )
        direct_child = '.' not in relative_path
        if direct_child and closest_slice_ancestor is None:
          assert relative_path not in self.child_defs, (
              f'{relative_path} found twice among children in structure'
              f' definition {self._struct_def.url.value}'
          )
          self._child_defs[relative_path] = elem

        if closest_slice_ancestor is not None:
          # Gather all the element definitions which describe the same
          # slice into a single data structure.
          slice_def = slices[closest_slice_ancestor[1]]
          if elem.slice_name.value:
            # This is the definition for the slice itself, e.g. Foo.bar:baz.
            slice_def.slice_def = elem
            slice_def.relative_path = relative_path
          else:
            # This is a constraint describing the slice, e.g. Foo.bar:baz.quux.
            slice_def.slice_rules.append((relative_path, elem))
        elif direct_child:
          self._direct_children.append((relative_path, elem))
        else:
          self._other_descendants.append((relative_path, elem))

    if not self._root_element_definition:
      raise ValueError(
          f'StructureDataType {self._url} searching on {qualified_path} '
          f' missing root element definition. {self._struct_def}'
      )

    self._slices = tuple(slice_def.to_slice() for slice_def in slices.values())

  def __eq__(self, o) -> bool:
    if isinstance(o, StructureDataType):
      return cast(StructureDataType, o).url == self.url
    return False

  def __hash__(self) -> int:
    return hash(self.url)

  def _class_name(self) -> str:
    return f'<StructureFhirPathDataType(url={self.url})>'

  def fields(self) -> Set[str]:
    return set(self._child_defs.keys())

  @property
  def child_defs(self) -> Mapping[str, message.Message]:
    return self._child_defs

  def iter_children(self) -> Iterable[Tuple[str, message.Message]]:
    """Returns an iterator over all direct child element definitions.

    Contains all entries in `child_defs`. Does not contain slices.
    """
    return iter(self._direct_children)

  def iter_all_descendants(self) -> Iterable[Tuple[str, message.Message]]:
    """Returns an iterator over all element definitions.

    Contains all entries in `iter_children`, as well as additional element
    definitions for elements describing descendants deeper than direct children.
    Does not contain slices.
    """
    return itertools.chain(self._direct_children, self._other_descendants)

  def iter_slices(self) -> Iterable[Slice]:
    """Returns an iterator over all slices.

    Contains both direction children and descendants deeper than direct
    children.
    """
    return iter(self._slices)


class QuantityStructureDataType(StructureDataType, _Quantity):
  """Represents quantity FHIR specification data types."""

  _URL = 'http://hl7.org/fhirpath/System.Quantity'

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Quantity])

  @property
  def url(self) -> str:
    return self._URL

  def __init__(
      self,
      struct_def_proto: message.Message,
      backbone_element_path: Optional[str] = None,
  ) -> None:
    super().__init__(
        struct_def_proto=struct_def_proto,
        backbone_element_path=backbone_element_path,
        comparable=True,
    )

  def _class_name(self) -> str:
    return f'<QuantityStructureFhirPathDataType(url={self.url})>'


class ReferenceStructureDataType(StructureDataType):
  """Represents a FHIR Reference.

  See more at: https://build.fhir.org/references.html

  Attributes:
    target_profiles: The types of resources to which the reference may link. See
      more at:
      https://build.fhir.org/elementdefinition-definitions.html#ElementDefinition.type.targetProfile
  """

  target_profiles: CollectionType[str]

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Reference])

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Reference'

  def __init__(
      self,
      struct_def_proto: message.Message,
      element_definition: message.Message,
      backbone_element_path: Optional[str] = None,
  ) -> None:
    super().__init__(
        struct_def_proto=struct_def_proto,
        backbone_element_path=backbone_element_path,
        comparable=False,
    )
    self.target_profiles = [
        profile.value
        for profile in cast(Any, element_definition).type[0].target_profile
    ]

  def _class_name(self) -> str:
    return f'<ReferenceStructureFhirPathDataType(url={self.url})>'


class _Any(FhirPathDataType):
  """Represents any type in FHIRPath.

  Used in situations where we cannot determine the type.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self):
    return None

  # We don't restrict what the Any type can be compared to.
  def __init__(self) -> None:
    super().__init__(comparable=True)

  def __eq__(self, o: Any) -> bool:
    return isinstance(o, _Any)

  def _class_name(self) -> str:
    return '<AnyFhirPathDataType>'

  def __hash__(self) -> int:
    return hash(self._class_name())


class PolymorphicDataType(FhirPathDataType):
  """A heterogeneous ordered group of FhirPathDataTypes.

  In FHIRPath, some fields are polymorphic and can be multiple types.
  This type stores all the possible types a field can be.

  See more at: https://hl7.org/fhirpath/#paths-and-polymorphic-items.
  """

  def returns_polymorphic(self) -> bool:
    return True

  @property
  def supported_coercion(self) -> Set['FhirPathDataType']:
    return set()

  @property
  def url(self) -> str:
    return list(self._urls)[0] if self._urls else ''

  @property
  def urls(self) -> Set[str]:
    return self._urls

  def types(self) -> Dict[str, FhirPathDataType]:
    return self._types

  def fields(self) -> Set[str]:
    return set(self._types.keys())

  def __init__(self, types: Dict[str, FhirPathDataType]) -> None:
    super().__init__(comparable=False)
    self._types = types
    self._urls = set([t.url for _, t in types.items()])

  def __eq__(self, o) -> bool:
    if isinstance(o, PolymorphicDataType):
      return cast(PolymorphicDataType, o).urls == self.urls
    return False

  def __hash__(self) -> int:
    return hash(' '.join(self.urls))

  def _class_name(self) -> str:
    type_name_strings = [f'{name}: {t.url}' for name, t in self._types.items()]
    return f'<PolymorphicDataType(types={type_name_strings})>'


# Module-level instances for import+type inference.
Boolean = _Boolean()
Date = _Date()
Time = _Time()
DateTime = _DateTime()
Decimal = _Decimal()
Integer = _Integer()
Quantity = _Quantity()
Reference = _Reference()
String = _String()

Empty = _Empty()
Any_ = _Any()

# TODO(b/202892821): Consolidate with SQL data types.
# See more at: http://hl7.org/fhir/datatypes.html.
_PRIMITIVE_TYPES_BY_CODE: Dict[str, FhirPathDataType] = {
    # python types
    'bool': Boolean,
    'str': String,
    'int': Integer,
    'float': Decimal,
    'nonetype': Empty,
    # fhir types
    'base64binary': String,
    'boolean': Boolean,
    'canonical': String,
    'code': String,
    'date': Date,
    'datetime': DateTime,
    'decimal': Decimal,
    'id': String,
    'instant': DateTime,
    'integer': Integer,
    'markdown': String,
    'oid': String,
    'positiveint': Integer,
    'string': String,
    'time': DateTime,
    'unsignedint': Integer,
    'uri': String,
    'uuid': String,
    'xhtml': String,
    'http://hl7.org/fhirpath/system.string': String,
}


def primitive_type_from_type_code(type_code: str) -> Optional[FhirPathDataType]:
  """Returns the FhirPathDataType for the primitive identifed by the URL."""
  return _PRIMITIVE_TYPES_BY_CODE.get(type_code.casefold())


def is_numeric(fhir_type: FhirPathDataType) -> bool:
  return (
      isinstance(fhir_type, _Integer) or isinstance(fhir_type, _Decimal)
  ) or isinstance(fhir_type, _Empty)


def is_primitive(fhir_type: FhirPathDataType) -> bool:
  return not isinstance(fhir_type, StructureDataType) and not isinstance(
      fhir_type, PolymorphicDataType
  )


def is_coercible(lhs: FhirPathDataType, rhs: FhirPathDataType) -> bool:
  """Returns `True` if implicit conversion can occur between `lhs` and `rhs`.

  See more at: https://hl7.org/fhirpath/#conversion.

  Args:
    lhs: The left operand.
    rhs: The right operand.

  Raises:
    ValueError: In the event that a coercion cycle is detected.

  Returns:
    `True` if coercion can occur, otherwise `False.`
  """

  if isinstance(rhs, _Any) or isinstance(lhs, _Any):
    return True  # All types can be coerced to _Any

  if not rhs or not lhs:
    return True  # All types can be coerced to None

  if isinstance(rhs, _Empty) or isinstance(lhs, _Empty):
    return True

  if rhs == lhs:
    return True  # Early-exit if same type

  # Legacy collection type kept around for _semant.
  if isinstance(rhs, Collection) or isinstance(lhs, Collection):
    return False  # Early-exit if either operand is a complex type

  if rhs in lhs.supported_coercion and lhs in rhs.supported_coercion:
    raise ValueError(f'Coercion cycle between: {lhs} and {rhs}.')

  return rhs in lhs.supported_coercion or lhs in rhs.supported_coercion


def coerce(lhs: FhirPathDataType, rhs: FhirPathDataType) -> FhirPathDataType:
  """Performs implicit type coercion between two datatypes.

  See more at: https://hl7.org/fhirpath/#conversion.

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
        f'Unsupported Standard SQL coercion between {lhs} and {rhs}.'
    )

  if isinstance(rhs, _Any) or isinstance(lhs, _Any):
    return _Any

  if rhs in lhs.supported_coercion:
    return rhs
  else:  # lhs in rhs.supported_coercion
    return lhs


def is_coding(fhir_type: FhirPathDataType) -> bool:
  """Indicates if the type is a Coding."""
  return fhir_type.url == 'http://hl7.org/fhir/StructureDefinition/Coding'


def is_codeable_concept(fhir_type: FhirPathDataType) -> bool:
  """Indicates if the type is a Codeable Concept."""
  return (
      fhir_type.url == 'http://hl7.org/fhir/StructureDefinition/CodeableConcept'
  )


def is_scalar(fhir_type: Optional[FhirPathDataType]) -> bool:
  # None return type is considered to be a scalar.
  return not fhir_type or fhir_type.cardinality == Cardinality.SCALAR


def returns_collection(return_type: FhirPathDataType) -> bool:
  return return_type and return_type.returns_collection()


def is_collection(return_type: FhirPathDataType) -> bool:
  return return_type and return_type.cardinality == Cardinality.COLLECTION


# Captures the names appearing after 'extension:' stanzas in IDs.
_EXTENSION_SLICE_NAMES_RE = re.compile(r'(?:^|\.)extension:(\w+)(?=$|\.)')
# Captures the word 'extension' in dotted paths.
_EXTENSION_PATH_ELEMENTS_RE = re.compile(r'(?:^|\.)(extension)(?=$|\.)')


def _get_analytic_path(path: str, elem_id: str) -> str:
  """Builds a usable FHIRPath given an element path and id.

  Removes choice type indicators '[x]'

  Some element definitions reference attributes from extension slices. These
  elements will have ids like 'Foo.extension:someExtension' but
  paths like 'Foo.extension' We want to treat these paths as
  'Foo.someExtension' so we replace the 'extension' part of the
  path with the slice name.

  Args:
    path: The path attribute from the element definition to clean.
    elem_id: The id attribute from the element definition to clean.

  Returns:
    The cleaned path name.
  """
  # Use a regex to find all extension slice names in the id.
  extension_slice_names = _EXTENSION_SLICE_NAMES_RE.findall(elem_id)
  if extension_slice_names:
    # Replace each .extension element of the path with the extension
    # slice name. Use a regex to find the indices of each .extension
    # part of the path.
    extension_path_elements = _EXTENSION_PATH_ELEMENTS_RE.finditer(path)
    index_adjustment = 0
    for slice_name, path_element in zip(
        extension_slice_names, extension_path_elements
    ):
      # Replace the extension elements with the slice name.
      start = path_element.start(1) + index_adjustment
      end = path_element.end(1) + index_adjustment
      path = path[:start] + slice_name + path[end:]

      # Because we're changing the length of the `path` by
      # replacing 'extension' with the slice name, we'll need to
      # take that into account when referencing future indices.
      index_adjustment += len(slice_name) - 9  # 9 is len('extension')

  # Remove choice type indicators from the path.
  path = path.replace('[x]', '')

  return path

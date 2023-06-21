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
import dataclasses
import enum
import itertools
import operator
import re
from typing import Any, Collection as CollectionType, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple, cast

import stringcase

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

# Given a type code like http://hl7.org/fhirpath/System.String
# captures 'String'
_TYPE_CODE_URI_RE = re.compile(r'^http://hl7.org/fhirpath/System\.(.+)')


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


@dataclasses.dataclass(frozen=True, eq=False)
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

  cardinality: Cardinality
  root_element_definition: Optional[message.Message]

  def __init__(
      self,
      *,
      cardinality: Cardinality = Cardinality.SCALAR,
      root_element_definition: Optional[message.Message] = None,
  ) -> None:
    object.__setattr__(self, 'cardinality', cardinality)
    object.__setattr__(self, 'root_element_definition', root_element_definition)

  @property
  @abc.abstractmethod
  def supported_coercion(self) -> Set['FhirPathDataType']:
    pass

  @property
  @abc.abstractmethod
  def url(self) -> str:
    pass

  @property
  @abc.abstractmethod
  def comparable(self) -> bool:
    pass

  def get_new_cardinality_type(
      self, cardinality: Cardinality
  ) -> 'FhirPathDataType':
    return dataclasses.replace(self, cardinality=cardinality)

  def copy_fhir_type_with_root_element_definition(
      self, root_element_definition: message.Message
  ) -> 'FhirPathDataType':
    """Copies the type and sets the root_element_definition.

    Args:
      root_element_definition: Element definition to set for the type.

    Returns:
      A copy of the original type with the root_element_definition set.
    """
    return dataclasses.replace(
        self, root_element_definition=root_element_definition
    )

  def returns_collection(self) -> bool:
    return (
        self.cardinality == Cardinality.COLLECTION
        or self.cardinality == Cardinality.CHILD_OF_COLLECTION
    )

  def returns_polymorphic(self) -> bool:
    """Indicates if the type returns a polymorphic choice type."""
    return False

  def fields(self) -> Set[str]:
    return set()

  @property
  def child_defs(self) -> Mapping[str, message.Message]:
    return {}

  def __eq__(self, o) -> bool:
    if isinstance(o, FhirPathDataType):
      return self.url == o.url and self.__class__ == o.__class__
    return False

  def __hash__(self) -> int:
    return hash(self.url)

  def __str__(self) -> str:
    name = self.__class__.__name__
    name = name.lstrip('_')
    name = f'<{name}>'
    return self._wrap_collection(name)

  def _wrap_collection(self, name: str) -> str:
    return f'[{name}]' if self.returns_collection() else name


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _Boolean(FhirPathDataType):
  """Represents the logical Boolean values `true` and `false`.

  These values are used as the result of comparisons, and can be combined using
  logical operators such as `and` and `or`.

  See more at: https://hl7.org/fhirpath/#boolean.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Boolean'

  @property
  def comparable(self) -> bool:
    return False


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _Date(FhirPathDataType):
  """Represents date and partial date values.

  Values are within the range @0001-01-01 to @9999-12-31 with a 1 day step size.

  See more at: https://hl7.org/fhirpath/#date.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([DateTime])

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Date'

  @property
  def comparable(self) -> bool:
    return True


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _Time(FhirPathDataType):
  """Represents time-of-day and partial time-of-day values.

  Values are in the range @T00:00:00.000 to @T23:59:59.999 with a step size of
  1 millisecond. The Time literal uses a subset of [ISO8601].

  See more at: https://hl7.org/fhirpath/#time.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Time'

  @property
  def comparable(self) -> bool:
    return True


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _DateTime(FhirPathDataType):
  """Represents date/time and partial date/time values.

  Values are within the range @0001-01-01T00:00:00.000 to
  @9999-12-31T23:59:59.999 with a 1 millisecond step size. The `DateTime`
  literal combines the `Date` and `Time` literals and is a subset of ISO8601.

  See more at: https://hl7.org/fhirpath/#datetime.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.DateTime'

  @property
  def comparable(self) -> bool:
    return True


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _Decimal(FhirPathDataType):
  """Represents real values.

  Values are within the range (-10^28 + 1)/10^8 to (10^28-1)/10^8 with a step
  size of 10^(-8).

  See more at: https://hl7.org/fhirpath/#decimal.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Quantity])

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Decimal'

  @property
  def comparable(self) -> bool:
    return True


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _Integer(FhirPathDataType):
  """Represents whole numbers in the range -2^31 to 2^31 - 1.

  Note that an integer's polarity is part of a FHIRPath expression, and not the
  type itself.

  See more at: https://hl7.org/fhirpath/#integer.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Decimal, Quantity])

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Integer'

  @property
  def comparable(self) -> bool:
    return True


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _Quantity(FhirPathDataType):
  """Represents quantities with a specified unit.

  The `value` component is defined as a decimal, and the `unit` element is
  represented as a `String` that is required to be either a valid Unified Code
  for Units of Measure unit or one of the calendar duration keywords, singular
  or plural.

  See more at: https://hl7.org/fhirpath/#quantity.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Quantity'

  @property
  def comparable(self) -> bool:
    return True


@dataclasses.dataclass(frozen=True, eq=False, init=False)
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

  @property
  def comparable(self) -> bool:
    return False


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _String(FhirPathDataType):
  r"""Represents string values up to 2^31 - 1 characters in length.

  String literals are surrounded by single-quotes and may use '\'-escapes to
  escape quotes and represent Unicode characters.

  See more at: https://hl7.org/fhirpath/#string.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.String'

  @property
  def comparable(self) -> bool:
    return True


@dataclasses.dataclass(frozen=True, eq=False, init=False)
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
  def url(self) -> str:
    return ''

  @property
  def comparable(self) -> bool:
    return False

  def __eq__(self, o: Any) -> bool:
    return isinstance(o, _Empty)

  def __hash__(self) -> int:
    return hash(self.__class__)


@dataclasses.dataclass(frozen=True, eq=False)
class Collection(FhirPathDataType):
  """A heterogeneous ordered group of FHIRPath primitive datatypes.

  In FHIRPath, the result of every expression is a collection, even if that
  expression only results in a single element.

  This Collection type holds a set containing all the types currently in it.

  Note: It is not a concrete implementation of `Collection`, so it only stores
  one instance of every type that is present.
  """
  types: Sequence[FhirPathDataType]

  def __init__(
      self,
      *,
      types: CollectionType[FhirPathDataType],
      cardinality: Cardinality = Cardinality.COLLECTION,
      root_element_definition: Optional[message.Message] = None,
  ) -> None:
    super().__init__(
        cardinality=cardinality, root_element_definition=root_element_definition
    )
    object.__setattr__(
        self, 'types', tuple(sorted(types, key=operator.attrgetter('url')))
    )

  @property
  def url(self) -> str:
    return ''

  def __str__(self) -> str:
    type_str = ', '.join([str(t) for t in self.types])
    name = f'<{self.__class__.__name__}(types={type_str})>'
    return self._wrap_collection(name)

  @property
  def comparable(self) -> bool:
    return False

  def __eq__(self, o: Any) -> bool:
    if not isinstance(o, Collection):
      return False

    return self.types == o.types

  def __hash__(self) -> int:
    return hash(self.types)

  def __len__(self) -> int:
    return len(self.types)

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()

  def fields(self) -> Set[str]:
    return set(itertools.chain.from_iterable(t.fields() for t in self.types))


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class StructureDataType(FhirPathDataType):
  """The FHIR specification data types used in the resource elements.

  Their definitions are typically provided by FHIR StructureDefinitions.
  See https://www.hl7.org/fhir/datatypes.html
  """
  structure_definition: message.Message
  base_type: str
  element_type: str
  backbone_element_path: Optional[str]
  _child_defs: CollectionType[Tuple[str, message.Message]]
  _direct_children: CollectionType[Tuple[str, message.Message]]
  _other_descendants: CollectionType[Tuple[str, message.Message]]
  _slices: Tuple[Slice, ...]
  _raw_url: str

  def __init__(
      self,
      *,
      structure_definition: message.Message,
      base_type: str,
      element_type: str,
      backbone_element_path: Optional[str],
      _child_defs: CollectionType[Tuple[str, message.Message]],
      _direct_children: CollectionType[Tuple[str, message.Message]],
      _other_descendants: CollectionType[Tuple[str, message.Message]],
      _slices: Tuple[Slice, ...],
      _raw_url: str,
      cardinality: Cardinality = Cardinality.SCALAR,
      root_element_definition: Optional[message.Message] = None,
  ) -> None:
    super().__init__(
        cardinality=cardinality, root_element_definition=root_element_definition
    )
    object.__setattr__(self, 'structure_definition', structure_definition)
    object.__setattr__(self, 'base_type', base_type)
    object.__setattr__(self, 'element_type', element_type)
    object.__setattr__(self, 'backbone_element_path', backbone_element_path)
    object.__setattr__(self, '_child_defs', _child_defs)
    object.__setattr__(self, '_direct_children', _direct_children)
    object.__setattr__(self, '_other_descendants', _other_descendants)
    object.__setattr__(self, '_slices', _slices)
    object.__setattr__(self, '_raw_url', _raw_url)

  @property
  def supported_coercion(self) -> Set['FhirPathDataType']:
    return set()

  @property
  def child_defs(self) -> Mapping[str, message.Message]:
    return {k: v for k, v in self._child_defs}

  @classmethod
  def from_proto(
      cls,
      struct_def_proto: message.Message,
      backbone_element_path: Optional[str] = None,
      element_type: Optional[str] = None,
  ) -> 'StructureDataType':
    """Creates a StructureDataType from a proto.

    Args:
      struct_def_proto: Proto containing information about the structure
        definition.
      backbone_element_path: Optional path to the structure def.
      element_type: Potential alternative type name for the type.

    Returns:
      A StructureDataType.
    """
    struct_def = cast(Any, struct_def_proto)
    raw_url = struct_def.url.value
    base_type = struct_def.type.value
    # For some custom types, the element type differs from the base type.
    element_type = element_type if element_type else base_type

    # For backbone elements, prepend their paths with the path to the
    # root of the backbone element.
    qualified_path = (
        f'{element_type}.{backbone_element_path}'
        if backbone_element_path
        else element_type
    )

    child_defs = {}
    direct_children = []
    other_descendants = []
    # A map of slice ID (e.g. some.path:SomeSlice) to the _SliceBuilder
    # object representing that slice.
    slices: dict[str, _SliceBuilder] = collections.defaultdict(
        lambda: _SliceBuilder(None, None, [])
    )
    root_element_definition = None

    for elem in struct_def.snapshot.element:
      # Extension.url does not provide any additional meaningful information for
      # extensions so we will skip it as it also conflicts with
      # Extension.extension:url if there is a subextension type. More info in
      # b/284998302
      if elem.base.path.value == 'Extension.url':
        continue

      path = _get_analytic_path(elem.path.value, elem.id.value)

      if path == qualified_path:
        if elem.slice_name.value:
          # This is a slice definition at `qualified_path`, i.e. a
          # path like <qualified_path>:Slice'
          slice_def = slices[f':{elem.slice_name.value}']
          slice_def.slice_def = elem
          slice_def.relative_path = ''
        else:
          root_element_definition = elem

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
            rf'^{qualified_path}[\.]?(.*(?<!.extension):[\w-]+)(?:$|\.)',
            elem.id.value,
        )
        direct_child = '.' not in relative_path

        if direct_child and closest_slice_ancestor is None:
          assert relative_path not in child_defs, (
              f'{relative_path} found twice among children in structure'
              f' definition {struct_def.url.value}.'
          )
          child_defs[relative_path] = elem

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
          direct_children.append((relative_path, elem))
        else:
          other_descendants.append((relative_path, elem))

    if not root_element_definition:
      raise ValueError(
          f'StructureDataType {raw_url} searching on {qualified_path} '
          f' missing root element definition. {struct_def}'
      )

    # pylint can't infer the arguments from a base class b/253217163
    # pylint: disable=unexpected-keyword-arg
    return cls(
        structure_definition=struct_def,
        backbone_element_path=backbone_element_path,
        base_type=base_type,
        element_type=element_type,
        _child_defs=tuple(child_defs.items()),
        _direct_children=tuple(direct_children),
        _other_descendants=tuple(other_descendants),
        _slices=tuple(slice_def.to_slice() for slice_def in slices.values()),
        _raw_url=raw_url,
        root_element_definition=root_element_definition,
        cardinality=Cardinality.SCALAR,
    )
    # pylint: enable=unexpected-keyword-arg

  @property
  def url(self) -> str:
    return (
        '.'.join([self._raw_url, self.backbone_element_path])
        if self.backbone_element_path
        else self._raw_url
    )

  def __str__(self) -> str:
    name = f'<{self.__class__.__name__}(url={self.url})>'
    return self._wrap_collection(name)

  @property
  def comparable(self) -> bool:
    return False

  def fields(self) -> Set[str]:
    return set(self.child_defs.keys())

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


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class QuantityStructureDataType(StructureDataType, _Quantity):
  """Represents quantity FHIR specification data types."""

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Quantity])

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Quantity'

  @property
  def comparable(self) -> bool:
    return True

  @classmethod
  def from_proto(
      cls,
      struct_def_proto: message.Message,
      backbone_element_path: Optional[str] = None,
  ) -> 'QuantityStructureDataType':
    """Creates a QuantityStructureDataType from a proto.

    Args:
      struct_def_proto: Proto containing information about the structure
        definition.
      backbone_element_path: Optional path to the structure def.

    Returns:
      A QuantityStructureDataType.
    """

    struct_type = StructureDataType.from_proto(
        struct_def_proto=struct_def_proto,
        backbone_element_path=backbone_element_path,
    )
    return cls(
        structure_definition=struct_type.structure_definition,
        backbone_element_path=struct_type.backbone_element_path,
        base_type=struct_type.base_type,
        element_type=struct_type.element_type,
        _child_defs=struct_type._child_defs,  # pylint: disable=protected-access
        _direct_children=struct_type._direct_children,  # pylint: disable=protected-access
        _other_descendants=struct_type._other_descendants,  # pylint: disable=protected-access
        _slices=struct_type._slices,  # pylint: disable=protected-access
        _raw_url=struct_type._raw_url,  # pylint: disable=protected-access
        root_element_definition=struct_type.root_element_definition,
        cardinality=struct_type.cardinality,
    )
    # pylint: enable=unexpected-keyword-arg


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class ReferenceStructureDataType(StructureDataType):
  """Represents a FHIR Reference.

  See more at: https://build.fhir.org/references.html

  Attributes:
    target_profiles: The types of resources to which the reference may link. See
      more at:
      https://build.fhir.org/elementdefinition-definitions.html#ElementDefinition.type.targetProfile
  """

  target_profiles: CollectionType[str]

  def __init__(
      self,
      *,
      target_profiles: CollectionType[str],
      structure_definition: message.Message,
      base_type: str,
      element_type: str,
      backbone_element_path: Optional[str],
      _child_defs: CollectionType[Tuple[str, message.Message]],
      _direct_children: CollectionType[Tuple[str, message.Message]],
      _other_descendants: CollectionType[Tuple[str, message.Message]],
      _slices: Tuple[Slice, ...],
      _raw_url: str,
      cardinality: Cardinality = Cardinality.SCALAR,
      root_element_definition: Optional[message.Message] = None,
  ) -> None:
    super().__init__(
        structure_definition=structure_definition,
        backbone_element_path=backbone_element_path,
        base_type=base_type,
        element_type=element_type,
        _child_defs=_child_defs,
        _direct_children=_direct_children,
        _other_descendants=_other_descendants,
        _slices=_slices,
        _raw_url=_raw_url,
        root_element_definition=root_element_definition,
        cardinality=cardinality,
    )
    object.__setattr__(self, 'target_profiles', target_profiles)

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set([Reference])

  @property
  def url(self) -> str:
    return 'http://hl7.org/fhirpath/System.Reference'

  @property
  def comparable(self) -> bool:
    return False

  @classmethod
  def from_proto(
      cls,
      struct_def_proto: message.Message,
      backbone_element_path: Optional[str] = None,
      element_type: Optional[str] = None,
      element_definition: Optional[message.Message] = None,
  ) -> 'ReferenceStructureDataType':
    target_profiles = [
        profile.value
        for profile in cast(Any, element_definition).type[0].target_profile
    ]
    struct_type = StructureDataType.from_proto(
        struct_def_proto=struct_def_proto,
        backbone_element_path=backbone_element_path,
        element_type=None,
    )

    # pylint: disable=unexpected-keyword-arg
    return cls(
        target_profiles=tuple(target_profiles),
        structure_definition=struct_type.structure_definition,
        backbone_element_path=struct_type.backbone_element_path,
        base_type=struct_type.base_type,
        element_type=struct_type.element_type,
        _child_defs=struct_type._child_defs,  # pylint: disable=protected-access
        _direct_children=struct_type._direct_children,  # pylint: disable=protected-access
        _other_descendants=struct_type._other_descendants,  # pylint: disable=protected-access
        _slices=struct_type._slices,  # pylint: disable=protected-access
        _raw_url=struct_type._raw_url,  # pylint: disable=protected-access
        root_element_definition=struct_type.root_element_definition,
        cardinality=struct_type.cardinality,
    )
    # pylint: enable=unexpected-keyword-arg


@dataclasses.dataclass(frozen=True, eq=False, init=False)
class _Any(FhirPathDataType):
  """Represents any type in FHIRPath.

  Used in situations where we cannot determine the type.
  """

  @property
  def supported_coercion(self) -> Set[FhirPathDataType]:
    return set()  # No supported coercion

  @property
  def url(self) -> str:
    return ''

  @property
  def comparable(self) -> bool:
    # We don't restrict what the Any type can be compared to.
    return True

  def __eq__(self, o: Any) -> bool:
    return isinstance(o, _Any)

  def __hash__(self) -> int:
    return hash(self.__class__)


@dataclasses.dataclass(frozen=True, eq=False)
class PolymorphicDataType(FhirPathDataType):
  """A heterogeneous ordered group of FhirPathDataTypes.

  In FHIRPath, some fields are polymorphic and can be multiple types.
  This type stores all the possible types a field can be.

  See more at: https://hl7.org/fhirpath/#paths-and-polymorphic-items.
  """

  types: Mapping[str, FhirPathDataType]

  def __init__(
      self,
      *,
      types: Mapping[str, FhirPathDataType],
      cardinality: Cardinality = Cardinality.SCALAR,
      root_element_definition: Optional[message.Message] = None,
  ) -> None:
    super().__init__(
        cardinality=cardinality, root_element_definition=root_element_definition
    )
    sorted_types = collections.OrderedDict()
    for k in sorted(types):
      sorted_types[k] = types[k]

    object.__setattr__(self, 'types', sorted_types)

  def returns_polymorphic(self) -> bool:
    return True

  @property
  def supported_coercion(self) -> Set['FhirPathDataType']:
    return set()

  @property
  def url(self) -> str:
    return next(t.url for t in self.types.values()) if self.urls else ''

  @property
  def urls(self) -> Set[str]:
    return set(t.url for t in self.types.values())

  def __str__(self) -> str:
    type_name_strings = [f'{name}: {t.url}' for name, t in self.types.items()]
    return f'{self.__class__.__name__}(types={type_name_strings})'

  @property
  def comparable(self) -> bool:
    return False

  def fields(self) -> Set[str]:
    return set(self.types.keys())

  def __eq__(self, o) -> bool:
    if isinstance(o, PolymorphicDataType):
      return o.types == self.types
    return False

  def __hash__(self) -> int:
    return hash(tuple(self.types))


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


def is_type_code_primitive(type_code: str) -> bool:
  """Indicates if `type_code` refers to a primitive type.

  Args:
    type_code: The FHIR type code to look up. Could be a value like 'Boolean' or
      URL like 'http://hl7.org/fhirpath/System.Boolean'

  Returns:
    True if the type code represents a FHIR primitive and False otherwise.
  """
  url_type_code = _TYPE_CODE_URI_RE.search(type_code)
  if url_type_code is not None:
    type_code = url_type_code[1]

  return type_code.casefold() in _PRIMITIVE_TYPES_BY_CODE


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
_EXTENSION_SLICE_NAMES_RE = re.compile(r'(?:^|\.)extension:([\w-]+)(?=$|\.)')
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


def fixed_field_for_type_code(type_code: str) -> str:
  """Retrieves the `ElementDefinition.fixed.choice` oneof field for `type_code`.

  Args:
    type_code: The FHIR type code to look up. Could be a value like 'boolean' or
      URL like 'http://hl7.org/fhirpath/System.Boolean'

  Returns:
    The attribute corresponding to this type code on the
    ElementDefinition.FixedX.choice oneof.
  """
  url_type_code = _TYPE_CODE_URI_RE.search(type_code)
  if url_type_code is not None:
    type_code = url_type_code[1]

  # Lower-case the first character to avoid leading _ characters from
  # snake case-ing.
  fixed_field = stringcase.snakecase(type_code[:1].lower() + type_code[1:])
  # We special-case 'string' as 'string_value' in the proto.
  return 'string_value' if fixed_field == 'string' else fixed_field

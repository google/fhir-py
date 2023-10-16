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
"""Library for wrapping the FHIR Path expressions Builder."""

from typing import Any, Iterable, List, Optional, Union

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import expressions
from google.fhir.core.internal import primitive_handler

BuilderOperand = Union[
    expressions.Comparable,
    expressions.Builder,
    'ColumnExpressionBuilder',
]


class ColumnExpressionBuilder:
  """Wraps a FHIRPath expressions.Builder to support FHIRViews features.

  The requirements of this wrapper include:
  1. allow users to easily build the View column expressions on top of the
     FHIRPath expressions, such as:

     >>> Patient.name.first().given.alias('given_name')

     , where the `alias` function does not belong to the FHIRPath builder.

  2. NOT allow users to keep building the FHIRPath expressions once they have
     already called the FHIRViews functions. For example,
     if the users write:

     >>> Patient.name.first().alias('patient_name').given

     , it should throw an error.
  """

  def __init__(
      self,
      fhir_path_builder: expressions.Builder,
      column_name: Optional[str],
      children: List['ColumnExpressionBuilder'],
      needs_unnest: bool,
      sealed: bool,
  ):
    self._builder: expressions.Builder = fhir_path_builder
    self._column_name: Optional[str] = column_name
    self._children: List['ColumnExpressionBuilder'] = children
    self._needs_unnest: bool = needs_unnest
    self._sealed: bool = sealed

  def alias(self, name: str) -> 'ColumnExpressionBuilder':
    """The alias() function.

    Sets the column name of a given FHIR path in the View. Once the colomn
    name is set, the FHIR path is sealed to be immutable.

    Args:
      name: The column name as a string.

    Returns:
      A new ColumnExpressionBuilder with the given alias name.
    """
    if self._children:
      raise AttributeError(
          'alias() must not be called on a builder with child selects. '
          f'Got alias called on {str(self)}.'
      )

    return ColumnExpressionBuilder(
        self._builder, name, self._children, self._needs_unnest, True
    )

  def forEach(self) -> 'ColumnExpressionBuilder':  # pylint: disable=invalid-name
    """The forEach() function.

    Unnests the repeated values from a FHIR path. If the FHIR path does not
    return a collection, we treat that as a collection with a single value.
    Once this function is called, the FHIR path is sealed to be immutable.

    Returns:
      A new ColumnExpressionBuilder with needs_unnest set to True.
    """
    return ColumnExpressionBuilder(
        self._builder, self._column_name, self._children, True, True
    )

  def select(
      self, children: List['ColumnExpressionBuilder']
  ) -> 'ColumnExpressionBuilder':
    """The select() function.

    Selects the child fields from a FHIR path which returns a StructureDataType.
    Once this function is called, the FHIR path is sealed to be immutable.

    Args:
      children: A list of selected FHIR Path.

    Returns:
      A new ColumnExpressionBuilder with the given children.
    """
    if self._column_name:
      raise AttributeError(
          'select() must not be called on a builder with alias set already. '
          f'Got select called on {str(self)}.'
      )

    if (
        self._builder.return_type.returns_collection()
        and not self._needs_unnest
    ):
      raise AttributeError(
          'select() must not be called on a builder which returns collection. '
          f'Got select called on {str(self)}.'
      )

    if not isinstance(
        self._builder.return_type, _fhir_path_data_types.StructureDataType
    ):
      raise AttributeError(
          'select() can only be called on a FHIR path which returns '
          'a StructureDataType. '
          f'Got type of {self._builder.return_type} from {str(self)}.'
      )

    for child_builder in children:
      if not child_builder.column_name and not child_builder.children:
        raise AttributeError(
            'select() child builders must either have alias names or children. '
            f'Got {str(child_builder)}'
        )

      if not child_builder.fhir_path.startswith(self._builder.fhir_path):
        raise AttributeError(
            'select() child builders must be built by starting with their '
            'parent FHIR path. '
            f'Got {str(child_builder)} in {self._builder.fhir_path}.'
        )
      path_to_replace = self._builder.fhir_path
      reference_node = self._builder.node

      child_builder.node.replace_operand(
          path_to_replace,
          _evaluation.ReferenceNode(
              self._builder.node.context,
              reference_node,
              unnested=True,
          ),
      )

    return ColumnExpressionBuilder(
        self._builder, self._column_name, children, self._needs_unnest, True
    )

  @classmethod
  def from_fhir_path_builder(
      cls,
      fhir_path_builder: expressions.Builder,
      column_name: Optional[str] = None,
      children: Optional[List['ColumnExpressionBuilder']] = None,
      needs_unnest: bool = False,
      sealed: bool = False,
  ):
    return cls(
        fhir_path_builder, column_name, children or [], needs_unnest, sealed
    )

  @classmethod
  def from_node_and_handler(
      cls,
      node: _evaluation.ExpressionNode,
      handler: primitive_handler.PrimitiveHandler,
      column_name: Optional[str] = None,
      children: Optional[List['ColumnExpressionBuilder']] = None,
      needs_unnest: bool = False,
      sealed: bool = False,
  ):
    return cls(
        expressions.Builder(node, handler),
        column_name,
        children or [],
        needs_unnest,
        sealed,
    )

  @property
  def builder(self) -> expressions.Builder:
    return self._builder

  @property
  def column_name(self) -> Optional[str]:
    return self._column_name

  @property
  def children(self) -> List['ColumnExpressionBuilder']:
    return self._children

  @property
  def needs_unnest(self) -> bool:
    return self._needs_unnest

  @property
  def sealed(self) -> bool:
    return self._sealed

  def __getattr__(self, name: str):
    """Redirects to the expressions.Builder when the attribute is not here.

    Note that in Python, '__getattribute__' always gets called first (the
    highest priority). Thus for attributes which has already been defined in
    this class, they won't be redirected to the expressions.Builder.

    Args:
      name: The attribute name as a string.

    Returns:
      The attribute get from expressions.Builder wrapped with _wrap_any.

    Raises:
      AttributeError: if the FHIR path in this class is already sealed, or if
      getting the attribute from self._builder fails.
    """
    # Prevents infinite recursion when Builder is deep copied.
    if name.startswith('__'):
      raise AttributeError(name)

    attr = getattr(self._builder, name)
    if isinstance(attr, expressions.Builder) and self._sealed:
      raise self._fhir_path_sealed_error(name)
    return ColumnExpressionBuilder._wrap_any(self, attr)

  def __getitem__(self, key: Any) -> 'ColumnExpressionBuilder':
    """Redirects to the expressions.Builder to get the item.

    Args:
      key: the key of the item.

    Returns:
      A ColumnExpressionBuilder, because the item got from the
      expressions.Builder is always the type of Builder.

    Raises:
      AttributeError: if the FHIR path in this class is already sealed.
      TypeError: if getting the key from self._builder fails.
    """
    item = self._builder[key]
    if isinstance(item, expressions.Builder) and self._sealed:
      raise self._fhir_path_sealed_error(key)
    return ColumnExpressionBuilder._wrap_any(self, item)

  def _to_string(
      self, builder: 'ColumnExpressionBuilder', indent: int = 0
  ) -> str:
    """Function to recursively print the operands of the input operand."""
    indent_string = f'{"  " * indent}'
    foreach_string = '.forEach()' if builder.needs_unnest else ''
    alias_string = (
        f'.alias({builder.column_name})' if builder.column_name else ''
    )
    base_string = (
        f'{indent_string}{builder.fhir_path}{foreach_string}{alias_string}'
    )
    child_strings = []
    if builder.children:
      base_string = f'{base_string}.select([\n'
      for child in builder.children:
        child_strings.append(self._to_string(child, indent + 1))
      selects_string = ',\n'.join(child_strings)
      base_string = f'{base_string}{selects_string}\n{indent_string}])'
    return base_string

  def __str__(self) -> str:
    return self._to_string(self)

  def __repr__(self) -> str:
    return f'ColumnExpressionBuilder("{str(self)}")'

  def __dir__(self) -> Iterable[str]:  # pytype: disable=signature-mismatch  # overriding-return-type-checks
    fields = [name for name in dir(self._builder) if not name.startswith('__')]
    fields.extend(dir(type(self)))
    return fields

  def __eq__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__eq__', rhs)

  def __ne__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__ne__', rhs)

  def __or__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__or__', rhs)

  def __and__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__and__', rhs)

  def __xor__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__xor__', rhs)

  def __lt__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__lt__', rhs)

  def __gt__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__gt__', rhs)

  def __le__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__le__', rhs)

  def __ge__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__ge__', rhs)

  def __add__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__add__', rhs)

  def __mul__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__mul__', rhs)

  def __sub__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__sub__', rhs)

  def __truediv__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__truediv__', rhs)

  def __floordiv__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__floordiv__', rhs)

  def __mod__(self, rhs: BuilderOperand) -> 'ColumnExpressionBuilder':
    return self._redirect_operation('__mod__', rhs)

  def _redirect_operation(
      self, operation_name: str, rhs: BuilderOperand
  ) -> 'ColumnExpressionBuilder':
    """Redirects an operation to the expressions.Builder.

    Args:
      operation_name: The operation name as a string.
      rhs: The right hand side operand of the operation.

    Returns:
      A ColumnExpressionBuilder, because the operation result from the
      expressions.Builder is always the type of Builder.

    Raises:
      AttributeError: if the FHIR path in this class is already sealed.
    """
    if self._sealed:
      raise self._fhir_path_sealed_error(operation_name)

    operand = rhs.builder if isinstance(rhs, type(self)) else rhs
    result = getattr(self._builder, operation_name)(operand)
    return ColumnExpressionBuilder._wrap_any(self, result)

  def _fhir_path_sealed_error(self, execution_name: str):
    return AttributeError(
        'Cannot keep building the fhir path after calling FHIRViews features. '
        f'Got {str(self)} when getting / calling {execution_name}'
    )

  @classmethod
  def _wrap_function(cls, self, func):
    """Wraps any function's return result with _wrap_any."""

    def wrapper(*args, **kwargs):
      new_args = [arg.builder if isinstance(arg, cls) else arg for arg in args]
      new_kwargs = {
          (key.builder if isinstance(key, cls) else key): (
              value.builder if isinstance(value, cls) else value
          )
          for key, value in kwargs.items()
      }
      result = func(*new_args, **new_kwargs)
      if isinstance(result, expressions.Builder) and self._sealed:  # pylint: disable=protected-access
        raise self._fhir_path_sealed_error(func.__name__)  # pylint: disable=protected-access
      return cls._wrap_any(self, result)

    return wrapper

  @classmethod
  def _wrap_any(cls, self, obj: Any):
    """Wraps any object with the logic below.

    Args:
      self: self instance reference.
      obj: any object.

    Returns:
    If the object is:
      - an expressions.Builder: returns this class to wrap it;
      - a list: returns a new list with each item in the list wrapped;
      - a tuple: returns a new tuple with each item in the list wrapped;
      - a dictionary: returns a new dictionary with each key/value pair in
        the dictionary wrapped.
      - a callable function: returns a wrapper function with the return result
        of the function wrapped.
      - anything else: returns the object itself.
    """
    if isinstance(obj, expressions.Builder):
      return cls.from_fhir_path_builder(obj)
    if isinstance(obj, list):
      return [cls._wrap_any(self, item) for item in obj]
    if isinstance(obj, tuple):
      return (cls._wrap_any(self, item) for item in obj)
    if isinstance(obj, dict):
      return {
          cls._wrap_any(self, key): cls._wrap_any(self, value)
          for key, value in obj.items()
      }
    if callable(obj):
      return cls._wrap_function(self, obj)
    return obj

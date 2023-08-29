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

from typing import Any, Iterable, Optional, Union

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import expressions
from google.fhir.r4 import primitive_handler

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

  def __init__(self, *args):
    if len(args) == 1 and isinstance(args[0], type(self)):
      fhir_path_builder = args[0].builder
      column_name = args[0].column_name
    elif len(args) == 1 and isinstance(args[0], expressions.Builder):
      fhir_path_builder = args[0]
      column_name = None
    elif (
        len(args) == 2
        and isinstance(args[0], _evaluation.ExpressionNode)
        and isinstance(args[1], primitive_handler.PrimitiveHandler)
    ):
      fhir_path_builder = expressions.Builder(args[0], args[1])
      column_name = None
    else:
      raise AttributeError(
          f'Cannot create ColumnExpressionBuilder from args: {args}. Expected'
          ' one argument of type fhir_path.expressions.Builder, or two'
          ' parameters of type fhir_path._evaluation.ExpressionNode and'
          ' r4.primitive_handler.PrimitiveHandler.'
      )

    self._builder: expressions.Builder = fhir_path_builder
    self._column_name: Optional[str] = column_name
    self._sealed: bool = False
    self._str: str = fhir_path_builder.fhir_path

  def alias(self, name: str):
    """The alias() function.

    Sets the column name of a given FHIR path in the View. Once the colomn
    name is set, the FHIR path is sealed to be immutable.

    Args:
      name: The column name as a string.

    Returns:
      The class itself.
    """
    self._column_name = name
    self._sealed = True
    self._str += f'.alias({name})'
    return self

  @property
  def column_name(self) -> Optional[str]:
    return self._column_name

  @property
  def builder(self) -> expressions.Builder:
    return self._builder

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
      raise self._fhir_path_sealed_error()
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
      raise self._fhir_path_sealed_error()
    return ColumnExpressionBuilder._wrap_any(self, item)

  def __str__(self) -> str:
    return self._str

  def __repr__(self) -> str:
    return f'ColumnExpressionBuilder("{self._str}")'

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
      raise self._fhir_path_sealed_error()

    operand = rhs.builder if isinstance(rhs, type(self)) else rhs
    result = getattr(self._builder, operation_name)(operand)
    return ColumnExpressionBuilder._wrap_any(self, result)

  def _fhir_path_sealed_error(self):
    return AttributeError(
        f'Cannot keep building the fhir path {self._builder.fhir_path} after'
        ' calling FHIRViews features.'
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
        raise self._fhir_path_sealed_error()  # pylint: disable=protected-access
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
      return ColumnExpressionBuilder(obj)
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

# Copyright 2023 Google LLC
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

"""Support for the execution nodes."""
import enum


class AccessModifier(str, enum.Enum):
  """Is used to specify the access level for the various definitions within a library such as parameters, expressions, and functions."""

  PUBLIC = 'Public'
  PRIVATE = 'Private'


class DateTimePrecision(str, enum.Enum):
  """Specifies the units of precision available for temporal operations such as durationbetween, sameas, sameorbefore, sameorafter, and datetimecomponentfrom."""

  YEAR = 'Year'
  MONTH = 'Month'
  WEEK = 'Week'
  DAY = 'Day'
  HOUR = 'Hour'
  MINUTE = 'Minute'
  SECOND = 'Second'
  MILLISECOND = 'Millisecond'


class ErrorSeverity(str, enum.Enum):
  INFO = 'info'
  WARNING = 'warning'
  ERROR = 'error'


class ErrorType(str, enum.Enum):
  """Represents the type of CQL to ELM conversion error."""

  ENVIRONMENT = 'environment'
  SYNTAX = 'syntax'
  INCLUDE = 'include'
  SEMANTIC = 'semantic'
  INTERNAL = 'internal'


class SortDirection(str, enum.Enum):
  ASC = 'asc'
  ASCENDING = 'ascending'
  DESC = 'desc'
  DESCENDING = 'descending'


class ElementNode:
  """Defines the abstract base type for all library elements in elm."""

  def __init__(self, result_type_name=None, result_type_specifier=None):
    self.result_type_name = result_type_name
    self.result_type_specifier = result_type_specifier


class TypeSpecifierNode(ElementNode):
  """TypeSpecifierNode is the abstract base type for all type specifiers."""


class NamedTypeSpecifierNode(TypeSpecifierNode):
  """NamedTypeSpecifierNode defines a type identified by a name, such as Integer, String, Patient, or Encounter."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, name=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name


class IntervalTypeSpecifierNode(TypeSpecifierNode):
  """IntervalTypeSpecifierNode defines an interval type by specifying the point type."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, point_type=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.point_type = point_type


class ListTypeSpecifierNode(TypeSpecifierNode):
  """ListTypeSpecifierNode defines a list type by specifying the type of elements the list may contain."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, element_type=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.element_type = element_type


class TupleElementDefinitionNode(ElementNode):
  """TupleElementDefinitionNode defines the name and type of a single element within a TupleTypeSpecifier."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      type_=None,
      element_type=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.type_ = type_
    self.element_type = element_type


class TupleTypeSpecifierNode(TypeSpecifierNode):
  """TupleTypeSpecifierNode defines the possible elements of a tuple."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, element=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    if element is None:
      self.element = []
    else:
      self.element = element


class ChoiceTypeSpecifierNode(TypeSpecifierNode):
  """ChoiceTypeSpecifierNode defines the possible types of a choice type."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      type_=None,
      choice=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    if type_ is None:
      self.type_ = []
    else:
      self.type_ = type_
    if choice is None:
      self.choice = []
    else:
      self.choice = choice


class ParameterTypeSpecifierNode(TypeSpecifierNode):
  """A type which is generic class parameter such as T in MyGeneric."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      parameter_name=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.parameter_name = parameter_name


class ExpressionNode(ElementNode):
  """Defines the abstract base type for all expressions used in the elm expression language."""


class OperatorExpressionNode(ExpressionNode):
  """Defines the abstract base type for all built-in operators used in the elm expression language."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, signature=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    if signature is None:
      self.signature = []
    else:
      self.signature = signature


class UnaryExpressionNode(OperatorExpressionNode):
  """Defines the abstract base type for expressions that take a single argument."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.operand = operand


class BinaryExpressionNode(OperatorExpressionNode):
  """Defines the abstract base type for expressions that take two arguments."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    if operand is None:
      self.operand = []
    else:
      self.operand = operand


class TernaryExpressionNode(OperatorExpressionNode):
  """Defines the abstract base type for expressions that take three arguments."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    if operand is None:
      self.operand = []
    else:
      self.operand = operand


class NaryExpressionNode(OperatorExpressionNode):
  """Defines an abstract base class for an expression that takes any number of arguments, including zero."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    if operand is None:
      self.operand = []
    else:
      self.operand = operand


class ExpressionDefNode(ElementNode):
  """Defines an expression and an associated name that can be referenced by any expression in the artifact."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      context=None,
      access_level='Public',
      expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.context = context
    self.access_level = access_level
    self.expression = expression


class FunctionDefNode(ExpressionDefNode):
  """Defines a named function that can be invoked by any expression in the artifact."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      context=None,
      access_level='Public',
      expression=None,
      external=None,
      fluent=None,
      operand=None,
  ):
    super().__init__(
        result_type_name,
        result_type_specifier,
        name,
        context,
        access_level,
        expression,
    )
    self.external = external
    self.fluent = fluent
    if operand is None:
      self.operand = []
    else:
      self.operand = operand


class ExpressionRefNode(ExpressionNode):
  """Defines an expression that references a previously defined namedexpression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.library_name = library_name


class FunctionRefNode(ExpressionRefNode):
  """Defines an expression that invokes a previously defined function."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
      signature=None,
      operand=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, name, library_name
    )
    if signature is None:
      self.signature = []
    else:
      self.signature = signature
    if operand is None:
      self.operand = []
    else:
      self.operand = operand


class ParameterDefNode(ElementNode):
  """Defines a parameter that can be referenced by name anywhere within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      parameter_type=None,
      access_level='Public',
      default=None,
      parameter_type_specifier=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.parameter_type = parameter_type
    self.access_level = access_level
    self.default = default
    self.parameter_type_specifier = parameter_type_specifier


class ParameterRefNode(ExpressionNode):
  """The ParameterRefNode expression allows the value of a parameter to be referenced as part of an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.library_name = library_name


class OperandDefNode(ElementNode):
  """Defines an operand to a function that can be referenced by name anywhere within the body of a function definition."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      operand_type=None,
      operand_type_specifier=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.operand_type = operand_type
    self.operand_type_specifier = operand_type_specifier


class OperandRefNode(ExpressionNode):
  """The OperandRefNode expression allows the value of an operand to be referenced as part of an expression within the body of a function definition."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, name=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name


class IdentifierRefNode(ExpressionNode):
  """Defines an expression that references an identifier that is either unresolved, or has been resolved to an attribute in an unambiguous iteration scope such as a sort."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.library_name = library_name


class LiteralNode(ExpressionNode):
  """Defines a single scalar value."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      value_type=None,
      value=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.value_type = value_type
    self.value = value


class TupleElementNode:
  """The TupleElementNode is used within a Tuple expression to provide the value of a specific element within a tuple literal expression."""

  def __init__(self, name=None, value=None):
    self.name = name
    self.value = value


class TupleNode(ExpressionNode):
  """To be built up as an expression."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, element=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    if element is None:
      self.element = []
    else:
      self.element = element


class InstanceElementNode:
  """The InstanceElementNode is used within an Instance expression to provide the value of a specific element within an object literal expression."""

  def __init__(self, name=None, value=None):
    self.name = name
    self.value = value


class InstanceNode(ExpressionNode):
  """To be built up as an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      class_type=None,
      element=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.class_type = class_type
    if element is None:
      self.element = []
    else:
      self.element = element


class IntervalNode(ExpressionNode):
  """The IntervalNode selector defines an interval value."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      low_closed=True,
      high_closed=True,
      low=None,
      low_closed_expression=None,
      high=None,
      high_closed_expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.low_closed = low_closed
    self.high_closed = high_closed
    self.low = low
    self.low_closed_expression = low_closed_expression
    self.high = high
    self.high_closed_expression = high_closed_expression


class ListNode(ExpressionNode):
  """ListNode, whose elements are the result of evaluating the arguments to the list selector, in order."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      type_specifier=None,
      element=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.type_specifier = type_specifier
    if element is None:
      self.element = []
    else:
      self.element = element


class AndNode(BinaryExpressionNode):
  """The AndNode operator returns the logical conjunction of its arguments."""


class OrNode(BinaryExpressionNode):
  """The OrNode operator returns the logical disjunction of its arguments."""


class XorNode(BinaryExpressionNode):
  """The XorNode operator returns the exclusive or of its arguments."""


class ImpliesNode(BinaryExpressionNode):
  """The ImpliesNode operator returns the logical implication of its arguments."""


class NotNode(UnaryExpressionNode):
  """The NotNode operator returns the logical negation of its argument."""


class IfNode(ExpressionNode):
  """The IfNode operator evaluates a condition, and returns the then argument if the condition evaluates to true; if the condition evaluates to false or null, the result of the else argument is returned."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      condition=None,
      then=None,
      else_=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.condition = condition
    self.then = then
    self.else_ = else_


class CaseItemNode(ElementNode):

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      when=None,
      then=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.when = when
    self.then = then


class CaseNode(ExpressionNode):
  """The CaseNode operator allows for multiple conditional expressions to be chained together in a single expression, rather than having to nest multiple If operators."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      comparand=None,
      case_item=None,
      else_=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.comparand = comparand
    if case_item is None:
      self.case_item = []
    else:
      self.case_item = case_item
    self.else_ = else_


class NullNode(ExpressionNode):
  """The NullNode operator returns a null, or missing information marker."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, value_type=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.value_type = value_type


class IsNullNode(UnaryExpressionNode):
  """The IsNullNode operator determines whether or not its argument evaluates to null."""


class IsTrueNode(UnaryExpressionNode):
  """The IsTrueNode operator determines whether or not its argument evaluates to true."""


class IsFalseNode(UnaryExpressionNode):
  """The IsFalseNode operator determines whether or not its argument evaluates to false."""


class CoalesceNode(NaryExpressionNode):
  """The CoalesceNode operator returns the first non-null result in a list of arguments."""


class IsNode(UnaryExpressionNode):
  """Of a result to be tested."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      is_type=None,
      is_type_specifier=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.is_type = is_type
    self.is_type_specifier = is_type_specifier


class AsNode(UnaryExpressionNode):
  """The AsNode operator allows the result of an expression to be cast as a given target type."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      as_type=None,
      strict=False,
      as_type_specifier=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.as_type = as_type
    self.strict = strict
    self.as_type_specifier = as_type_specifier


class ConvertNode(UnaryExpressionNode):
  """The ConvertNode operator converts a value to a specific type."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      to_type=None,
      to_type_specifier=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.to_type = to_type
    self.to_type_specifier = to_type_specifier


class CanConvertNode(UnaryExpressionNode):
  """The CanConvertNode operator returns true if the given value can be converted to a specific type, and false otherwise."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      to_type=None,
      to_type_specifier=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.to_type = to_type
    self.to_type_specifier = to_type_specifier


class ToBooleanNode(UnaryExpressionNode):
  """The ToBooleanNode operator converts the value of its argument to a Boolean value."""


class ConvertsToBooleanNode(UnaryExpressionNode):
  """The ConvertsToBooleanNode operator returns true if the value of its argument is or can be converted to a Boolean value."""


class ToConceptNode(UnaryExpressionNode):
  """Code to a concept value with the given code as its primary and only code."""


class ConvertsToDateNode(UnaryExpressionNode):
  """The ConvertsToDateNode operator returns true if the value of its argument is or can be converted to a Date value."""


class ToDateNode(UnaryExpressionNode):
  """The ToDateNode operator converts the value of its argument to a Date value."""


class ConvertsToDateTimeNode(UnaryExpressionNode):
  """The ConvertsToDateTimeNode operator returns true if the value of its argument is or can be converted to a DateTime value."""


class ToDateTimeNode(UnaryExpressionNode):
  """The ToDateTimeNode operator converts the value of its argument to a DateTime value."""


class ConvertsToDecimalNode(UnaryExpressionNode):
  """The ConvertsToDecimalNode operator returns true if the value of its argument is or can be converted to a Decimal value."""


class ToDecimalNode(UnaryExpressionNode):
  """The ToDecimalNode operator converts the value of its argument to a Decimal value."""


class ConvertsToIntegerNode(UnaryExpressionNode):
  """The ConvertsToIntegerNode operator returns true if the value of its argument is or can be converted to an Integer value."""


class ToIntegerNode(UnaryExpressionNode):
  """The ToIntegerNode operator converts the value of its argument to an Integer value."""


class ConvertsToLongNode(UnaryExpressionNode):
  """The ConvertsToLongNode operator returns true if the value of its argument is or can be converted to a Long value."""


class ToLongNode(UnaryExpressionNode):
  """The ToLongNode operator converts the value of its argument to a Long value."""


class ConvertsToQuantityNode(UnaryExpressionNode):
  """The ConvertsToQuantityNode operator returns true if the value of its argument is or can be converted to a Quantity value."""


class ToQuantityNode(UnaryExpressionNode):
  """The ToQuantityNode operator converts the value of its argument to a Quantity value."""


class ConvertsToRatioNode(UnaryExpressionNode):
  """The ConvertsToRatioNode operator returns true if the value of its argument is or can be converted to a Ratio value."""


class ToRatioNode(UnaryExpressionNode):
  """The ToRatioNode operator converts the value of its argument to a Ratio value."""


class ToListNode(UnaryExpressionNode):
  """The ToListNode operator returns its argument as a List value."""


class ToCharsNode(UnaryExpressionNode):
  """The ToCharsNode operator takes a string and returns a list with one string for each character in the input, in the order in which they appear in the string."""


class ConvertsToStringNode(UnaryExpressionNode):
  """The ConvertsToStringNode operator returns true if the value of its argument is or can be converted to a String value."""


class ToStringNode(UnaryExpressionNode):
  """The ToStringNode operator converts the value of its argument to a String value."""


class ConvertsToTimeNode(UnaryExpressionNode):
  """The ConvertsToTimeNode operator returns true if the value of its argument is or can be converted to a Time value."""


class ToTimeNode(UnaryExpressionNode):
  """The ToTimeNode operator converts the value of its argument to a Time value."""


class CanConvertQuantityNode(BinaryExpressionNode):
  """The CanConvertQuantityNode operator returns true if the Quantity can be converted to an equivalent Quantity with the given Unit."""


class ConvertQuantityNode(BinaryExpressionNode):
  """The ConvertQuantityNode operator converts a Quantity to an equivalent Quantity with the given unit."""


class EqualNode(BinaryExpressionNode):
  """The EqualNode operator returns true if the arguments are equal; false if the arguments are known unequal, and null otherwise."""


class EquivalentNode(BinaryExpressionNode):
  """The EquivalentNode operator returns true if the arguments are the same value, or if they are both null; and false otherwise."""


class NotEqualNode(BinaryExpressionNode):
  """The NotEqualNode operator returns true if its arguments are not the same value."""


class LessNode(BinaryExpressionNode):
  """The LessNode operator returns true if the first argument is less than the second argument."""


class GreaterNode(BinaryExpressionNode):
  """The GreaterNode operator returns true if the first argument is greater than the second argument."""


class LessOrEqualNode(BinaryExpressionNode):
  """The LessOrEqualNode operator returns true if the first argument is less than or equal to the second argument."""


class GreaterOrEqualNode(BinaryExpressionNode):
  """The GreaterOrEqualNode operator returns true if the first argument is greater than or equal to the second argument."""


class AddNode(BinaryExpressionNode):
  """The AddNode operator performs numeric addition of its arguments."""


class SubtractNode(BinaryExpressionNode):
  """The SubtractNode operator performs numeric subtraction of its arguments."""


class MultiplyNode(BinaryExpressionNode):
  """The MultiplyNode operator performs numeric multiplication of its arguments."""


class DivideNode(BinaryExpressionNode):
  """The DivideNode operator performs numeric division of its arguments."""


class TruncatedDivideNode(BinaryExpressionNode):
  """The TruncatedDivideNode operator performs integer division of its arguments."""


class ModuloNode(BinaryExpressionNode):
  """The ModuloNode operator computes the remainder of the division of its arguments."""


class CeilingNode(UnaryExpressionNode):
  """The CeilingNode operator returns the first integer greater than or equal to the argument."""


class FloorNode(UnaryExpressionNode):
  """The FloorNode operator returns the first integer less than or equal to the argument."""


class TruncateNode(UnaryExpressionNode):
  """The TruncateNode operator returns the integer component of its argument."""


class AbsNode(UnaryExpressionNode):
  """The AbsNode operator returns the absolute value of its argument."""


class NegateNode(UnaryExpressionNode):
  """The NegateNode operator returns the negative of its argument."""


class RoundNode(OperatorExpressionNode):
  """The RoundNode operator returns the nearest integer to its argument."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.operand = operand
    self.precision = precision


class LnNode(UnaryExpressionNode):
  """The LnNode operator computes the natural logarithm of its argument."""


class ExpNode(UnaryExpressionNode):
  """The ExpNode operator returns e raised to the given power."""


class LogNode(BinaryExpressionNode):
  """The LogNode operator computes the logarithm of its first argument, using the second argument as the base."""


class PowerNode(BinaryExpressionNode):
  """The PowerNode operator raises the first argument to the power given by the second argument."""


class SuccessorNode(UnaryExpressionNode):
  """The SuccessorNode operator returns the successor of the argument."""


class PredecessorNode(UnaryExpressionNode):
  """The PredecessorNode operator returns the predecessor of the argument."""


class MinValueNode(ExpressionNode):
  """The MinValueNode operator returns the minimum representable value for the given type."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, value_type=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.value_type = value_type


class MaxValueNode(ExpressionNode):
  """The MaxValueNode operator returns the maximum representable value for the given type."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, value_type=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.value_type = value_type


class PrecisionNode(UnaryExpressionNode):
  """The PrecisionNode operator returns the number of digits of precision in the input value."""


class LowBoundaryNode(BinaryExpressionNode):
  """The LowBoundaryNode operator returns the least possible value of the input to the specified precision."""


class HighBoundaryNode(BinaryExpressionNode):
  """The HighBoundaryNode operator returns the greatest possible value of the input to the specified precision."""


class ConcatenateNode(NaryExpressionNode):
  """The ConcatenateNode operator performs string concatenation of its arguments."""


class CombineNode(OperatorExpressionNode):
  """The CombineNode operator combines a list of strings, optionally separating each string with the given separator."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      source=None,
      separator=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.source = source
    self.separator = separator


class SplitNode(OperatorExpressionNode):
  """The SplitNode operator splits a string into a list of strings using a separator."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      string_to_split=None,
      separator=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.string_to_split = string_to_split
    self.separator = separator


class SplitOnMatchesNode(OperatorExpressionNode):
  """The SplitOnMatchesNode operator splits a string into a list of strings using matches of a regex pattern."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      string_to_split=None,
      separator_pattern=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.string_to_split = string_to_split
    self.separator_pattern = separator_pattern


class LengthNode(UnaryExpressionNode):
  """The LengthNode operator returns the length of its argument."""


class UpperNode(UnaryExpressionNode):
  """The UpperNode operator returns the given string with all characters converted to their upper case equivalents."""


class LowerNode(UnaryExpressionNode):
  """The LowerNode operator returns the given string with all characters converted to their lowercase equivalents."""


class IndexerNode(BinaryExpressionNode):
  """The IndexerNode operator returns the indexth element in a string or list."""


class PositionOfNode(OperatorExpressionNode):
  """The PositionOfNode operator returns the 0-based index of the beginning given pattern in the given string."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      pattern=None,
      string=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.pattern = pattern
    self.string = string


class LastPositionOfNode(OperatorExpressionNode):
  """The LastPositionOfNode operator returns the 0-based index of the beginning of the last appearance of the given pattern in the given string."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      pattern=None,
      string=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.pattern = pattern
    self.string = string


class SubstringNode(OperatorExpressionNode):
  """The SubstringNode operator returns the string within stringToSub, starting at the 0-based index startIndex, and consisting of length characters."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      string_to_sub=None,
      start_index=None,
      length=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.string_to_sub = string_to_sub
    self.start_index = start_index
    self.length = length


class StartsWithNode(BinaryExpressionNode):
  """The StartsWithNode operator returns true if the given string starts with the given prefix."""


class EndsWithNode(BinaryExpressionNode):
  """The EndsWithNode operator returns true if the given string ends with the given suffix."""


class MatchesNode(BinaryExpressionNode):
  """The MatchesNode operator returns true if the given string matches the given regular expression pattern."""


class ReplaceMatchesNode(TernaryExpressionNode):
  """The ReplaceMatchesNode operator matches the given string using the regular expression pattern, replacing each match with the given substitution."""


class DurationBetweenNode(BinaryExpressionNode):
  """The DurationBetweenNode operator returns the number of whole calendar periods for the specified precision between the first and second arguments."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class DifferenceBetweenNode(BinaryExpressionNode):
  """The DifferenceBetweenNode operator returns the number of boundaries crossed for the specified precision between the first and second arguments."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class DateFromNode(UnaryExpressionNode):
  """The DateFromNode operator returns the date (with no time components specified) of the argument."""


class TimeFromNode(UnaryExpressionNode):
  """The TimeFromNode operator returns the Time of the argument."""


class TimezoneFromNode(UnaryExpressionNode):
  """DEPRECATED (as of 1.4): The TimezoneFromNode operator returns the timezone offset of the argument."""


class TimezoneOffsetFromNode(UnaryExpressionNode):
  """The TimezoneOffsetFromNode operator returns the timezone offset of the argument."""


class DateTimeComponentFromNode(UnaryExpressionNode):
  """The DateTimeComponentFromNode operator returns the specified component of the argument."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class TimeOfDayNode(OperatorExpressionNode):
  """The TimeOfDayNode operator returns the time-of-day of the start timestamp associated with the evaluation request."""


class TodayNode(OperatorExpressionNode):
  """The TodayNode operator returns the date (with no time component) of the start timestamp associated with the evaluation request."""


class NowNode(OperatorExpressionNode):
  """The NowNode operator returns the date and time of the start timestamp associated with the evaluation request."""


class DateNode(OperatorExpressionNode):
  """The DateNode operator constructs a date value from the given components."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      year=None,
      month=None,
      day=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.year = year
    self.month = month
    self.day = day


class DateTimeNode(OperatorExpressionNode):
  """The DateTimeNode operator constructs a DateTimeNode value from the given components."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      year=None,
      month=None,
      day=None,
      hour=None,
      minute=None,
      second=None,
      millisecond=None,
      timezone_offset=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.year = year
    self.month = month
    self.day = day
    self.hour = hour
    self.minute = minute
    self.second = second
    self.millisecond = millisecond
    self.timezone_offset = timezone_offset


class TimeNode(OperatorExpressionNode):
  """The TimeNode operator constructs a time value from the given components."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      hour=None,
      minute=None,
      second=None,
      millisecond=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.hour = hour
    self.minute = minute
    self.second = second
    self.millisecond = millisecond


class SameAsNode(BinaryExpressionNode):
  """The SameAsNode operator is defined for Date, DateTime, and Time values, as well as intervals."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class SameOrBeforeNode(BinaryExpressionNode):
  """The SameOrBeforeNode operator is defined for Date, DateTime, and Time values, as well as intervals."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class SameOrAfterNode(BinaryExpressionNode):
  """The SameOrAfterNode operator is defined for Date, DateTime, and Time values, as well as intervals."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class PointFromNode(UnaryExpressionNode):
  """The PointFromNode expression extracts the single point from the source interval."""


class WidthNode(UnaryExpressionNode):
  """The WidthNode operator returns the width of an interval."""


class SizeNode(UnaryExpressionNode):
  """The SizeNode operator returns the size of an interval."""


class StartNode(UnaryExpressionNode):
  """The StartNode operator returns the starting point of an interval."""


class EndNode(UnaryExpressionNode):
  """The EndNode operator returns the ending point of an interval."""


class ContainsNode(BinaryExpressionNode):
  """The ContainsNode operator returns true if the first operand contains the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class ProperContainsNode(BinaryExpressionNode):
  """The ProperContainsNode operator returns true if the first operand properly contains the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class InNode(BinaryExpressionNode):
  """The InNode operator tests for membership in an interval or list."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class ProperInNode(BinaryExpressionNode):
  """The ProperInNode operator tests for proper membership in an interval or list."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class IncludesNode(BinaryExpressionNode):
  """The IncludesNode operator returns true if the first operand completely includes the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class IncludedInNode(BinaryExpressionNode):
  """The IncludedInNode operator returns true if the first operand is completely included in the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class ProperIncludesNode(BinaryExpressionNode):
  """The ProperIncludesNode operator returns true if the first operand includes the second, and is strictly larger."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class ProperIncludedInNode(BinaryExpressionNode):
  """The ProperIncludedInNode operator returns true if the first operand is included in the second, and is strictly smaller."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class BeforeNode(BinaryExpressionNode):
  """The BeforeNode operator is defined for Intervals, as well as Date, DateTime, and Time values."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class AfterNode(BinaryExpressionNode):
  """The AfterNode operator is defined for Intervals, as well as Date, DateTime, and Time values."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class MeetsNode(BinaryExpressionNode):
  """The MeetsNode operator returns true if the first interval ends immediately before the second interval starts, or if the first interval starts immediately after the second interval ends."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class MeetsBeforeNode(BinaryExpressionNode):
  """The MeetsBeforeNode operator returns true if the first interval ends immediately before the second interval starts."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class MeetsAfterNode(BinaryExpressionNode):
  """The MeetsAfterNode operator returns true if the first interval starts immediately after the second interval ends."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class OverlapsNode(BinaryExpressionNode):
  """The OverlapsNode operator returns true if the first interval overlaps the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class OverlapsBeforeNode(BinaryExpressionNode):
  """The OverlapsBeforeNode operator returns true if the first interval starts before and overlaps the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class OverlapsAfterNode(BinaryExpressionNode):
  """The OverlapsAfterNode operator returns true if the first interval overlaps and ends after the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class StartsNode(BinaryExpressionNode):
  """The StartsNode operator returns true if the first interval starts the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class EndsNode(BinaryExpressionNode):
  """The EndsNode operator returns true if the first interval ends the second."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class CollapseNode(BinaryExpressionNode):
  """The CollapseNode operator returns the unique set of intervals that completely covers the ranges present in the given list of intervals."""


class ExpandNode(BinaryExpressionNode):
  """The ExpandNode operator returns the set of intervals of size per for all the ranges present in the given list of intervals, or the list of points covering the range of the given interval, if invoked on a single interval."""


class UnionNode(NaryExpressionNode):
  """The UnionNode operator returns the union of its arguments."""


class IntersectNode(NaryExpressionNode):
  """The IntersectNode operator returns the intersection of its arguments."""


class ExceptNode(NaryExpressionNode):
  """The ExceptNode operator returns the set difference of the two arguments."""


class ExistsNode(UnaryExpressionNode):
  """The ExistsNode operator returns true if the list contains any elements."""


class TimesNode(BinaryExpressionNode):
  """The TimesNode operator performs the cartesian product of two lists of tuples."""


class FilterNode(ExpressionNode):
  """The FilterNode operator returns a list with only those elements in the source list for which the condition element evaluates to true."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      scope=None,
      source=None,
      condition=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.scope = scope
    self.source = source
    self.condition = condition


class FirstNode(OperatorExpressionNode):
  """The FirstNode operator returns the first element in a list."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      order_by=None,
      source=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.order_by = order_by
    self.source = source


class LastNode(OperatorExpressionNode):
  """The LastNode operator returns the last element in a list."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      order_by=None,
      source=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.order_by = order_by
    self.source = source


class SliceNode(OperatorExpressionNode):
  """The SliceNode operator returns a portion of the elements in a list, beginning at the start index and ending just before the ending index."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      source=None,
      start_index=None,
      end_index=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.source = source
    self.start_index = start_index
    self.end_index = end_index


class IndexOfNode(OperatorExpressionNode):
  """The IndexOfNode operator returns the 0-based index of the given element in the given source list."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      source=None,
      element=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.source = source
    self.element = element


class FlattenNode(UnaryExpressionNode):
  """The FlattenNode operator flattens a list of lists into a single list."""


class SortNode(ExpressionNode):
  """The SortNode operator returns a list with all the elements in source, sorted as described by the by element."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      source=None,
      by=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.source = source
    if by is None:
      self.by = []
    else:
      self.by = by


class ForEachNode(ExpressionNode):
  """The ForEachNode expression iterates over the list of elements in the source element, and returns a list with the same number of elements, where each element in the new list is the result of evaluating the element expression for each element in the source list."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      scope=None,
      source=None,
      element=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.scope = scope
    self.source = source
    self.element = element


class RepeatNode(ExpressionNode):
  """The RepeatNode expression performs successive ForEach until no new elements are returned."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      scope=None,
      source=None,
      element=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.scope = scope
    self.source = source
    self.element = element


class DistinctNode(UnaryExpressionNode):
  """The DistinctNode operator takes a list of elements and returns a list containing only the unique elements within the input."""


class CurrentNode(ExpressionNode):
  """The CurrentNode expression returns the value of the object currently in scope."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, scope=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.scope = scope


class IterationNode(ExpressionNode):
  """The IterationNode expression returns the current iteration number of a scoped operation."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, scope=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.scope = scope


class TotalNode(ExpressionNode):
  """The TotalNode expression returns the current value of the total aggregation accumulator in an aggregate operation."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, scope=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.scope = scope


class SingletonFromNode(UnaryExpressionNode):
  """The SingletonFromNode expression extracts a single element from the source list."""


class AggregateExpressionNode(ExpressionNode):
  """Aggregate expressions perform operations on lists of data, either directly on a list of scalars, or indirectly on a list of objects, with a reference to a property present on each object in the list."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      path=None,
      signature=None,
      source=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.path = path
    if signature is None:
      self.signature = []
    else:
      self.signature = signature
    self.source = source


class AggregateNode(AggregateExpressionNode):
  """The AggregateNode operator performs custom aggregation by evaluating an expression for each element of the source."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      path=None,
      signature=None,
      source=None,
      iteration=None,
      initial_value=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, path, signature, source
    )
    self.iteration = iteration
    self.initial_value = initial_value


class CountNode(AggregateExpressionNode):
  """The CountNode operator returns the number of non-null elements in the source."""


class SumNode(AggregateExpressionNode):
  """The SumNode operator returns the sum of non-null elements in the source."""


class ProductNode(AggregateExpressionNode):
  """The ProductNode operator returns the geometric product of non-null elements in the source."""


class MinNode(AggregateExpressionNode):
  """The MinNode operator returns the minimum element in the source."""


class MaxNode(AggregateExpressionNode):
  """The MaxNode operator returns the maximum element in the source."""


class AvgNode(AggregateExpressionNode):
  """The AvgNode operator returns the average of the non-null elements in source."""


class GeometricMeanNode(AggregateExpressionNode):
  """The GeometricMeanNode operator returns the geometric mean of the non-null elements in source."""


class MedianNode(AggregateExpressionNode):
  """The MedianNode operator returns the median of the elements in source."""


class ModeNode(AggregateExpressionNode):
  """The ModeNode operator returns the statistical mode of the elements in source."""


class VarianceNode(AggregateExpressionNode):
  """The VarianceNode operator returns the statistical variance of the elements in source."""


class PopulationVarianceNode(AggregateExpressionNode):
  """The PopulationVarianceNode operator returns the statistical population variance of the elements in source."""


class StdDevNode(AggregateExpressionNode):
  """The StdDevNode operator returns the statistical standard deviation of the elements in source."""


class PopulationStdDevNode(AggregateExpressionNode):
  """The PopulationStdDevNode operator returns the statistical standard deviation of the elements in source."""


class AllTrueNode(AggregateExpressionNode):
  """The AllTrueNode operator returns true if all the non-null elements in source are true."""


class AnyTrueNode(AggregateExpressionNode):
  """The AnyTrueNode operator returns true if any non-null element in source is true."""


class PropertyNode(ExpressionNode):
  """The PropertyNode operator returns the value of the property on source specified by the path attribute."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      path=None,
      scope=None,
      source=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.path = path
    self.scope = scope
    self.source = source


class AliasedQuerySourceNode(ElementNode):
  """The AliasedQuerySourceNode element defines a single source for inclusion in a query scope."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      alias=None,
      expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.alias = alias
    self.expression = expression


class LetClauseNode(ElementNode):
  """The LetClauseNode element allows any number of expression definitions to be introduced within a query scope."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      identifier=None,
      expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.identifier = identifier
    self.expression = expression


class RelationshipClauseNode(AliasedQuerySourceNode):
  """The RelationshipClauseNode element allows related sources to be used to restrict the elements included from another source in a query scope."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      alias=None,
      expression=None,
      such_that=None,
  ):
    super().__init__(result_type_name, result_type_specifier, alias, expression)
    self.such_that = such_that


class WithNode(RelationshipClauseNode):
  """The WithNode clause restricts the elements of a given source to only those elements that have elements in the related source that satisfy the suchThat condition."""


class WithoutNode(RelationshipClauseNode):
  """The WithoutNode clause restricts the elements of a given source to only those elements that do not have elements in the related source that satisfy the suchThat condition."""


class SortByItemNode(ElementNode):

  def __init__(
      self, result_type_name=None, result_type_specifier=None, direction=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.direction = direction


class ByDirectionNode(SortByItemNode):
  """The ByDirectionNode element specifies that the sort should be performed using the given direction."""


class ByColumnNode(SortByItemNode):
  """The ByColumnNode element specifies that the sort should be performed using the given column and direction."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      direction=None,
      path=None,
  ):
    super().__init__(result_type_name, result_type_specifier, direction)
    self.path = path


class ByExpressionNode(SortByItemNode):
  """The ByExpressionNode element specifies that the sort should be performed using the given expression and direction."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      direction=None,
      expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier, direction)
    self.expression = expression


class SortClauseNode(ElementNode):
  """The SortClauseNode element defines the sort order for the query."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, by=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    if by is None:
      self.by = []
    else:
      self.by = by


class ReturnClauseNode(ElementNode):
  """The ReturnClauseNode element defines the shape of the result set of the query."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      distinct=True,
      expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.distinct = distinct
    self.expression = expression


class AggregateClauseNode(ElementNode):
  """The AggregateClauseNode element defines the result of the query in terms of an aggregation expression performed for each item in the query."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      identifier=None,
      distinct=False,
      expression=None,
      starting=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.identifier = identifier
    self.distinct = distinct
    self.expression = expression
    self.starting = starting


class QueryNode(ExpressionNode):
  """The QueryNode operator represents a clause-based query."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      source=None,
      let=None,
      relationship=None,
      where=None,
      return_=None,
      aggregate=None,
      sort=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    if source is None:
      self.source = []
    else:
      self.source = source
    if let is None:
      self.let = []
    else:
      self.let = let
    if relationship is None:
      self.relationship = []
    else:
      self.relationship = relationship
    self.where = where
    self.return_ = return_
    self.aggregate = aggregate
    self.sort = sort


class AliasRefNode(ExpressionNode):
  """The AliasRefNode expression allows for the reference of a specific source within the scope of a query."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, name=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name


class QueryLetRefNode(ExpressionNode):
  """The QueryLetRefNode expression allows for the reference of a specific let definition within the scope of a query."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, name=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name


class ChildrenNode(OperatorExpressionNode):
  """For structured types, the ChildrenNode operator returns a list of all the values of the elements of the type."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      source=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.source = source


class DescendentsNode(OperatorExpressionNode):
  """For structured types, the DescendentsNode operator returns a list of all the values of the elements of the type, recursively."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      source=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.source = source


class MessageNode(OperatorExpressionNode):
  """The MessageNode operator is used to support errors, warnings, messages, and tracing in an ELM evaluation environment."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      source=None,
      condition=None,
      code=None,
      severity=None,
      message=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.source = source
    self.condition = condition
    self.code = code
    self.severity = severity
    self.message = message


class CqlToElmBaseNode:
  """Defines the abstract base type for all annotation elements in the cql translator."""


class AnnotationNode(CqlToElmBaseNode):

  def __init__(self, t=None, s=None):
    super().__init__()
    if t is None:
      self.t = []
    else:
      self.t = t
    self.s = s


class TagNode:

  def __init__(self, name=None, value=None):
    self.name = name
    self.value = value


class LocatorNode(CqlToElmBaseNode):
  """Used to locate sections of the underlying CQL."""

  def __init__(
      self,
      library_system=None,
      library_id=None,
      library_version=None,
      start_line=None,
      start_char=None,
      end_line=None,
      end_char=None,
  ):
    super().__init__()
    self.library_system = library_system
    self.library_id = library_id
    self.library_version = library_version
    self.start_line = start_line
    self.start_char = start_char
    self.end_line = end_line
    self.end_char = end_char


class CqlToElmErrorNode(LocatorNode):
  """Represents CQL to ELM conversion errors."""

  def __init__(
      self,
      library_system=None,
      library_id=None,
      library_version=None,
      start_line=None,
      start_char=None,
      end_line=None,
      end_char=None,
      message=None,
      error_type=None,
      error_severity=None,
      target_include_library_system=None,
      target_include_library_id=None,
      target_include_library_version_id=None,
  ):
    super().__init__(
        library_system,
        library_id,
        library_version,
        start_line,
        start_char,
        end_line,
        end_char,
    )
    self.message = message
    self.error_type = error_type
    self.error_severity = error_severity
    self.target_include_library_system = target_include_library_system
    self.target_include_library_id = target_include_library_id
    self.target_include_library_version_id = target_include_library_version_id


class CqlToElmInfoNode(CqlToElmBaseNode):

  def __init__(self, translator_version=None, translator_options=None):
    super().__init__()
    self.translator_version = translator_version
    self.translator_options = translator_options


class VersionedIdentifierNode:
  """VersionedIdentifierNode is composed of three parts: (1) an optional system, or."""

  def __init__(self, id_=None, system=None, version=None):
    self.id_ = id_
    self.system = system
    self.version = version


class UsingDefNode(ElementNode):
  """Defines a data model that is available within the artifact."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      uri=None,
      version=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.uri = uri
    self.version = version


class IncludeDefNode(ElementNode):
  """Includes a library for use within the artifact."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      media_type='application/elm+xml',
      path=None,
      version=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.media_type = media_type
    self.path = path
    self.version = version


class ContextDefNode(ElementNode):
  """Defines a context definition statement."""

  def __init__(
      self, result_type_name=None, result_type_specifier=None, name=None
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name


class LibraryNode(ElementNode):
  """A LibraryNode is an instance of a CQL-ELM library."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      identifier=None,
      schema_identifier=None,
      usings=None,
      includes=None,
      parameters=None,
      code_systems=None,
      value_sets=None,
      codes=None,
      concepts=None,
      contexts=None,
      statements=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.identifier = identifier
    self.schema_identifier = schema_identifier
    self.usings = usings
    self.includes = includes
    self.parameters = parameters
    self.code_systems = code_systems
    self.value_sets = value_sets
    self.codes = codes
    self.concepts = concepts
    self.contexts = contexts
    self.statements = statements


class CalculateAgeAtNode(BinaryExpressionNode):
  """Calculates the age in the specified precision of a person born on a given date, as of another given date."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class CalculateAgeNode(UnaryExpressionNode):
  """Calculates the age in the specified precision of a person born on the given date."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      operand=None,
      precision=None,
  ):
    super().__init__(
        result_type_name, result_type_specifier, signature, operand
    )
    self.precision = precision


class RatioNode(ExpressionNode):
  """Defines a ratio between two quantities."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      numerator=None,
      denominator=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.numerator = numerator
    self.denominator = denominator


class QuantityNode(ExpressionNode):
  """Defines a clinical quantity."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      value=None,
      unit=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.value = value
    self.unit = unit


class SubsumedByNode(BinaryExpressionNode):
  """The SubsumedByNode operator returns true if the given codes are equivalent, or if the first code is subsumed by the second code (i.e."""


class SubsumesNode(BinaryExpressionNode):
  """The SubsumesNode operator returns true if the given codes are equivalent, or if the first code subsumes the second (i.e."""


class ExpandValueSetNode(UnaryExpressionNode):
  """The ExpandValueSetNode operator returns the current expansion for the given value set."""


class AnyInValueSetNode(OperatorExpressionNode):
  """The AnyInValueSetNode operator returns true if any of the given codes are in the given value set."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      codes=None,
      valueset=None,
      valueset_expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.codes = codes
    self.valueset = valueset
    self.valueset_expression = valueset_expression


class InValueSetNode(OperatorExpressionNode):
  """The InValueSetNode operator returns true if the given code is in the given value set."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      code=None,
      valueset=None,
      valueset_expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.code = code
    self.valueset = valueset
    self.valueset_expression = valueset_expression


class AnyInCodeSystemNode(OperatorExpressionNode):
  """The AnyInCodeSystemNode operator returns true if any of the given codes are in the given code system."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      codes=None,
      codesystem=None,
      codesystem_expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.codes = codes
    self.codesystem = codesystem
    self.codesystem_expression = codesystem_expression


class InCodeSystemNode(OperatorExpressionNode):
  """The InCodeSystemNode operator returns true if the given code is in the given code system."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      signature=None,
      code=None,
      codesystem=None,
      codesystem_expression=None,
  ):
    super().__init__(result_type_name, result_type_specifier, signature)
    self.code = code
    self.codesystem = codesystem
    self.codesystem_expression = codesystem_expression


class ConceptNode(ExpressionNode):
  """Represents a literal concept selector."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      display=None,
      code=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.display = display
    if code is None:
      self.code = []
    else:
      self.code = code


class CodeNode(ExpressionNode):
  """Represents a literal code selector."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      code=None,
      display=None,
      system=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.code = code
    self.display = display
    self.system = system


class ConceptRefNode(ExpressionNode):
  """The ConceptRefNode expression allows a previously defined concept to be referenced within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.library_name = library_name


class CodeRefNode(ExpressionNode):
  """The CodeRefNode expression allows a previously defined code to be referenced within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.library_name = library_name


class ValueSetRefNode(ExpressionNode):
  """The ValueSetRefNode expression allows a previously defined named value set to be referenced within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
      preserve=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.library_name = library_name
    self.preserve = preserve


class CodeSystemRefNode(ExpressionNode):
  """The CodeSystemRefNode expression allows a previously defined named code system to be referenced within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      library_name=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.library_name = library_name


class ConceptDefNode(ElementNode):
  """Defines a concept identifier that can then be used to reference single concepts anywhere within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      display=None,
      access_level='Public',
      code=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.display = display
    self.access_level = access_level
    if code is None:
      self.code = []
    else:
      self.code = code


class CodeDefNode(ElementNode):
  """Defines a code identifier that can then be used to reference single codes anywhere within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      id_=None,
      display=None,
      access_level='Public',
      code_system=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.id_ = id_
    self.display = display
    self.access_level = access_level
    self.code_system = code_system


class ValueSetDefNode(ElementNode):
  """Defines a value set identifier that can be referenced by name anywhere within an expression."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      id_=None,
      version=None,
      access_level='Public',
      code_system=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.id_ = id_
    self.version = version
    self.access_level = access_level
    if code_system is None:
      self.code_system = []
    else:
      self.code_system = code_system


class CodeSystemDefNode(ElementNode):
  """Defines a code system identifier that can then be used to identify code systems involved in value set definitions."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      name=None,
      id_=None,
      version=None,
      access_level='Public',
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.name = name
    self.id_ = id_
    self.version = version
    self.access_level = access_level


class SearchNode(PropertyNode):
  """The SearchNode operation provides an operator that returns the result of an indexing expression on an instance."""


class RetrieveNode(ExpressionNode):
  """The retrieve expression defines clinical data that will be used by the artifact."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      data_type=None,
      template_id=None,
      id_property=None,
      id_search=None,
      context_property=None,
      context_search=None,
      code_property=None,
      code_search=None,
      code_comparator=None,
      value_set_property=None,
      date_property=None,
      date_low_property=None,
      date_high_property=None,
      date_search=None,
      included_in=None,
      id_=None,
      codes=None,
      date_range=None,
      context=None,
      include=None,
      code_filter=None,
      date_filter=None,
      other_filter=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.data_type = data_type
    self.template_id = template_id
    self.id_property = id_property
    self.id_search = id_search
    self.context_property = context_property
    self.context_search = context_search
    self.code_property = code_property
    self.code_search = code_search
    self.code_comparator = code_comparator
    self.value_set_property = value_set_property
    self.date_property = date_property
    self.date_low_property = date_low_property
    self.date_high_property = date_high_property
    self.date_search = date_search
    self.included_in = included_in
    self.id_ = id_
    self.codes = codes
    self.date_range = date_range
    self.context = context
    if include is None:
      self.include = []
    else:
      self.include = include
    if code_filter is None:
      self.code_filter = []
    else:
      self.code_filter = code_filter
    if date_filter is None:
      self.date_filter = []
    else:
      self.date_filter = date_filter
    if other_filter is None:
      self.other_filter = []
    else:
      self.other_filter = other_filter


class IncludeElementNode(ElementNode):
  """Specifies include information for an include within a retrieve."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      include_from=None,
      related_data_type=None,
      related_property=None,
      related_search=None,
      is_reverse=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.include_from = include_from
    self.related_data_type = related_data_type
    self.related_property = related_property
    self.related_search = related_search
    self.is_reverse = is_reverse


class OtherFilterElementNode(ElementNode):
  """Specifies an arbitrarily-typed filter criteria for use within a retrieve, specified as either [property] [comparator] [value] or [search] [comparator] [value]."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      property_=None,
      search=None,
      comparator=None,
      value=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.property_ = property_
    self.search = search
    self.comparator = comparator
    self.value = value


class DateFilterElementNode(ElementNode):
  """Specifies a date-valued filter criteria for use within a retrieve, specified as either a date-valued [property], a date-value [lowproperty] and [highproperty] or a [search], and an expression that evaluates to a date or time type, an interval of a date or time type, or a time-valued quantity."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      property_=None,
      low_property=None,
      high_property=None,
      search=None,
      value=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.property_ = property_
    self.low_property = low_property
    self.high_property = high_property
    self.search = search
    self.value = value


class CodeFilterElementNode(ElementNode):
  """Specifies a terminology filter criteria for use within a retrieve, specified as either [property] [comparator] [value] or [search] [comparator] [value]."""

  def __init__(
      self,
      result_type_name=None,
      result_type_specifier=None,
      property_=None,
      value_set_property=None,
      search=None,
      comparator=None,
      value=None,
  ):
    super().__init__(result_type_name, result_type_specifier)
    self.property_ = property_
    self.value_set_property = value_set_property
    self.search = search
    self.comparator = comparator
    self.value = value

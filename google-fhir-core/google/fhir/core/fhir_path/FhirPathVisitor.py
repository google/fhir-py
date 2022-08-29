# Generated from fhir_path/FhirPath.g4 by ANTLR 4.9.3
from antlr4 import *
if __name__ is not None and "." in __name__:
  from .FhirPathParser import FhirPathParser
else:
  from FhirPathParser import FhirPathParser

# This class defines a complete generic visitor for a parse tree produced by FhirPathParser.


class FhirPathVisitor(ParseTreeVisitor):

  # Visit a parse tree produced by FhirPathParser#indexerExpression.
  def visitIndexerExpression(self,
                             ctx: FhirPathParser.IndexerExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#polarityExpression.
  def visitPolarityExpression(self,
                              ctx: FhirPathParser.PolarityExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#additiveExpression.
  def visitAdditiveExpression(self,
                              ctx: FhirPathParser.AdditiveExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#multiplicativeExpression.
  def visitMultiplicativeExpression(
      self, ctx: FhirPathParser.MultiplicativeExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#unionExpression.
  def visitUnionExpression(self, ctx: FhirPathParser.UnionExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#orExpression.
  def visitOrExpression(self, ctx: FhirPathParser.OrExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#andExpression.
  def visitAndExpression(self, ctx: FhirPathParser.AndExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#membershipExpression.
  def visitMembershipExpression(
      self, ctx: FhirPathParser.MembershipExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#inequalityExpression.
  def visitInequalityExpression(
      self, ctx: FhirPathParser.InequalityExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#invocationExpression.
  def visitInvocationExpression(
      self, ctx: FhirPathParser.InvocationExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#equalityExpression.
  def visitEqualityExpression(self,
                              ctx: FhirPathParser.EqualityExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#impliesExpression.
  def visitImpliesExpression(self,
                             ctx: FhirPathParser.ImpliesExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#termExpression.
  def visitTermExpression(self, ctx: FhirPathParser.TermExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#typeExpression.
  def visitTypeExpression(self, ctx: FhirPathParser.TypeExpressionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#invocationTerm.
  def visitInvocationTerm(self, ctx: FhirPathParser.InvocationTermContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#literalTerm.
  def visitLiteralTerm(self, ctx: FhirPathParser.LiteralTermContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#externalConstantTerm.
  def visitExternalConstantTerm(
      self, ctx: FhirPathParser.ExternalConstantTermContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#parenthesizedTerm.
  def visitParenthesizedTerm(self,
                             ctx: FhirPathParser.ParenthesizedTermContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#nullLiteral.
  def visitNullLiteral(self, ctx: FhirPathParser.NullLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#booleanLiteral.
  def visitBooleanLiteral(self, ctx: FhirPathParser.BooleanLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#stringLiteral.
  def visitStringLiteral(self, ctx: FhirPathParser.StringLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#numberLiteral.
  def visitNumberLiteral(self, ctx: FhirPathParser.NumberLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#dateLiteral.
  def visitDateLiteral(self, ctx: FhirPathParser.DateLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#dateTimeLiteral.
  def visitDateTimeLiteral(self, ctx: FhirPathParser.DateTimeLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#timeLiteral.
  def visitTimeLiteral(self, ctx: FhirPathParser.TimeLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#quantityLiteral.
  def visitQuantityLiteral(self, ctx: FhirPathParser.QuantityLiteralContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#externalConstant.
  def visitExternalConstant(self, ctx: FhirPathParser.ExternalConstantContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#memberInvocation.
  def visitMemberInvocation(self, ctx: FhirPathParser.MemberInvocationContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#functionInvocation.
  def visitFunctionInvocation(self,
                              ctx: FhirPathParser.FunctionInvocationContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#thisInvocation.
  def visitThisInvocation(self, ctx: FhirPathParser.ThisInvocationContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#indexInvocation.
  def visitIndexInvocation(self, ctx: FhirPathParser.IndexInvocationContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#totalInvocation.
  def visitTotalInvocation(self, ctx: FhirPathParser.TotalInvocationContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#function.
  def visitFunction(self, ctx: FhirPathParser.FunctionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#paramList.
  def visitParamList(self, ctx: FhirPathParser.ParamListContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#quantity.
  def visitQuantity(self, ctx: FhirPathParser.QuantityContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#unit.
  def visitUnit(self, ctx: FhirPathParser.UnitContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#dateTimePrecision.
  def visitDateTimePrecision(self,
                             ctx: FhirPathParser.DateTimePrecisionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#pluralDateTimePrecision.
  def visitPluralDateTimePrecision(
      self, ctx: FhirPathParser.PluralDateTimePrecisionContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#typeSpecifier.
  def visitTypeSpecifier(self, ctx: FhirPathParser.TypeSpecifierContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#qualifiedIdentifier.
  def visitQualifiedIdentifier(self,
                               ctx: FhirPathParser.QualifiedIdentifierContext):
    return self.visitChildren(ctx)

  # Visit a parse tree produced by FhirPathParser#identifier.
  def visitIdentifier(self, ctx: FhirPathParser.IdentifierContext):
    return self.visitChildren(ctx)


del FhirPathParser

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
"""FHIR Path support for R4."""

from google.fhir.core.fhir_path import _evaluation
from google.fhir.core.fhir_path import _fhir_path_data_types
from google.fhir.core.fhir_path import context
from google.fhir.core.fhir_path import expressions
from google.fhir.core.fhir_path import python_compiled_expressions
from google.fhir.r4 import primitive_handler

_PRIMITIVE_HANDLER = primitive_handler.PrimitiveHandler()


def compile_expression(
    structdef_url: str, fhir_context: context.FhirPathContext, fhir_path: str
) -> python_compiled_expressions.PythonCompiledExpression:
  """Compiles the FHIRPath expression for the given resource.

  Args:
    structdef_url: the URL of the FHIR StructureDefinition to use.
    fhir_context: a DefinitionLoader used to load FHIR structure definitions and
      dependencies.
    fhir_path: a FHIRPath expression to be run on the resource

  Returns:

    A PythonCompiledExpression representing the given FHIRPath string that can
    be evaluated against the target resource.
  """
  return python_compiled_expressions.PythonCompiledExpression.compile(
      fhir_path, _PRIMITIVE_HANDLER, structdef_url, fhir_context
  )


def builder(structdef_url: str,
            fhir_context: context.FhirPathContext) -> expressions.Builder:
  """Returns a FHIRPath expression builder.

  This gives the caller tab suggestions and early error detection when
  building FHIRPath expressions. See the documentation on the returned
  expressions.Builder for more details.

  Args:
    structdef_url: the URL of the FHIR StructureDefinition to use.
    fhir_context: a DefinitionLoader used to load FHIR structure definitions and
      dependencies.
  Returns: a builder object to creae FHIRPath expressions.
  """
  structdef = fhir_context.get_structure_definition(structdef_url)
  struct_type = _fhir_path_data_types.StructureDataType.from_proto(structdef)
  return expressions.Builder(
      _evaluation.RootMessageNode(fhir_context, struct_type),
      _PRIMITIVE_HANDLER)

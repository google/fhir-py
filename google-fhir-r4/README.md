Protocol buffer conversion, validation and utilities for FHIR R4 resources.

Users in interested in analyzing FHIR data should reference the
`google.fhir.views` package. This library contains underlying capabilities to
work with FHIR data represented in protocol buffers in PYthon code.

## FHIR JSON to and from Protocol Buffers
The `json_format` package supports converting FHIR protocol buffers to and
from the FHIR JSON format. Here are some simple examples:

```py
from google.fhir.r4 import json_format
from google.fhir.r4.proto.core.resources import patient_pb2

patient_json = """
{
  "resourceType" : "Patient",
  "id" : "example",
  "name" : [{
    "use" : "official",
    "family" : "Roosevelt",
    "given" : ["Franklin", "Delano"]
  }]
}
"""

# Read the JSON as a proto:
patient = json_format.json_fhir_string_to_proto(patient_json, patient_pb2.Patient)

# Get the FHIR JSON representation of the proto:
json_format.print_fhir_to_json_string(patient)
```

## FHIRPath support
FHIRPath is the basis of the `google-fhir-views` logic, but can also be used
directly against FHIR protos themselves. Here is an example:

```py
from google.fhir.r4 import fhir_path
from google.fhir.r4 import r4_package
from google.fhir.core.fhir_path import context

# Create the FHIRPath context for use.
fhir_path_context = context.LocalFhirPathContext(r4_package.load_base_r4())

# Compile the FHIRPath expressions for evaluation. This validates the FHIRPath and returns
# an optimized structure that can be efficiently evaluated over protocol buffers.
# The compiled expression is typically created once and reused for many following invocations.
expr = fhir_path.compile_expression('Patient', fhir_path_context, "name.where(use = 'official').family")

# Now we evaluate the expression on the given resource.
result = expr.evaluate(patient)

# Check to see if the result matched anything.
if result.has_value():
  # Gets the result as protocol buffer messages.
  result_as_messages = result.messages
  # Converts the result to a simple string, if applicable.
  result_as_simple_string = result.as_string()
```

The library also supports a fluent Python builder for creating FHIRPath expressions,
which is used extensively in the `google.fhir.views` package. Here is
the above string being created using a Python builder pattern:

```py
# FHIRPath builder for patients.
pat = fhir_path.builder('Patient', fhir_path_context)

# Build the expression and get it in string form.
fhir_path_string = pat.name.where(pat.name.use == 'official').family.fhir_path
```

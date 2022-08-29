
Google's tools for working with FHIR data in Python. This includes:

* Support for converting FHIR data to and from an efficient Protocol Buffer-based format.
* Support for creating  and analyzing views over large FHIR datasets. See the [Google FHIR Views](google-fhir-views/README.md) documentation for details.

This is not an officially supported Google product.

# Installation

These libraries are installed via pip.

## From PyPi
Users interested in FHIR Views and the underlying libraries can simply run
`pip install google-fhir-views[r4,bigquery]` to install that and its FHIR R4
and BigQuery dependencies.

Users looking for only the underlying FHIR Protocol Buffer support can
run `pip install google-fhir-r4` to retrieve only that and its dependencies.

## From source code
This can be installed locally, directly from source by running the following
commands in this directory. As always, doing so within a Python virtual
environment is recommended.

```
pip install ./google-fhir-core[bigquery]
pip install ./google-fhir-r4
pip install ./google-fhir-views[r4,bigquery]
```

See the [Google FHIR Views](google-fhir-views/README.md) documentation for
details on use.

# Trademark
FHIR® is the registered trademark of HL7 and is used with the permission of HL7. Use of the FHIR trademark does not constitute endorsement of this product by HL7.

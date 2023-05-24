
Google's tools for working with FHIR data in Python. This includes:

* Support for converting FHIR data to and from an efficient Protocol Buffer-based format.
* Support for creating and analyzing views over large FHIR datasets. See the [Google FHIR Views](google-fhir-views/README.md) documentation and [notebook examples](examples) for details.

This is not an officially supported Google product.

# Installation

__Note: Requires Python >= `3.8`.__

These libraries are installed via pip.

## From PyPi

Users interested in FHIR Views and the underlying libraries can simply run the
following to install the views library, BigQuery, Spark and FHIR R4 dependencies:

```
pip install google-fhir-views[r4,bigquery,spark]
```

Users who only need the BigQuery or Spark runners can run:

```
pip install google-fhir-views[r4,bigquery]
```

or

```
pip install google-fhir-views[r4,spark]
```
respectively, to reduce the installation size.

Note: If installing for use in a Jupyter notebook, it's best `pip install ...` _before_ starting the notebook kernel to avoid dependency version issues.

Users looking for only the underlying FHIR Protocol Buffer support can
run `pip install google-fhir-r4` to retrieve only that and its dependencies.

## From source code
This can be installed locally, directly from source by running the following
commands in this directory. As always, doing so within a Python virtual
environment is recommended.

### protoc installation prerequisite
This library generates Protocol Buffers for FHIR resources, so the protoc
executable must be available. This can be done on Linux by running:

```
apt install protobuf-compiler
protoc --version # Ensure version 3+
```

Or on MacOS with Homebrew:

```
brew install protobuf
protoc --version # Ensure version 3+
```

Windows users can download protoc releases [here](https://github.com/protocolbuffers/protobuf/releases).

### pip installation
Once protoc is available, the fhir-py libraries can be installed from source by
running the following in the fhir-py directory:

```
pip install ./google-fhir-core[bigquery,spark]
pip install ./google-fhir-r4
pip install ./google-fhir-views[r4,bigquery,spark]
```

See the [Google FHIR Views](google-fhir-views/README.md) documentation for
details on use.

# Contributors

Due to the nature of the initial commit squashing internal contributor history,
we would like to recognize some of those who contributed to the initial
commit work:
Ryan Brush ([@rbrush](https://github.com/rbrush)),
Cameron Tew ([@cam2337](https://github.com/cam2337)),
Ose Umolu ([@luid101](https://github.com/luid101)),
Walt Askew ([@waltaskew](https://github.com/waltaskew)),
Nick George ([@nickgeorge](https://github.com/nickgeorge)),
Wilson Sun ([@wilsonssun](https://github.com/wilsonssun))
Lisa Yin ([@lisayin](https://github.com/lisayin)),
Suyash Kumar ([@suyashkumar](https://github.com/suyashkumar)), and other Googlers.

In addition, contributors who committed after the initial squash commit can be
found in the
[GitHub contributors tab](https://github.com/google/fhir-py/graphs/contributors).

Thank you to all contributors!

# Trademark

FHIR® is the registered trademark of HL7 and is used with the permission of HL7. Use of the FHIR trademark does not constitute endorsement of this product by HL7.

#
# Copyright 2022 Google LLC
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
"""The setuptools script for the google-fhir-core distribution package."""

from distutils import spawn
import glob
import os
import pathlib
import subprocess
import warnings

import setuptools


def _get_protoc_command():
  """Finds the version-compatible protoc command."""
  if 'PROTOC' in os.environ and os.path.exists(os.environ['PROTOC']):
    protoc = os.environ['PROTOC']
  else:
    protoc = spawn.find_executable('protoc')

  if not protoc:
    raise FileNotFoundError('Could not find protoc executable. Please install '
                            'protoc to compile the google-fhir-core package.')

  result = subprocess.run(args=[protoc, '--version'],
                          capture_output=True, text=True, check=True)
  # The assumption of version number as the last output of `protoc --version`
  # appears to be stable across versions and the simplest way to check it.
  if result.stdout.split(' ')[-1] < '3.19':
    warnings.warn('protoc should be at least 3.19 to ensure forward '
                  'compatibility of generated files.')

  return protoc


def _generate_proto(protoc, source):
  """Invokes the Protocol Compiler to generate a _pb2.py."""

  if not os.path.exists(source):
    raise FileNotFoundError('Cannot find required file: {}'.format(source))

  output = source.replace('.proto', '_pb2.py')

  if (os.path.exists(output) and
      os.path.getmtime(source) < os.path.getmtime(output)):
    # No need to regenerate if output is newer than source.
    return

  print('Generating {}...'.format(output))
  protoc_args = [protoc, '-I.', '--python_out=.', source]
  subprocess.run(args=protoc_args, check=True)


def main():
  package_dir = os.path.dirname(os.path.abspath(__file__))
  namespace_packages = setuptools.find_namespace_packages(where=package_dir)
  long_description = pathlib.Path(package_dir).joinpath('README.md').read_text()

  version = os.environ.get('FHIR_PY_VERSION')
  if version is None:
    raise RuntimeError('FHIR_PY_VERSION must be set to build this package.')

  protoc_command = _get_protoc_command()
  proto_files = glob.glob('google/fhir/core/proto/*.proto')
  for proto_file in proto_files:
    _generate_proto(protoc_command, proto_file)

  setuptools.setup(
      name='google-fhir-core',
      version=version,
      description='Core components for working with FHIR.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Google LLC',
      author_email='google-fhir-pypi@google.com',
      url='https://github.com/google/fhir-py',
      download_url='https://github.com/google-py/fhir/releases',
      packages=namespace_packages,
      include_package_data=True,
      license='Apache 2.0',
      python_requires='>=3.7, <3.11',
      install_requires=[
          'absl-py~=1.1',
          'antlr4-python3-runtime~=4.9.3',
          'backports.zoneinfo~=0.2.1;python_version<"3.9"',
          'immutabledict~=2.2',
          # TODO(b/276635321): Fix compatibility issue with protobuf 4.x
          'protobuf~=3.19',
          'python-dateutil~=2.8',
      ],
      # Include proto files so consuming libraries can build protos that
      # use them.
      data_files=[('', proto_files)],
      extras_require={
          'bigquery': [
              'google-cloud-bigquery~=3.1',
              'sqlalchemy~=1.4',
              'sqlalchemy-bigquery~=1.4',
          ]
      },
      zip_safe=False,
      keywords='google,fhir,python,healthcare',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
      ],
  )


if __name__ == '__main__':
  main()

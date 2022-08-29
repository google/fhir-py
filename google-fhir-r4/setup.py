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
"""The setuptools script for the google-fhir-r4 distribution package."""

from distutils import spawn
import glob
import os
import pathlib
import subprocess
from urllib import request

import setuptools


def _get_protoc_command():
  """Finds the protoc command."""
  if 'PROTOC' in os.environ and os.path.exists(os.environ['PROTOC']):
    protoc = os.environ['PROTOC']
  else:
    protoc = spawn.find_executable('protoc')

  if not protoc:
    raise FileNotFoundError('Could not find protoc executable. Please install '
                            'protoc to compile the google-fhir-r4 package.')
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
  protoc_args = [
      protoc, '-I.', '-I../google-fhir-core', '--python_out=.', source
  ]
  subprocess.run(args=protoc_args, check=True)


# URL of the FHIR Package to retrieve.
_FHIR_PACKAGE_URL = 'http://hl7.org/fhir/R4/hl7.fhir.r4.core.tgz'

# Path relative to the setup.py directory to write the package.
_FHIR_PACKAGE_PATH = 'google/fhir/r4/data/hl7.fhir.r4.core.tgz'


def main():
  package_dir = os.path.dirname(os.path.abspath(__file__))
  namespace_packages = setuptools.find_namespace_packages(where=package_dir)
  long_description = pathlib.Path(package_dir).joinpath('README.md').read_text()

  protoc_command = _get_protoc_command()
  for proto_file in glob.glob(
      'google/fhir/r4/proto/**/*.proto', recursive=True):
    _generate_proto(protoc_command, proto_file)

  absolute_package_path = pathlib.Path(package_dir).joinpath(_FHIR_PACKAGE_PATH)

  # Download FHIR implementation guide.
  if not os.path.exists(absolute_package_path):
    request.urlretrieve(_FHIR_PACKAGE_URL, absolute_package_path)

  setuptools.setup(
      name='google-fhir-r4',
      version='0.8.0',
      description='Components for working with FHIR R4.',
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
          'google-fhir-core~=0.8',
          'backports.zoneinfo~=0.2.1;python_version<"3.9"',
          'immutabledict~=2.2',
          'protobuf~=3.13',
          'python-dateutil~=2.8',
      ],
      data_files=[('', [_FHIR_PACKAGE_PATH])],
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

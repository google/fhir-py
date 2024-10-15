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
"""The setuptools script for the google-fhir-views distribution package."""

import os
import pathlib

import setuptools


def main():
  package_dir = os.path.dirname(os.path.abspath(__file__))
  namespace_packages = setuptools.find_namespace_packages(where=package_dir)
  long_description = pathlib.Path(package_dir).joinpath('README.md').read_text()

  version = os.environ.get('FHIR_PY_VERSION')
  if version is None:
    raise RuntimeError('FHIR_PY_VERSION must be set to build this package.')

  setuptools.setup(
      name='google-fhir-views',
      version=version,
      description='Tools to create views of FHIR data for analysis.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      author='Google LLC',
      author_email='google-fhir-pypi@google.com',
      url='https://github.com/google/fhir-py',
      download_url='https://github.com/google/fhir-py/releases',
      packages=namespace_packages,
      include_package_data=True,
      license='Apache 2.0',
      python_requires='>=3.8, <3.12',
      install_requires=[
          'absl-py~=1.1',
          'deprecated~=1.2',
          f'google-fhir-core~={version}',
          'immutabledict~=2.2',
          'backports.zoneinfo~=0.2.1;python_version<"3.9"',
          # Pin to numpy 1 to avoid binary compatibility changes in numpy 2.
          'numpy~=1.24',
          'pandas~=1.1',
          'protobuf~=4.23',
          'python-dateutil~=2.8',
          'requests~=2.27',
          'requests-mock~=1.9',
      ],
      extras_require={
          'r4': [f'google-fhir-r4~={version}'],
          'bigquery': [
              f'google-fhir-core[bigquery]~={version}',
              'google-cloud-bigquery~=3.1',
              'sqlalchemy~=1.4',
              'sqlalchemy-bigquery~=1.4',
          ],
          'spark': [
              'sqlalchemy~=1.4',
              'pandas>=1.5',
              'pyhive>=0.6',
          ],
      },
      zip_safe=False,
      keywords='google,fhir,python,healthcare',
      classifiers=[
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Programming Language :: Python :: 3.11',
      ],
  )


if __name__ == '__main__':
  main()

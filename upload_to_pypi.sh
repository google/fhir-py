#!/bin/bash
#
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
#
# Script to publish releases to PyPI. Use the -t flag to publish to the PyPI
# test repo.
#
# This script may be run by hand or as part of a larger release process.
# Typically, users would create a git tag in the format of 'vX.Y.Z',
# corresponding to the project version. Running this script on that tag
# version will publish to PyPI.
#
# Users can install and verify the uploaded wheels this from the PyPI test repo
# with the following command:
#
# python3 -m pip install --index-url https://test.pypi.org/simple/ \
#   --extra-index-url https://pypi.org/simple/ google-fhir-views[r4,bigquery]

set -e

usage() {
  echo "Usage: $0 [ -t ]" 1>&2
  echo "Add the -t flag to upload to the PyPI test repository" 1>&2
  exit 1
}

USE_TEST_REPO=0

while getopts "t" options; do
  case "${options}" in
    t) USE_TEST_REPO=1 ;;
    *) usage ;;
  esac
done

export FHIR_PY_VERSION=`cat VERSION`

python3 -m venv ./test_env
. ./test_env/bin/activate

pip install wheel
pip install pytest
pip install twine

# Create wheels and install and test them to ensure they are built correctly.
pip wheel ./google-fhir-core --wheel-dir wheels
pip install ./wheels/google_fhir_core-${FHIR_PY_VERSION}-py3-none-any.whl[bigquery,spark]

pip wheel ./google-fhir-r4 --wheel-dir wheels --no-deps
pip install ./wheels/google_fhir_r4-${FHIR_PY_VERSION}-py3-none-any.whl

pip wheel ./google-fhir-views --wheel-dir wheels --no-deps
pip install ./wheels/google_fhir_views-${FHIR_PY_VERSION}-py3-none-any.whl[r4,bigquery,spark]

# Ensure tests pass.
pytest

if [[ $USE_TEST_REPO -eq 1 ]]
then
  twine upload --skip-existing -r testpypi -u google-fhir-test-pypi ./wheels/google*whl
else
  twine upload --skip-existing -u google-fhir-pypi ./wheels/google*whl
fi
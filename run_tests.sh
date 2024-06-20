#!/bin/bash
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

# Convenience script to run tests, installing needed packages into a test
# environment and running pytest.

python3 -m venv ./test_env
. ./test_env/bin/activate

export FHIR_PY_VERSION=`cat VERSION`

pip install wheel

# Version pinned for https://github.com/pytest-dev/pytest/issues/12275
pip install pytest==8.1.2

# Create wheels and install and test them to ensure they are built correctly.
pip wheel ./google-fhir-core --wheel-dir wheels
pip install ./wheels/google_fhir_core-${FHIR_PY_VERSION}-py3-none-any.whl[bigquery,spark]

pip wheel ./google-fhir-r4 --wheel-dir wheels --no-deps
pip install ./wheels/google_fhir_r4-${FHIR_PY_VERSION}-py3-none-any.whl

pip wheel ./google-fhir-views --wheel-dir wheels --no-deps
pip install ./wheels/google_fhir_views-${FHIR_PY_VERSION}-py3-none-any.whl[r4,bigquery,spark]

pytest
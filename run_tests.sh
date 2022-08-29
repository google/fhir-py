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

pip install ./google-fhir-core[bigquery]
pip install ./google-fhir-r4
pip install ./google-fhir-views[r4,bigquery]

pip install pytest

pytest
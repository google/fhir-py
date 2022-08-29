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
"""Defines structures expressing settings for SQL generation and validation."""

import dataclasses
from typing import Dict, Optional


@dataclasses.dataclass
class SqlValidationOptions:
  """Definition of optional settings for validating FHIR Path expressions.

  Attributes:
    num_code_systems_per_value_set: A mapping from value set URLs to the number
      of code systems within that value set. If provided, memberOf expressions
      will use these counts to detect calls against undefined value sets (i.e.
      those not included in the dictionary) and ambiguous calls against of code
      strings against value sets with multiple code systems.
  """
  num_code_systems_per_value_set: Optional[Dict[str, int]]

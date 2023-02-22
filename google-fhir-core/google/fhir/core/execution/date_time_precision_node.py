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

"""Support for the DateTimePrecisionNode."""
import enum


class DateTimePrecisionNode(str, enum.Enum):
  """Specifies the units of precision available for temporal operations such as durationbetween, sameas, sameorbefore, sameorafter, and datetimecomponentfrom."""

  YEAR = 'Year'
  MONTH = 'Month'
  WEEK = 'Week'
  DAY = 'Day'
  HOUR = 'Hour'
  MINUTE = 'Minute'
  SECOND = 'Second'
  MILLISECOND = 'Millisecond'

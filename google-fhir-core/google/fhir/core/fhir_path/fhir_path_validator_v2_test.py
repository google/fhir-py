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

from unittest import mock

from absl.testing import absltest
from google.fhir.core.fhir_path import fhir_path_validator_v2


class FhirProfileStandardSqlEncoderTest(absltest.TestCase):

  def test_error_message_for_exception_with_verbose_error_reporting_reports_stack_trace(
      self,
  ):
    validator = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        mock.MagicMock(),
        mock.MagicMock(),
        mock.MagicMock(),
        mock.MagicMock(verbose_error_reporting=True),
    )

    try:
      _raise_value_error()
    except ValueError as exc:
      result = validator._error_message_for_exception(exc)

    self.assertIn('_raise_value_error()', result)
    self.assertIn('_more_stack()', result)
    self.assertIn('oh no!', result)

  def test_error_message_for_exception_without_verbose_error_reporting_reports_stack_trace(
      self,
  ):
    validator = fhir_path_validator_v2.FhirProfileStandardSqlEncoder(
        mock.MagicMock(),
        mock.MagicMock(),
        mock.MagicMock(),
        mock.MagicMock(verbose_error_reporting=False),
    )

    try:
      _raise_value_error()
    except ValueError as exc:
      result = validator._error_message_for_exception(exc)

    self.assertEqual(result, 'oh no!')


def _raise_value_error():
  def _more_stack():
    raise ValueError('oh no!')

  _more_stack()


if __name__ == '__main__':
  absltest.main()

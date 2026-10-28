#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest import mock

import pytest
from atlassian import ConfluenceServer

from airflow.providers.atlassian.confluence.exceptions import ConfluenceSpaceNotFoundError
from airflow.providers.atlassian.confluence.sensors.confluence import (
    ConfluencePageSensor,
    ConfluenceSensor,
)

SENSORS = "airflow.providers.atlassian.confluence.sensors.confluence"


@mock.patch(f"{SENSORS}.ConfluenceHook", autospec=True)
class TestConfluenceSensor:
    @pytest.mark.parametrize("result", [True, False])
    def test_raw_response_decides_the_outcome(self, hook_cls, result):
        client = mock.create_autospec(ConfluenceServer, instance=True)
        client.page_exists.return_value = result
        hook_cls.return_value.get_conn.return_value = client
        sensor = ConfluenceSensor(
            task_id="wait",
            method_name="page_exists",
            method_params={"space_key": "SPACE", "title": "Report"},
            confluence_conn_id="my_conn",
        )

        assert sensor.poke(context={}) is result
        hook_cls.assert_called_once_with(confluence_conn_id="my_conn")
        client.page_exists.assert_called_once_with(space_key="SPACE", title="Report")

    def test_result_processor_receives_context_and_result(self, hook_cls):
        client = mock.create_autospec(ConfluenceServer, instance=True)
        client.page_exists.return_value = {"id": "1"}
        hook_cls.return_value.get_conn.return_value = client
        processor = mock.Mock(return_value=False)
        sensor = ConfluenceSensor(
            task_id="wait",
            method_name="page_exists",
            method_params={"space_key": "SPACE", "title": "Report"},
            result_processor=processor,
        )
        context = {"ds": "2026-01-01"}

        assert sensor.poke(context=context) is False
        processor.assert_called_once_with(context, {"id": "1"})
        client.page_exists.assert_called_once_with(space_key="SPACE", title="Report")


@mock.patch(f"{SENSORS}.ConfluenceHook", autospec=True)
class TestConfluencePageSensor:
    @pytest.mark.parametrize(("found", "expected"), [({"id": "10"}, True), (None, False)])
    @pytest.mark.parametrize("content_type", ["page", "blogpost"])
    def test_succeeds_when_the_content_exists(self, hook_cls, content_type, found, expected):
        hook_cls.return_value.find_content.return_value = found
        sensor = ConfluencePageSensor(
            task_id="wait",
            space="SPACE",
            title="Report",
            content_type=content_type,
            confluence_conn_id="my_conn",
        )

        assert sensor.poke(context={}) is expected
        hook_cls.assert_called_once_with(confluence_conn_id="my_conn")
        hook_cls.return_value.find_content.assert_called_once_with(
            "SPACE", "Report", content_type=content_type
        )

    def test_missing_space_means_not_yet(self, hook_cls):
        hook_cls.return_value.find_content.side_effect = ConfluenceSpaceNotFoundError("gone")
        sensor = ConfluencePageSensor(task_id="wait", space="SPACE", title="Report")

        assert sensor.poke(context={}) is False

    def test_space_and_title_are_templated(self, hook_cls):
        sensor = ConfluencePageSensor(task_id="wait", space="{{ space }}", title="Report {{ ds }}")

        sensor.render_template_fields({"space": "DOCS", "ds": "2026-01-01"})

        assert (sensor.space, sensor.title) == ("DOCS", "Report 2026-01-01")

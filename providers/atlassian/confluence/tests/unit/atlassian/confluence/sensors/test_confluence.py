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

from airflow.models import Connection
from airflow.providers.atlassian.confluence.sensors.confluence import ConfluencePageSensor

from tests_common.test_utils.compat import connection_as_json


@pytest.fixture(autouse=True)
def confluence_connection(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_CONFLUENCE_DEFAULT",
        connection_as_json(
            Connection(
                conn_id="confluence_default",
                conn_type="confluence",
                host="https://localhost/confluence/",
                login="user",
                password="password",
            )
        ),
    )


@pytest.fixture
def mocked_confluence_client():
    with mock.patch("airflow.providers.atlassian.confluence.hooks.confluence.Confluence", autospec=True) as m:
        client = mock.Mock(name="confluence_client")
        m.return_value = client
        yield client


class TestConfluencePageSensor:
    def test_raw_response_decides_the_outcome(self, mocked_confluence_client):
        mocked_confluence_client.page_exists.return_value = True
        sensor = ConfluencePageSensor(
            task_id="wait",
            method_name="page_exists",
            method_params={"space": "SPACE", "title": "Report"},
        )

        assert sensor.poke(context={}) is True
        mocked_confluence_client.page_exists.assert_called_once_with(space="SPACE", title="Report")

    def test_missing_page_keeps_poking(self, mocked_confluence_client):
        mocked_confluence_client.page_exists.return_value = False
        sensor = ConfluencePageSensor(
            task_id="wait", method_name="page_exists", method_params={"space": "S", "title": "T"}
        )

        assert sensor.poke(context={}) is False

    def test_method_params_default_to_empty(self, mocked_confluence_client):
        sensor = ConfluencePageSensor(task_id="wait", method_name="get_all_spaces")

        sensor.poke(context={})

        mocked_confluence_client.get_all_spaces.assert_called_once_with()

    def test_result_processor_decides_the_outcome(self, mocked_confluence_client):
        mocked_confluence_client.get_page_by_id.return_value = {"version": {"number": 4}}
        sensor = ConfluencePageSensor(
            task_id="wait",
            method_name="get_page_by_id",
            method_params={"page_id": "123"},
            result_processor=lambda result: result["version"]["number"] >= 3,
        )

        assert sensor.poke(context={}) is True

    def test_method_params_are_templated(self):
        assert "method_params" in ConfluencePageSensor.template_fields

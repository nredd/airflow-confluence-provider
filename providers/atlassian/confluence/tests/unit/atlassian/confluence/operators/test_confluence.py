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
from airflow.providers.atlassian.confluence.operators.confluence import (
    ConfluenceBlogPostOperator,
    ConfluenceOperator,
    ConfluencePageOperator,
)

from tests_common.test_utils.compat import connection_as_json

PAGE = {"id": "123", "title": "Report"}


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


class TestConfluenceOperator:
    def test_calls_the_named_method_on_the_client(self, mocked_confluence_client):
        mocked_confluence_client.get_page_by_id.return_value = PAGE
        operator = ConfluenceOperator(
            task_id="get_page",
            confluence_method="get_page_by_id",
            confluence_method_args={"page_id": "123"},
        )

        assert operator.execute(context={}) == PAGE
        mocked_confluence_client.get_page_by_id.assert_called_once_with(page_id="123")

    def test_method_args_default_to_empty(self, mocked_confluence_client):
        operator = ConfluenceOperator(task_id="ping", confluence_method="get_all_spaces")

        operator.execute(context={})

        mocked_confluence_client.get_all_spaces.assert_called_once_with()

    def test_result_processor_replaces_the_return_value(self, mocked_confluence_client):
        mocked_confluence_client.get_page_by_id.return_value = PAGE
        context: dict = {}
        operator = ConfluenceOperator(
            task_id="get_title",
            confluence_method="get_page_by_id",
            confluence_method_args={"page_id": "123"},
            result_processor=lambda ctx, result: (ctx is context, result["title"]),
        )

        assert operator.execute(context=context) == (True, "Report")

    def test_resource_method_supplies_the_object_to_call(self, mocked_confluence_client):
        resource = mock.Mock()
        resource.custom_call.return_value = "ok"
        operator = ConfluenceOperator(
            task_id="custom",
            confluence_method="custom_call",
            get_confluence_resource_method=lambda **_: resource,
        )

        assert operator.execute(context={}) == "ok"
        mocked_confluence_client.custom_call.assert_not_called()

    def test_a_nested_operator_may_supply_the_resource(self, mocked_confluence_client):
        resource = mock.Mock()
        resource.custom_call.return_value = "ok"
        inner = ConfluenceOperator(task_id="inner", confluence_method="unused")
        inner.execute = mock.Mock(return_value=resource)
        operator = ConfluenceOperator(
            task_id="outer",
            confluence_method="custom_call",
            get_confluence_resource_method=inner,
        )

        assert operator.execute(context={}) == "ok"
        inner.execute.assert_called_once_with()

    def test_method_args_are_templated(self):
        assert {"confluence_method_args", "confluence_conn_id"} <= set(ConfluenceOperator.template_fields)


class TestConfluencePageOperator:
    def test_upserts_a_top_level_page(self, mocked_confluence_client):
        mocked_confluence_client.get_page_by_title.return_value = None
        mocked_confluence_client.create_page.return_value = PAGE
        operator = ConfluencePageOperator(task_id="publish", space="SPACE", title="Report", body="<p>hi</p>")

        assert operator.execute(context={}) == PAGE
        assert mocked_confluence_client.create_page.call_args.kwargs["space"] == "SPACE"

    def test_upserts_under_a_parent_id(self, mocked_confluence_client):
        mocked_confluence_client.update_or_create.return_value = PAGE
        operator = ConfluencePageOperator(
            task_id="publish",
            space="SPACE",
            title="Report",
            body="<p>hi</p>",
            parent_id="42",
            version_comment="nightly",
        )

        assert operator.execute(context={}) == PAGE
        mocked_confluence_client.update_or_create.assert_called_once_with(
            parent_id="42",
            title="Report",
            body="<p>hi</p>",
            representation="storage",
            minor_edit=False,
            version_comment="nightly",
        )

    def test_parent_title_is_resolved_to_an_id(self, mocked_confluence_client):
        mocked_confluence_client.get_page_id.return_value = "99"
        mocked_confluence_client.update_or_create.return_value = PAGE
        operator = ConfluencePageOperator(
            task_id="publish",
            space="SPACE",
            title="Report",
            body="<p>hi</p>",
            parent_title="Parent",
        )

        operator.execute(context={})

        mocked_confluence_client.get_page_id.assert_called_once_with(space="SPACE", title="Parent")
        assert mocked_confluence_client.update_or_create.call_args.kwargs["parent_id"] == "99"

    def test_unresolvable_parent_title_raises(self, mocked_confluence_client):
        mocked_confluence_client.get_page_id.return_value = None
        operator = ConfluencePageOperator(
            task_id="publish",
            space="SPACE",
            title="Report",
            body="<p>hi</p>",
            parent_title="Missing",
        )

        with pytest.raises(ValueError, match="'Missing' was not found in space 'SPACE'"):
            operator.execute(context={})

    def test_parent_id_and_parent_title_are_mutually_exclusive(self, mocked_confluence_client):
        operator = ConfluencePageOperator(
            task_id="publish",
            space="SPACE",
            title="Report",
            body="<p>hi</p>",
            parent_id="42",
            parent_title="Parent",
        )

        with pytest.raises(ValueError, match="at most one of `parent_id` and `parent_title`"):
            operator.execute(context={})

    def test_body_and_title_are_templated(self):
        assert {"title", "body", "parent_id", "parent_title", "confluence_conn_id"} <= set(
            ConfluencePageOperator.template_fields
        )


class TestConfluenceBlogPostOperator:
    def test_creates_a_blog_post_when_absent(self, mocked_confluence_client):
        mocked_confluence_client.get_page_by_title.return_value = None
        mocked_confluence_client.create_page.return_value = PAGE
        operator = ConfluenceBlogPostOperator(
            task_id="publish", space="SPACE", title="News", body="<p>hi</p>"
        )

        assert operator.execute(context={}) == PAGE
        assert mocked_confluence_client.create_page.call_args.kwargs["type"] == "blogpost"

    def test_updates_a_blog_post_when_present(self, mocked_confluence_client):
        mocked_confluence_client.get_page_by_title.return_value = {"id": "9"}
        mocked_confluence_client.update_page.return_value = PAGE
        operator = ConfluenceBlogPostOperator(
            task_id="publish", space="SPACE", title="News", body="<p>hi</p>"
        )

        assert operator.execute(context={}) == PAGE
        assert mocked_confluence_client.update_page.call_args.kwargs["page_id"] == "9"
        mocked_confluence_client.create_page.assert_not_called()

    def test_body_and_title_are_templated(self):
        assert {"title", "body", "confluence_conn_id"} <= set(ConfluenceBlogPostOperator.template_fields)

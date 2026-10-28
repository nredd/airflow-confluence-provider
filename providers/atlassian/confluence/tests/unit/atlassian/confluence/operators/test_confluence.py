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

from airflow.providers.atlassian.confluence.hooks.confluence import ConfluenceContent, ConfluenceHook
from airflow.providers.atlassian.confluence.operators.confluence import (
    ConfluenceBlogPostOperator,
    ConfluenceOperator,
    ConfluencePageOperator,
)

OPERATORS = "airflow.providers.atlassian.confluence.operators.confluence"

CONTENT = ConfluenceContent(
    id="10",
    type="page",
    title="Report",
    space="SPACE",
    version=1,
    url="https://example.atlassian.net/wiki/spaces/SPACE/pages/10",
    action="created",
)


@mock.patch(f"{OPERATORS}.ConfluenceHook", autospec=True)
class TestConfluenceOperator:
    def test_calls_the_named_method_on_the_client(self, hook_cls):
        client = mock.create_autospec(ConfluenceServer, instance=True)
        client.get_page_by_id.return_value = {"id": "123"}
        hook_cls.return_value.get_conn.return_value = client
        operator = ConfluenceOperator(
            task_id="get_page",
            confluence_method="get_page_by_id",
            confluence_method_args={"page_id": "123"},
            confluence_conn_id="my_conn",
        )

        assert operator.execute(context={}) == {"id": "123"}
        hook_cls.assert_called_once_with(confluence_conn_id="my_conn")
        client.get_page_by_id.assert_called_once_with(page_id="123")

    def test_method_args_default_to_empty(self, hook_cls):
        client = mock.create_autospec(ConfluenceServer, instance=True)
        hook_cls.return_value.get_conn.return_value = client

        ConfluenceOperator(task_id="ping", confluence_method="get_all_spaces").execute(context={})

        client.get_all_spaces.assert_called_once_with()

    def test_result_processor_receives_context_and_result(self, hook_cls):
        client = mock.create_autospec(ConfluenceServer, instance=True)
        client.get_page_by_id.return_value = {"id": "123"}
        hook_cls.return_value.get_conn.return_value = client
        processor = mock.Mock(return_value="processed")
        operator = ConfluenceOperator(
            task_id="get_page",
            confluence_method="get_page_by_id",
            confluence_method_args={"page_id": "123"},
            result_processor=processor,
        )
        context = {"ds": "2026-01-01"}

        assert operator.execute(context=context) == "processed"
        processor.assert_called_once_with(context, {"id": "123"})


@mock.patch(f"{OPERATORS}.ConfluenceHook", autospec=True)
class TestConfluencePageOperator:
    def test_publishes_a_page_and_returns_the_summary(self, hook_cls):
        hook_cls.return_value.publish.return_value = CONTENT
        operator = ConfluencePageOperator(
            task_id="publish",
            space="SPACE",
            title="Report",
            body="<p>hi</p>",
            parent_title="Parent",
            if_exists="skip",
            version_comment="Nightly",
            confluence_conn_id="my_conn",
        )

        assert operator.execute(context={}) == CONTENT
        hook_cls.assert_called_once_with(confluence_conn_id="my_conn")
        hook_cls.return_value.publish.assert_called_once_with(
            "SPACE",
            "Report",
            "<p>hi</p>",
            content_type="page",
            parent_id=None,
            parent_title="Parent",
            if_exists="skip",
            version_comment="Nightly",
        )

    def test_content_fields_are_templated(self, hook_cls):
        operator = ConfluencePageOperator(
            task_id="publish",
            space="{{ space }}",
            title="Report {{ ds }}",
            body="<p>{{ ds }}</p>",
            parent_title="Parent {{ ds }}",
        )

        operator.render_template_fields({"space": "DOCS", "ds": "2026-01-01"})

        assert (operator.space, operator.title, operator.body, operator.parent_title) == (
            "DOCS",
            "Report 2026-01-01",
            "<p>2026-01-01</p>",
            "Parent 2026-01-01",
        )

    def test_if_exists_defaults_to_update(self, hook_cls):
        hook_cls.return_value.publish.return_value = CONTENT

        ConfluencePageOperator(task_id="publish", space="SPACE", title="Report", body="").execute(context={})

        assert hook_cls.return_value.publish.call_args.kwargs["if_exists"] == "update"

    def test_invalid_if_exists_is_rejected_at_construction(self, hook_cls):
        with pytest.raises(ValueError, match="`if_exists` must be one of"):
            ConfluencePageOperator(task_id="publish", space="S", title="T", body="", if_exists="replace")

        hook_cls.assert_not_called()


@mock.patch(f"{OPERATORS}.ConfluenceHook", autospec=True)
class TestConfluenceBlogPostOperator:
    def test_publishes_a_blog_post_and_returns_the_summary(self, hook_cls):
        post = ConfluenceContent(**{**CONTENT, "type": "blogpost"})
        hook_cls.return_value.publish.return_value = post
        operator = ConfluenceBlogPostOperator(
            task_id="publish",
            space="SPACE",
            title="Notes",
            body="<p>hi</p>",
            if_exists="fail",
            version_comment="Nightly",
        )

        assert operator.execute(context={}) == post
        hook_cls.assert_called_once_with(confluence_conn_id=ConfluenceHook.default_conn_name)
        hook_cls.return_value.publish.assert_called_once_with(
            "SPACE",
            "Notes",
            "<p>hi</p>",
            content_type="blogpost",
            if_exists="fail",
            version_comment="Nightly",
        )

    def test_content_fields_are_templated(self, hook_cls):
        operator = ConfluenceBlogPostOperator(
            task_id="publish", space="{{ space }}", title="Notes {{ ds }}", body="<p>{{ ds }}</p>"
        )

        operator.render_template_fields({"space": "DOCS", "ds": "2026-01-01"})

        assert (operator.space, operator.title, operator.body) == (
            "DOCS",
            "Notes 2026-01-01",
            "<p>2026-01-01</p>",
        )

    def test_invalid_if_exists_is_rejected_at_construction(self, hook_cls):
        with pytest.raises(ValueError, match="`if_exists` must be one of"):
            ConfluenceBlogPostOperator(task_id="publish", space="S", title="T", body="", if_exists="replace")

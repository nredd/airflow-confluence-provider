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
from airflow.providers.atlassian.confluence.exceptions import ConfluenceConnectionError
from airflow.providers.atlassian.confluence.hooks.confluence import ConfluenceHook

from tests_common.test_utils.compat import connection_as_json

CONN_ID = "confluence_default"


def set_connection(monkeypatch, **kwargs):
    """Register ``confluence_default`` in the environment, overriding the defaults given."""
    fields: dict = {
        "conn_id": CONN_ID,
        "conn_type": "confluence",
        "host": "https://localhost/confluence/",
        "login": "user",
        "password": "password",
    }
    fields.update(kwargs)
    monkeypatch.setenv(f"AIRFLOW_CONN_{CONN_ID}".upper(), connection_as_json(Connection(**fields)))


@pytest.fixture
def mocked_confluence_client():
    with mock.patch("airflow.providers.atlassian.confluence.hooks.confluence.Confluence", autospec=True) as m:
        m.return_value = mock.Mock(name="confluence_client")
        yield m


@pytest.fixture
def hook(monkeypatch, mocked_confluence_client):
    set_connection(monkeypatch)
    return ConfluenceHook()


class TestConfluenceHookConnection:
    def test_client_is_not_created_until_first_use(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch)

        confluence_hook = ConfluenceHook()

        assert confluence_hook.client is None
        mocked_confluence_client.assert_not_called()

    def test_basic_auth_is_used_without_a_token(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch, extra='{"verify": false}')

        ConfluenceHook(proxies={"https": "https://proxy.example"}).get_conn()

        mocked_confluence_client.assert_called_once_with(
            url="https://localhost/confluence/",
            verify_ssl=False,
            proxies={"https": "https://proxy.example"},
            api_version="latest",
            api_root="rest/api",
            username="user",
            password="password",
        )

    def test_token_takes_precedence_over_login_and_password(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch, extra='{"token": "s3cr3t", "cloud": true}')

        ConfluenceHook().get_conn()

        _, kwargs = mocked_confluence_client.call_args
        assert kwargs["token"] == "s3cr3t"
        assert kwargs["cloud"] is True
        assert "username" not in kwargs
        assert "password" not in kwargs

    def test_verify_defaults_to_true_without_extras(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch)

        ConfluenceHook().get_conn()

        _, kwargs = mocked_confluence_client.call_args
        assert kwargs["verify_ssl"] is True

    def test_cloud_is_omitted_when_unset_so_the_sdk_can_auto_detect(
        self, monkeypatch, mocked_confluence_client
    ):
        set_connection(monkeypatch)

        ConfluenceHook().get_conn()

        _, kwargs = mocked_confluence_client.call_args
        assert "cloud" not in kwargs

    def test_cloud_false_is_still_forwarded_when_set_explicitly(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch, extra='{"cloud": false}')

        ConfluenceHook().get_conn()

        _, kwargs = mocked_confluence_client.call_args
        assert kwargs["cloud"] is False

    def test_client_is_created_once_and_reused(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch)
        confluence_hook = ConfluenceHook()

        first = confluence_hook.get_conn()
        second = confluence_hook.get_conn()

        assert first is second
        mocked_confluence_client.assert_called_once()

    def test_missing_conn_id_raises(self):
        with pytest.raises(ConfluenceConnectionError, match="confluence_conn_id is required"):
            ConfluenceHook(confluence_conn_id="").get_conn()

    def test_missing_host_raises(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch, host=None)

        with pytest.raises(ConfluenceConnectionError, match="host is required"):
            ConfluenceHook().get_conn()

    @pytest.mark.parametrize(
        ("login", "password"),
        [
            pytest.param(None, "password", id="no-login"),
            pytest.param("user", None, id="no-password"),
            pytest.param(None, None, id="neither"),
        ],
    )
    def test_missing_credentials_without_a_token_raises(
        self, monkeypatch, mocked_confluence_client, login, password
    ):
        set_connection(monkeypatch, login=login, password=password)

        with pytest.raises(ConfluenceConnectionError, match="login and password are required"):
            ConfluenceHook().get_conn()

    def test_token_alone_is_enough(self, monkeypatch, mocked_confluence_client):
        set_connection(monkeypatch, login=None, password=None, extra='{"token": "s3cr3t"}')

        ConfluenceHook().get_conn()

        mocked_confluence_client.assert_called_once()

    @pytest.mark.parametrize(
        ("host", "schema", "port", "expected"),
        [
            pytest.param(
                "confluence.example.com", None, None, "https://confluence.example.com", id="bare-host"
            ),
            pytest.param(
                "http://confluence.example.com",
                None,
                None,
                "http://confluence.example.com",
                id="scheme-in-host",
            ),
            pytest.param(
                "confluence.example.com", "http", None, "http://confluence.example.com", id="schema-field"
            ),
            pytest.param(
                "confluence.example.com",
                None,
                8443,
                "https://confluence.example.com:8443",
                id="port-injected",
            ),
            pytest.param(
                "https://confluence.example.com:9000",
                None,
                8443,
                "https://confluence.example.com:9000",
                id="port-in-host-wins",
            ),
            pytest.param(
                "https://user@confluence.example.com",
                None,
                8443,
                "https://user@confluence.example.com:8443",
                id="userinfo-not-mistaken-for-port",
            ),
            pytest.param(
                "confluence.example.com/wiki",
                None,
                8443,
                "https://confluence.example.com:8443/wiki",
                id="path-preserved",
            ),
        ],
    )
    def test_connection_url_is_built_from_host_schema_and_port(
        self, monkeypatch, mocked_confluence_client, host, schema, port, expected
    ):
        set_connection(monkeypatch, host=host, schema=schema, port=port)

        ConfluenceHook().get_conn()

        _, kwargs = mocked_confluence_client.call_args
        assert kwargs["url"] == expected


class TestConfluenceHookUiFields:
    def test_connection_form_widgets_match_the_documented_extras(self):
        pytest.importorskip("flask_babel")

        assert set(ConfluenceHook.get_connection_form_widgets()) == {"verify", "token", "cloud"}

    def test_only_schema_is_hidden(self):
        assert ConfluenceHook.get_ui_field_behaviour() == {"hidden_fields": ["schema"]}


class TestConfluenceHookPageLookup:
    def test_find_child_page_by_title_returns_the_matching_id(self, hook):
        client = hook.get_conn()
        client.get_page_child_by_type.return_value = [
            {"id": "1", "title": "Other"},
            {"id": "2", "title": "Target"},
        ]

        assert hook.find_child_page_by_title("100", "Target") == "2"

    def test_find_child_page_by_title_never_caps_pagination(self, hook):
        client = hook.get_conn()
        client.get_page_child_by_type.return_value = []

        hook.find_child_page_by_title("100", "Target")

        # Omitting start/limit makes the SDK return a fully paginated generator.
        client.get_page_child_by_type.assert_called_once_with("100", type="page")

    def test_find_child_page_by_title_returns_none_when_absent(self, hook):
        client = hook.get_conn()
        client.get_page_child_by_type.return_value = [{"id": "1", "title": "Other"}]

        assert hook.find_child_page_by_title("100", "Target") is None

    def test_find_child_page_by_title_handles_no_children(self, hook):
        client = hook.get_conn()
        client.get_page_child_by_type.return_value = None

        assert hook.find_child_page_by_title("100", "Target") is None

    def test_find_child_page_by_title_honours_page_type(self, hook):
        client = hook.get_conn()
        client.get_page_child_by_type.return_value = []

        hook.find_child_page_by_title("100", "Target", page_type="blogpost")

        client.get_page_child_by_type.assert_called_once_with("100", type="blogpost")


class TestConfluenceHookGetOrCreatePage:
    def test_delegates_to_the_sdk_upsert_when_a_parent_is_given(self, hook):
        client = hook.get_conn()

        hook.get_or_create_page(title="Report", body="<p>hi</p>", parent_id="42")

        client.update_or_create.assert_called_once_with(
            parent_id="42",
            title="Report",
            body="<p>hi</p>",
            representation="storage",
            minor_edit=False,
            version_comment=None,
        )

    def test_creates_a_top_level_page_when_the_title_is_free(self, hook):
        client = hook.get_conn()
        client.get_page_by_title.return_value = None

        hook.get_or_create_page(space="SPACE", title="Report", body="<p>hi</p>")

        client.create_page.assert_called_once_with(
            space="SPACE",
            title="Report",
            body="<p>hi</p>",
            parent_id=None,
            representation="storage",
            editor=None,
            full_width=False,
            status="current",
        )
        client.update_page.assert_not_called()

    def test_updates_an_existing_top_level_page(self, hook):
        client = hook.get_conn()
        client.get_page_by_title.return_value = {"id": "7"}

        hook.get_or_create_page(space="SPACE", title="Report", body="<p>hi</p>", version_comment="nightly")

        client.update_page.assert_called_once_with(
            page_id="7",
            title="Report",
            body="<p>hi</p>",
            parent_id=None,
            representation="storage",
            minor_edit=False,
            version_comment="nightly",
            always_update=False,
            full_width=False,
        )
        client.create_page.assert_not_called()

    def test_top_level_page_requires_a_space(self, hook):
        with pytest.raises(ValueError, match="`space` is required"):
            hook.get_or_create_page(title="Report", body="<p>hi</p>")


class TestConfluenceHookBlogPosts:
    def test_create_blog_post_sets_the_blogpost_type(self, hook):
        client = hook.get_conn()

        hook.create_blog_post(space="SPACE", title="News", body="<p>hi</p>")

        client.create_page.assert_called_once_with(
            space="SPACE",
            title="News",
            body="<p>hi</p>",
            type="blogpost",
            representation="storage",
            status="current",
        )

    def test_update_blog_post_sets_the_blogpost_type(self, hook):
        client = hook.get_conn()

        hook.update_blog_post(page_id="7", title="News", body="<p>hi</p>")

        client.update_page.assert_called_once_with(
            page_id="7",
            title="News",
            body="<p>hi</p>",
            type="blogpost",
            representation="storage",
            minor_edit=False,
            version_comment=None,
        )

    def test_get_blog_post_by_title_filters_by_type(self, hook):
        client = hook.get_conn()

        hook.get_blog_post_by_title(space="SPACE", title="News")

        client.get_page_by_title.assert_called_once_with(
            space="SPACE", title="News", expand=None, type="blogpost"
        )

    def test_get_or_create_blog_post_creates_when_absent(self, hook):
        client = hook.get_conn()
        client.get_page_by_title.return_value = None

        hook.get_or_create_blog_post(space="SPACE", title="News", body="<p>hi</p>")

        client.create_page.assert_called_once()
        client.update_page.assert_not_called()

    def test_get_or_create_blog_post_updates_when_present(self, hook):
        client = hook.get_conn()
        client.get_page_by_title.return_value = {"id": "9"}

        hook.get_or_create_blog_post(space="SPACE", title="News", body="<p>hi</p>")

        assert client.update_page.call_args.kwargs["page_id"] == "9"
        client.create_page.assert_not_called()


class TestConfluenceHookV2Content:
    @pytest.mark.parametrize(
        ("method", "path"),
        [
            pytest.param("create_folder", "/api/v2/folders", id="folder"),
            pytest.param("create_database", "/api/v2/databases", id="database"),
        ],
    )
    def test_create_posts_the_v2_payload(self, hook, method, path):
        client = hook.get_conn()

        getattr(hook, method)(space_id="99", title="Docs", parent_id="42")

        client.post.assert_called_once_with(path, data={"spaceId": "99", "title": "Docs", "parentId": "42"})

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            pytest.param("create_folder", "/api/v2/folders", id="folder"),
            pytest.param("create_database", "/api/v2/databases", id="database"),
        ],
    )
    def test_create_omits_the_parent_when_not_given(self, hook, method, path):
        client = hook.get_conn()

        getattr(hook, method)(space_id="99", title="Docs")

        client.post.assert_called_once_with(path, data={"spaceId": "99", "title": "Docs"})

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            pytest.param("get_folder", "/api/v2/folders/5", id="folder"),
            pytest.param("get_database", "/api/v2/databases/5", id="database"),
        ],
    )
    def test_get_uses_the_v2_path(self, hook, method, path):
        client = hook.get_conn()

        getattr(hook, method)("5")

        client.get.assert_called_once_with(path)

    @pytest.mark.parametrize(
        ("method", "path"),
        [
            pytest.param("delete_folder", "/api/v2/folders/5", id="folder"),
            pytest.param("delete_database", "/api/v2/databases/5", id="database"),
        ],
    )
    def test_delete_uses_the_v2_path(self, hook, method, path):
        client = hook.get_conn()

        getattr(hook, method)("5")

        client.delete.assert_called_once_with(path)

    def test_create_whiteboard_maps_to_the_sdk_camel_case_kwargs(self, hook):
        client = hook.get_conn()

        hook.create_whiteboard(space_id="99", title="Plan", parent_id="42")

        client.create_whiteboard.assert_called_once_with(spaceId="99", title="Plan", parentId="42")

    def test_get_whiteboard_delegates_to_the_sdk(self, hook):
        client = hook.get_conn()

        hook.get_whiteboard("5")

        client.get_whiteboard.assert_called_once_with("5")

    def test_delete_whiteboard_delegates_to_the_sdk(self, hook):
        client = hook.get_conn()

        hook.delete_whiteboard("5")

        client.delete_whiteboard.assert_called_once_with("5")


class TestConfluenceHookPassthroughs:
    """Each wrapper must forward its arguments to the identically named SDK method."""

    @pytest.mark.parametrize(
        ("hook_method", "sdk_method", "kwargs", "expected"),
        [
            pytest.param(
                "create_page",
                "create_page",
                {"space": "S", "title": "T", "body": "B"},
                {
                    "space": "S",
                    "title": "T",
                    "body": "B",
                    "parent_id": None,
                    "representation": "storage",
                    "editor": None,
                    "full_width": False,
                    "status": "current",
                },
                id="create_page",
            ),
            pytest.param(
                "update_page",
                "update_page",
                {"page_id": "1", "title": "T", "body": "B"},
                {
                    "page_id": "1",
                    "title": "T",
                    "body": "B",
                    "parent_id": None,
                    "representation": "storage",
                    "minor_edit": False,
                    "version_comment": None,
                    "always_update": False,
                    "full_width": False,
                },
                id="update_page",
            ),
            pytest.param(
                "append_page",
                "append_page",
                {"page_id": "1", "title": "T", "append_body": "B"},
                {
                    "page_id": "1",
                    "title": "T",
                    "append_body": "B",
                    "parent_id": None,
                    "representation": "storage",
                    "minor_edit": False,
                },
                id="append_page",
            ),
            pytest.param(
                "get_page_by_id",
                "get_page_by_id",
                {"page_id": "1"},
                {"page_id": "1", "expand": None, "status": None, "version": None},
                id="get_page_by_id",
            ),
            pytest.param(
                "get_page_by_title",
                "get_page_by_title",
                {"space": "S", "title": "T"},
                {"space": "S", "title": "T", "start": 0, "limit": 1, "expand": None, "type": "page"},
                id="get_page_by_title",
            ),
            pytest.param(
                "get_all_pages_from_space",
                "get_all_pages_from_space",
                {"space": "S"},
                {
                    "space": "S",
                    "start": 0,
                    "limit": 50,
                    "status": None,
                    "expand": None,
                    "content_type": "page",
                },
                id="get_all_pages_from_space",
            ),
            pytest.param(
                "remove_page",
                "remove_page",
                {"page_id": "1"},
                {"page_id": "1", "status": None, "recursive": False},
                id="remove_page",
            ),
            pytest.param(
                "move_page",
                "move_page",
                {"space_key": "S", "page_id": "1"},
                {
                    "space_key": "S",
                    "page_id": "1",
                    "target_id": None,
                    "target_title": None,
                    "position": "append",
                },
                id="move_page",
            ),
            pytest.param(
                "search_cql",
                "cql",
                {"cql": 'type=page and space="S"'},
                {
                    "cql": 'type=page and space="S"',
                    "start": 0,
                    "limit": None,
                    "expand": None,
                    "include_archived_spaces": None,
                    "excerpt": None,
                },
                id="search_cql",
            ),
            pytest.param(
                "get_page_labels",
                "get_page_labels",
                {"page_id": "1"},
                {"page_id": "1", "prefix": None, "start": None, "limit": None},
                id="get_page_labels",
            ),
            pytest.param(
                "set_page_label",
                "set_page_label",
                {"page_id": "1", "label": "L"},
                {"page_id": "1", "label": "L"},
                id="set_page_label",
            ),
            pytest.param(
                "remove_page_label",
                "remove_page_label",
                {"page_id": "1", "label": "L"},
                {"page_id": "1", "label": "L"},
                id="remove_page_label",
            ),
            pytest.param(
                "add_comment",
                "add_comment",
                {"page_id": "1", "text": "hi"},
                {"page_id": "1", "text": "hi"},
                id="add_comment",
            ),
            pytest.param(
                "get_page_comments",
                "get_page_comments",
                {"content_id": "1"},
                {
                    "content_id": "1",
                    "expand": None,
                    "parent_version": None,
                    "start": 0,
                    "limit": 25,
                    "location": None,
                    "depth": None,
                },
                id="get_page_comments",
            ),
            pytest.param(
                "attach_file",
                "attach_file",
                {"filename": "f.txt", "page_id": "1"},
                {
                    "filename": "f.txt",
                    "name": None,
                    "content_type": None,
                    "page_id": "1",
                    "title": None,
                    "space": None,
                    "comment": None,
                },
                id="attach_file",
            ),
            pytest.param(
                "attach_content",
                "attach_content",
                {"content": b"data", "name": "f.txt", "page_id": "1"},
                {
                    "content": b"data",
                    "name": "f.txt",
                    "content_type": "application/binary",
                    "page_id": "1",
                    "title": None,
                    "space": None,
                    "comment": None,
                },
                id="attach_content",
            ),
            pytest.param(
                "download_attachments_from_page",
                "download_attachments_from_page",
                {"page_id": "1"},
                {
                    "page_id": "1",
                    "path": None,
                    "start": 0,
                    "limit": 50,
                    "filename": None,
                    "to_memory": False,
                },
                id="download_attachments_from_page",
            ),
            pytest.param(
                "delete_attachment",
                "delete_attachment",
                {"page_id": "1", "filename": "f.txt"},
                {"page_id": "1", "filename": "f.txt", "version": None},
                id="delete_attachment",
            ),
            pytest.param(
                "get_page_property",
                "get_page_property",
                {"page_id": "1", "page_property_key": "k"},
                {"page_id": "1", "page_property_key": "k"},
                id="get_page_property",
            ),
            pytest.param(
                "set_page_property",
                "set_page_property",
                {"page_id": "1", "data": {"key": "k"}},
                {"page_id": "1", "data": {"key": "k"}},
                id="set_page_property",
            ),
            pytest.param(
                "update_page_property",
                "update_page_property",
                {"page_id": "1", "data": {"key": "k"}},
                {"page_id": "1", "data": {"key": "k"}},
                id="update_page_property",
            ),
            pytest.param(
                "delete_page_property",
                "delete_page_property",
                {"page_id": "1", "page_property": "k"},
                {"page_id": "1", "page_property": "k"},
                id="delete_page_property",
            ),
            pytest.param(
                "get_all_restrictions_for_content",
                "get_all_restrictions_for_content",
                {"content_id": "1"},
                {"content_id": "1"},
                id="get_all_restrictions_for_content",
            ),
        ],
    )
    def test_wrapper_forwards_to_the_sdk(self, hook, hook_method, sdk_method, kwargs, expected):
        client = hook.get_conn()

        result = getattr(hook, hook_method)(**kwargs)

        getattr(client, sdk_method).assert_called_once_with(**expected)
        assert result is getattr(client, sdk_method).return_value

    def test_export_page_is_positional(self, hook):
        client = hook.get_conn()

        result = hook.export_page("1")

        client.export_page.assert_called_once_with("1")
        assert result is client.export_page.return_value

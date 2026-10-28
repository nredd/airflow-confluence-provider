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

import sys
import warnings
from dataclasses import dataclass
from typing import Any
from unittest import mock

import pytest
from atlassian import ConfluenceServer, ConfluenceV2

from airflow.models import Connection
from airflow.providers.atlassian.confluence.exceptions import (
    ConfluenceConnectionError,
    ConfluenceContentExistsError,
    ConfluenceContentNotFoundError,
    ConfluenceSpaceNotFoundError,
)
from airflow.providers.atlassian.confluence.get_provider_info import get_provider_info
from airflow.providers.atlassian.confluence.hooks.confluence import ConfluenceHook

HOOK_MODULE = "airflow.providers.atlassian.confluence.hooks.confluence"
CONN_ID = "confluence_default"
DC_URL = "https://confluence.example.com/confluence"
CLOUD_URL = "https://example.atlassian.net/wiki"
BODY = "<p>hello</p>"


@dataclass
class Sdk:
    """The two SDK client constructors, patched in the hook module, with spec'd instances."""

    server_ctor: mock.Mock
    cloud_ctor: mock.Mock

    @property
    def server(self) -> mock.Mock:
        return self.server_ctor.return_value

    @property
    def cloud(self) -> mock.Mock:
        return self.cloud_ctor.return_value


@pytest.fixture
def sdk(monkeypatch) -> Sdk:
    server_ctor = mock.create_autospec(ConfluenceServer)
    cloud_ctor = mock.create_autospec(ConfluenceV2)
    monkeypatch.setattr(f"{HOOK_MODULE}.ConfluenceServer", server_ctor)
    monkeypatch.setattr(f"{HOOK_MODULE}.ConfluenceV2", cloud_ctor)
    return Sdk(server_ctor, cloud_ctor)


@pytest.fixture
def set_connection(create_connection_without_db):
    def _set_connection(**kwargs: Any) -> None:
        fields: dict[str, Any] = {
            "conn_id": CONN_ID,
            "conn_type": "confluence",
            "host": DC_URL,
            "login": "user",
            "password": "password",
        }
        fields.update(kwargs)
        create_connection_without_db(Connection(**fields))

    return _set_connection


class TestConnection:
    def test_nothing_is_created_on_init(self, sdk, set_connection):
        set_connection()

        ConfluenceHook()

        sdk.server_ctor.assert_not_called()
        sdk.cloud_ctor.assert_not_called()

    def test_data_center_basic_auth(self, sdk, set_connection):
        set_connection(extra={"verify": False})
        proxies = {"https": "https://proxy.example"}

        client = ConfluenceHook(proxies=proxies).get_conn()

        assert client is sdk.server
        sdk.server_ctor.assert_called_once_with(
            url=DC_URL, username="user", password="password", verify_ssl=False, proxies=proxies
        )
        sdk.cloud_ctor.assert_not_called()

    def test_data_center_token_replaces_login_and_password(self, sdk, set_connection):
        set_connection(extra={"token": "s3cr3t"})

        ConfluenceHook().get_conn()

        sdk.server_ctor.assert_called_once_with(url=DC_URL, token="s3cr3t", verify_ssl=True, proxies=None)

    def test_cloud_uses_basic_auth_with_the_v2_client(self, sdk, set_connection):
        set_connection(host=CLOUD_URL, login="me@example.com", password="api-token")

        client = ConfluenceHook().get_conn()

        assert client is sdk.cloud
        sdk.cloud_ctor.assert_called_once_with(
            url=CLOUD_URL, username="me@example.com", password="api-token", verify_ssl=True, proxies=None
        )
        sdk.server_ctor.assert_not_called()

    def test_cloud_client_deprecation_warning_is_silenced(self, sdk, set_connection):
        set_connection(host=CLOUD_URL)
        sdk.cloud_ctor.side_effect = lambda **_: warnings.warn("legacy", DeprecationWarning, stacklevel=1)

        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            ConfluenceHook().get_conn()

        assert recorded == []

    def test_client_is_created_once(self, sdk, set_connection):
        set_connection()
        hook = ConfluenceHook()

        assert hook.get_conn() is hook.get_conn()
        sdk.server_ctor.assert_called_once()

    @pytest.mark.parametrize(
        ("host", "extra", "expected"),
        [
            ("https://example.atlassian.net/wiki", None, True),
            ("example.atlassian.net", None, True),
            ("https://example.jira.com/wiki", None, True),
            ("https://api.atlassian.com/ex/confluence/abc", None, True),
            ("https://confluence.example.com/confluence", None, False),
            ("https://evilatlassian.net/wiki", None, False),
            ("https://wiki.example.com", {"deployment": "cloud"}, True),
            ("https://example.atlassian.net", {"deployment": "data_center"}, False),
            ("https://example.atlassian.net/wiki", {"deployment": "auto"}, True),
            ("https://wiki.example.com", {"deployment": "auto"}, False),
        ],
    )
    def test_is_cloud(self, sdk, set_connection, host, extra, expected):
        set_connection(host=host, extra=extra)

        hook = ConfluenceHook()

        assert hook.is_cloud is expected
        sdk.server_ctor.assert_not_called()
        sdk.cloud_ctor.assert_not_called()

    @pytest.mark.parametrize(
        ("host", "port", "schema", "expected"),
        [
            ("confluence.example.com", None, None, "https://confluence.example.com"),
            ("http://confluence.example.com", None, None, "http://confluence.example.com"),
            ("confluence.example.com/confluence/", None, None, "https://confluence.example.com/confluence"),
            ("confluence.example.com", 8443, None, "https://confluence.example.com:8443"),
            ("http://confluence.example.com/wiki", 8090, None, "http://confluence.example.com:8090/wiki"),
            (
                "https://confluence.example.com:9443/wiki",
                8443,
                None,
                "https://confluence.example.com:9443/wiki",
            ),
            ("https://[::1]/wiki", 8443, None, "https://[::1]:8443/wiki"),
            ("https://[::1]:9443/wiki", 8443, None, "https://[::1]:9443/wiki"),
            ("[::1]", None, None, "https://[::1]"),
            ("confluence.example.com", None, "wiki", "https://confluence.example.com"),
        ],
    )
    def test_url_is_built_from_host_and_port_and_schema_is_ignored(
        self, sdk, set_connection, host, port, schema, expected
    ):
        set_connection(host=host, port=port, schema=schema)

        ConfluenceHook().get_conn()

        assert sdk.server_ctor.call_args.kwargs["url"] == expected

    @pytest.mark.parametrize(
        ("fields", "message"),
        [
            ({"host": None}, "host is required"),
            ({"extra": {"deployment": "server"}}, "`deployment` extra must be one of"),
            ({"extra": {"verify": "maybe"}}, "`verify` extra must be true or false"),
            ({"extra": {"verify": 1}}, "`verify` extra must be true or false"),
            ({"host": "https://confluence.example.com:notaport"}, "invalid port"),
            ({"login": None, "password": None}, "requires either the `token` extra or both"),
            ({"password": None}, "requires either the `token` extra or both"),
            ({"host": CLOUD_URL, "extra": {"token": "pat"}}, "put the account email in Login"),
            ({"host": CLOUD_URL, "password": None}, "requires both Login"),
            (
                {"host": "https://wiki.example.com", "extra": {"deployment": "cloud", "token": "pat"}},
                "put the account email in Login",
            ),
        ],
    )
    def test_invalid_connections_are_rejected(self, sdk, set_connection, fields, message):
        set_connection(**fields)

        with pytest.raises(ConfluenceConnectionError, match=message):
            ConfluenceHook().get_conn()

    def test_conn_id_is_required(self, sdk):
        with pytest.raises(ConfluenceConnectionError, match="confluence_conn_id is required"):
            ConfluenceHook(confluence_conn_id="").get_conn()

    @pytest.mark.parametrize(
        ("verify", "expected"),
        [
            (None, True),
            (True, True),
            (False, False),
            ("true", True),
            ("TRUE", True),
            ("false", False),
            ("False", False),
        ],
    )
    def test_verify_is_coerced_to_bool(self, sdk, set_connection, verify, expected):
        set_connection(extra={"verify": verify})

        ConfluenceHook().get_conn()

        assert sdk.server_ctor.call_args.kwargs["verify_ssl"] is expected


class TestFormWidgets:
    def test_widgets_cover_the_connection_extras(self):
        pytest.importorskip("wtforms")

        widgets = ConfluenceHook.get_connection_form_widgets()

        assert set(widgets) == {"deployment", "token", "verify"}
        assert [value for value, _ in widgets["deployment"].kwargs["choices"]] == [
            "auto",
            "cloud",
            "data_center",
        ]

    def test_no_widgets_without_wtforms(self):
        with mock.patch.dict(sys.modules, {"wtforms": None}):
            assert ConfluenceHook.get_connection_form_widgets() == {}


class TestProviderMetadata:
    """The hook's legacy form hooks must stay in sync with the ``conn-fields`` in provider.yaml."""

    @staticmethod
    def get_connection_type() -> dict[str, Any]:
        (connection_type,) = get_provider_info()["connection-types"]
        return connection_type

    def test_ui_field_behaviour_matches_provider_yaml(self):
        yaml_behaviour = self.get_connection_type()["ui-field-behaviour"]

        assert ConfluenceHook.get_ui_field_behaviour() == {
            key.replace("-", "_"): value for key, value in yaml_behaviour.items()
        }

    def test_widgets_match_provider_yaml_conn_fields(self):
        pytest.importorskip("wtforms")

        assert set(ConfluenceHook.get_connection_form_widgets()) == set(
            self.get_connection_type()["conn-fields"]
        )


class TestTestConnection:
    @pytest.mark.parametrize("host", [DC_URL, CLOUD_URL])
    def test_authenticated_user_succeeds(self, sdk, set_connection, host):
        set_connection(host=host)
        client = sdk.cloud if host == CLOUD_URL else sdk.server
        client.get.return_value = {"type": "known", "username": "user"}

        assert ConfluenceHook().test_connection() == (True, "Connection successfully tested")
        client.get.assert_called_once_with("rest/api/user/current")

    def test_anonymous_user_fails(self, sdk, set_connection):
        set_connection()
        sdk.server.get.return_value = {"type": "anonymous"}

        success, message = ConfluenceHook().test_connection()

        assert not success
        assert "anonymous" in message

    def test_request_errors_are_reported(self, sdk, set_connection):
        set_connection()
        sdk.server.get.side_effect = ConnectionError("boom")

        assert ConfluenceHook().test_connection() == (False, "boom")

    def test_connection_errors_are_reported(self, sdk, set_connection):
        set_connection(login=None)

        success, message = ConfluenceHook().test_connection()

        assert not success
        assert "requires either the `token` extra" in message


@dataclass
class Deployment:
    """One deployment under test: its hook, spec'd client and the shapes of its responses."""

    name: str
    hook: ConfluenceHook
    client: mock.Mock
    base_url: str

    @property
    def is_cloud(self) -> bool:
        return self.name == "cloud"

    def item(self, content_id: str = "10", title: str = "Report", version: int = 3) -> dict[str, Any]:
        return {
            "id": content_id,
            "title": title,
            "version": {"number": version},
            "_links": {"webui": f"/spaces/SPACE/pages/{content_id}", "base": self.base_url},
        }

    def stub_content(self, *items: dict[str, Any]) -> None:
        """Make the client find the given items by title, and know the space ``SPACE``."""
        by_title = {item["title"]: item for item in items}

        def _get(path: str, params: dict[str, Any] | None = None, **_: Any) -> dict[str, Any]:
            if path == "api/v2/spaces":
                return {"results": [{"id": "99", "key": "SPACE"}]}
            params = params or {}
            title = params.get("title")
            if "cql" in params:
                quoted = params["cql"].rsplit("title = ", 1)[1]
                title = quoted[1:-1].replace('\\"', '"').replace("\\\\", "\\")
            found = by_title.get(title) if title else None
            return {"results": [found] if found else [], "_links": {"base": self.base_url}}

        self.client.get.side_effect = _get

    def content_calls(self) -> list[Any]:
        """The ``get`` calls that look content up, leaving out the Cloud space-ID lookup."""
        return [call for call in self.client.get.call_args_list if call.args[0] != "api/v2/spaces"]


@pytest.fixture(params=["data_center", "cloud"])
def deployment(request, sdk, set_connection) -> Deployment:
    if request.param == "cloud":
        set_connection(host=CLOUD_URL)
        return Deployment("cloud", ConfluenceHook(), sdk.cloud, CLOUD_URL)
    set_connection()
    return Deployment("data_center", ConfluenceHook(), sdk.server, DC_URL)


def _find_params(deployment: Deployment, title: str) -> dict[str, Any]:
    if deployment.is_cloud:
        return {"space-id": "99", "title": title, "status": "current", "limit": 1}
    return {
        "spaceKey": "SPACE",
        "title": title,
        "type": "page",
        "expand": "version,space",
        "limit": 1,
    }


def _collection(deployment: Deployment, content_type: str = "page") -> str:
    if deployment.is_cloud:
        return "api/v2/pages" if content_type == "page" else "api/v2/blogposts"
    return "rest/api/content"


class TestFindContent:
    def test_request_and_summary(self, deployment):
        deployment.stub_content(deployment.item())

        content = deployment.hook.find_content("SPACE", "Report")

        assert deployment.content_calls() == [
            mock.call(_collection(deployment), params=_find_params(deployment, "Report"))
        ]
        assert content == {
            "id": "10",
            "type": "page",
            "title": "Report",
            "space": "SPACE",
            "version": 3,
            "url": f"{deployment.base_url}/spaces/SPACE/pages/10",
            "action": "skipped",
        }

    def test_blogpost_lookup(self, deployment):
        title = 'Say "hi" \\o/'
        deployment.stub_content(deployment.item(title=title))

        content = deployment.hook.find_content("SPACE", title, content_type="blogpost")

        (call,) = deployment.content_calls()
        if deployment.is_cloud:
            assert call.args == ("api/v2/blogposts",)
            assert call.kwargs["params"]["title"] == title
        else:
            # Data Center's title lookup needs ``postingDay`` for blog posts, so it goes through CQL.
            assert call == mock.call(
                "rest/api/content/search",
                params={
                    "cql": 'space = "SPACE" and type = blogpost and title = "Say \\"hi\\" \\\\o/"',
                    "expand": "version,space",
                    "limit": 1,
                },
            )
        assert content is not None
        assert content["type"] == "blogpost"

    def test_missing_content_returns_none(self, deployment):
        deployment.stub_content()

        assert deployment.hook.find_content("SPACE", "Nope") is None

    @pytest.mark.parametrize(
        ("links", "list_base", "expected"),
        [
            (
                {"webui": "/p/1", "base": "https://item.example"},
                "https://list.example",
                "https://item.example/p/1",
            ),
            ({"webui": "/p/1"}, "https://list.example", "https://list.example/p/1"),
            ({"webui": "/p/1"}, None, None),
            ({"base": "https://item.example"}, None, None),
            (None, "https://list.example", None),
        ],
    )
    def test_url_is_built_from_base_and_webui(self, deployment, links, list_base, expected):
        item = {"id": "10", "title": "Report", "version": {"number": 1}}
        if links is not None:
            item["_links"] = links
        response: dict[str, Any] = {"results": [item]}
        if list_base:
            response["_links"] = {"base": list_base}
        deployment.client.get.side_effect = lambda path, params=None, **_: (
            {"results": [{"id": "99"}]} if path == "api/v2/spaces" else response
        )

        content = deployment.hook.find_content("SPACE", "Report")

        assert content is not None
        assert content["url"] == expected

    def test_unknown_space_on_cloud(self, sdk, set_connection):
        set_connection(host=CLOUD_URL)
        sdk.cloud.get.return_value = {"results": []}

        with pytest.raises(ConfluenceSpaceNotFoundError, match="'SPACE'"):
            ConfluenceHook().find_content("SPACE", "Report")

        sdk.cloud.get.assert_called_once_with("api/v2/spaces", params={"keys": "SPACE"})

    def test_cloud_space_id_is_looked_up_once(self, sdk, set_connection):
        set_connection(host=CLOUD_URL)
        deployment = Deployment("cloud", ConfluenceHook(), sdk.cloud, CLOUD_URL)
        deployment.stub_content()

        deployment.hook.find_content("SPACE", "One")
        deployment.hook.find_content("SPACE", "Two")

        spaces_calls = [c for c in sdk.cloud.get.call_args_list if c.args[0] == "api/v2/spaces"]
        assert len(spaces_calls) == 1


def _create_payload(deployment: Deployment, content_type: str, parent_id: str | None) -> dict[str, Any]:
    if deployment.is_cloud:
        payload: dict[str, Any] = {
            "spaceId": "99",
            "status": "current",
            "title": "Report",
            "body": {"representation": "storage", "value": BODY},
        }
        if parent_id:
            payload["parentId"] = parent_id
        return payload
    payload = {
        "type": content_type,
        "title": "Report",
        "space": {"key": "SPACE"},
        "body": {"storage": {"value": BODY, "representation": "storage"}},
    }
    if parent_id:
        payload["ancestors"] = [{"id": parent_id}]
    return payload


def _update_payload(deployment: Deployment, content_type: str, version_comment: str | None) -> dict[str, Any]:
    version: dict[str, Any] = {"number": 4}
    if version_comment:
        version["message"] = version_comment
    if deployment.is_cloud:
        return {
            "id": "10",
            "status": "current",
            "title": "Report",
            "body": {"representation": "storage", "value": BODY},
            "version": version,
        }
    return {
        "id": "10",
        "type": content_type,
        "title": "Report",
        "space": {"key": "SPACE"},
        "body": {"storage": {"value": BODY, "representation": "storage"}},
        "version": version,
    }


class TestPublish:
    @pytest.mark.parametrize(
        ("content_type", "parent_id"), [("page", None), ("page", "77"), ("blogpost", None)]
    )
    def test_creates_missing_content(self, deployment, content_type, parent_id):
        deployment.stub_content()
        deployment.client.post.return_value = deployment.item(content_id="11", version=1)

        result = deployment.hook.publish(
            "SPACE", "Report", BODY, content_type=content_type, parent_id=parent_id
        )

        deployment.client.post.assert_called_once_with(
            _collection(deployment, content_type), data=_create_payload(deployment, content_type, parent_id)
        )
        deployment.client.put.assert_not_called()
        assert result == {
            "id": "11",
            "type": content_type,
            "title": "Report",
            "space": "SPACE",
            "version": 1,
            "url": f"{deployment.base_url}/spaces/SPACE/pages/11",
            "action": "created",
        }

    @pytest.mark.parametrize("content_type", ["page", "blogpost"])
    @pytest.mark.parametrize("version_comment", [None, "Nightly run"])
    def test_updates_existing_content_in_place(self, deployment, content_type, version_comment):
        deployment.stub_content(deployment.item(version=3))
        deployment.client.put.return_value = deployment.item(version=4)

        result = deployment.hook.publish(
            "SPACE", "Report", BODY, content_type=content_type, version_comment=version_comment
        )

        deployment.client.put.assert_called_once_with(
            f"{_collection(deployment, content_type)}/10",
            data=_update_payload(deployment, content_type, version_comment),
        )
        deployment.client.post.assert_not_called()
        assert result["action"] == "updated"
        assert result["version"] == 4
        assert result["id"] == "10"

    def test_parent_is_ignored_when_updating(self, deployment):
        deployment.stub_content(deployment.item(), deployment.item(content_id="77", title="Parent"))
        deployment.client.put.return_value = deployment.item(version=4)

        deployment.hook.publish("SPACE", "Report", BODY, parent_title="Parent")

        deployment.client.put.assert_called_once_with(
            f"{_collection(deployment)}/10", data=_update_payload(deployment, "page", None)
        )
        assert len(deployment.content_calls()) == 1

    def test_skip_returns_existing_content_untouched(self, deployment):
        deployment.stub_content(deployment.item())

        result = deployment.hook.publish("SPACE", "Report", BODY, if_exists="skip")

        assert result["action"] == "skipped"
        assert result["id"] == "10"
        deployment.client.put.assert_not_called()
        deployment.client.post.assert_not_called()

    def test_fail_raises_when_content_exists(self, deployment):
        deployment.stub_content(deployment.item())

        with pytest.raises(ConfluenceContentExistsError, match="'Report'"):
            deployment.hook.publish("SPACE", "Report", BODY, if_exists="fail")

        deployment.client.put.assert_not_called()
        deployment.client.post.assert_not_called()

    def test_fail_creates_when_content_is_missing(self, deployment):
        deployment.stub_content()
        deployment.client.post.return_value = deployment.item(version=1)

        result = deployment.hook.publish("SPACE", "Report", BODY, if_exists="fail")

        assert result["action"] == "created"

    def test_parent_title_is_resolved_within_the_space(self, deployment):
        deployment.stub_content(deployment.item(content_id="77", title="Parent"))
        deployment.client.post.return_value = deployment.item(content_id="11", version=1)

        deployment.hook.publish("SPACE", "Report", BODY, parent_title="Parent")

        assert [call.kwargs["params"]["title"] for call in deployment.content_calls()] == ["Report", "Parent"]
        assert deployment.content_calls()[1].kwargs["params"] == _find_params(deployment, "Parent")
        deployment.client.post.assert_called_once_with(
            _collection(deployment), data=_create_payload(deployment, "page", "77")
        )

    def test_missing_parent_title_raises(self, deployment):
        deployment.stub_content()

        with pytest.raises(ConfluenceContentNotFoundError, match="'Parent'"):
            deployment.hook.publish("SPACE", "Report", BODY, parent_title="Parent")

        deployment.client.post.assert_not_called()

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"parent_id": "1", "parent_title": "P"}, "at most one of"),
            ({"content_type": "blogpost", "parent_id": "1"}, "Blog posts cannot have a parent"),
            ({"content_type": "blogpost", "parent_title": "P"}, "Blog posts cannot have a parent"),
            ({"if_exists": "replace"}, "`if_exists` must be one of"),
        ],
    )
    def test_invalid_arguments_are_rejected_before_any_request(self, deployment, kwargs, message):
        with pytest.raises(ValueError, match=message):
            deployment.hook.publish("SPACE", "Report", BODY, **kwargs)

        deployment.client.get.assert_not_called()

    def test_unknown_space_on_cloud(self, sdk, set_connection):
        set_connection(host=CLOUD_URL)
        sdk.cloud.get.return_value = {"results": []}

        with pytest.raises(ConfluenceSpaceNotFoundError):
            ConfluenceHook().publish("SPACE", "Report", BODY)

        sdk.cloud.post.assert_not_called()


class TestDeleteContent:
    @pytest.mark.parametrize("content_type", ["page", "blogpost"])
    def test_deletes_by_id(self, deployment, content_type):
        deployment.hook.delete_content("10", content_type=content_type)

        deployment.client.delete.assert_called_once_with(f"{_collection(deployment, content_type)}/10")


class TestSearchCql:
    @pytest.mark.parametrize("host", [DC_URL, CLOUD_URL])
    def test_returns_the_results(self, sdk, set_connection, host):
        set_connection(host=host)
        client = sdk.cloud if host == CLOUD_URL else sdk.server
        client.get.return_value = {"results": [{"title": "A"}], "size": 1}

        assert ConfluenceHook().search_cql('space = "DOCS"', limit=5) == [{"title": "A"}]
        client.get.assert_called_once_with("rest/api/search", params={"cql": 'space = "DOCS"', "limit": 5})

    def test_limit_defaults_to_25(self, sdk, set_connection):
        set_connection()
        sdk.server.get.return_value = {"results": []}

        ConfluenceHook().search_cql("type = page")

        sdk.server.get.assert_called_once_with("rest/api/search", params={"cql": "type = page", "limit": 25})

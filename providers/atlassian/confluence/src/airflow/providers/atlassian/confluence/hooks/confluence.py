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
"""Hook for Atlassian Confluence."""

from __future__ import annotations

import warnings
from abc import ABC, abstractmethod
from collections.abc import MutableMapping
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal, TypedDict, cast
from urllib.parse import urlsplit, urlunsplit

from atlassian import ConfluenceServer, ConfluenceV2

from airflow.providers.atlassian.confluence.exceptions import (
    ConfluenceConnectionError,
    ConfluenceContentExistsError,
    ConfluenceContentNotFoundError,
    ConfluenceSpaceNotFoundError,
)
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection

ContentType = Literal["page", "blogpost"]
IfExists = Literal["update", "skip", "fail"]
Action = Literal["created", "updated", "skipped"]

IF_EXISTS_CHOICES: tuple[str, ...] = ("update", "skip", "fail")

# Hostnames (and their subdomains) that the SDK itself treats as Confluence Cloud.
_CLOUD_DOMAINS = ("atlassian.net", "jira.com", "api.atlassian.com")
_DEPLOYMENTS = ("auto", "cloud", "data_center")


class ConfluenceContent(TypedDict):
    """Summary of a page or blog post, as returned by :class:`ConfluenceHook`."""

    id: str
    type: str
    title: str
    space: str
    version: int
    url: str | None
    action: Action


@dataclass(frozen=True)
class _ConnectionSettings:
    """Client settings resolved from an Airflow Connection."""

    url: str
    cloud: bool
    verify: bool
    login: str | None
    password: str | None
    token: str | None


def _coerce_verify(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return value.lower() == "true"
    raise ConfluenceConnectionError(f"The `verify` extra must be true or false, got {value!r}")


def _build_base_url(conn: Connection) -> str:
    """Build the base URL from the connection's host, applying ``port`` only if the URL has none."""
    host = str(conn.host).strip()
    parts = urlsplit(host if "://" in host else f"https://{host}")
    try:
        has_port = parts.port is not None
    except ValueError as e:
        raise ConfluenceConnectionError(f"The Confluence host {host!r} has an invalid port") from e

    netloc = parts.netloc
    if conn.port and not has_port:
        netloc = f"{netloc}:{conn.port}"
    return urlunsplit(parts._replace(netloc=netloc, path=parts.path.rstrip("/")))


def _quote_cql(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _is_cloud_hostname(url: str) -> bool:
    hostname = (urlsplit(url).hostname or "").lower()
    return any(hostname == domain or hostname.endswith(f".{domain}") for domain in _CLOUD_DOMAINS)


def _resolve_connection_settings(conn: Connection) -> _ConnectionSettings:
    """Turn a Connection into validated client settings; the one place connection rules live."""
    if not conn.host:
        raise ConfluenceConnectionError("Failed to create Confluence client: host is required")

    extras = conn.extra_dejson if conn.extra else {}
    deployment = extras.get("deployment") or "auto"
    if deployment not in _DEPLOYMENTS:
        raise ConfluenceConnectionError(
            f"The `deployment` extra must be one of {', '.join(_DEPLOYMENTS)}, got {deployment!r}"
        )

    url = _build_base_url(conn)
    cloud = _is_cloud_hostname(url) if deployment == "auto" else deployment == "cloud"
    token = extras.get("token") or None

    if cloud:
        if token:
            raise ConfluenceConnectionError(
                "Confluence Cloud does not accept the `token` extra: put the account email in "
                "Login and the API token in Password"
            )
        if not (conn.login and conn.password):
            raise ConfluenceConnectionError(
                "Confluence Cloud requires both Login (account email) and Password (API token)"
            )
    elif not token and not (conn.login and conn.password):
        raise ConfluenceConnectionError(
            "Confluence Data Center requires either the `token` extra or both Login and Password"
        )

    return _ConnectionSettings(
        url=url,
        cloud=cloud,
        verify=_coerce_verify(extras.get("verify")),
        login=conn.login or None,
        password=conn.password or None,
        token=token,
    )


class _ContentApi(ABC):
    """The REST shapes for finding, creating, updating and deleting content on one deployment."""

    def __init__(self, client: ConfluenceServer | ConfluenceV2) -> None:
        self._client = client

    @abstractmethod
    def find(self, space: str, title: str, content_type: ContentType) -> ConfluenceContent | None:
        """Return the content titled ``title`` in ``space``, if any."""

    @abstractmethod
    def create(
        self, space: str, title: str, body: str, content_type: ContentType, parent_id: str | None
    ) -> ConfluenceContent:
        """Create content and return its summary."""

    @abstractmethod
    def update(
        self, existing: ConfluenceContent, body: str, version_comment: str | None
    ) -> ConfluenceContent:
        """Replace the body of ``existing`` with a new version and return its summary."""

    @abstractmethod
    def delete(self, content_id: str, content_type: ContentType) -> None:
        """Delete content by ID."""

    def _get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        return cast("dict[str, Any]", self._client.get(path, params=params))

    def _post(self, path: str, data: dict[str, Any]) -> dict[str, Any]:
        return cast("dict[str, Any]", self._client.post(path, data=data))

    def _put(self, path: str, data: dict[str, Any]) -> dict[str, Any]:
        return cast("dict[str, Any]", self._client.put(path, data=data))

    @staticmethod
    def _summarize(
        item: dict[str, Any],
        *,
        space: str,
        content_type: ContentType,
        action: Action,
        list_base: str | None = None,
    ) -> ConfluenceContent:
        links = item.get("_links") or {}
        base = links.get("base") or list_base
        webui = links.get("webui")
        return ConfluenceContent(
            id=str(item["id"]),
            type=content_type,
            title=item["title"],
            space=space,
            version=int(item["version"]["number"]),
            url=f"{base}{webui}" if base and webui else None,
            action=action,
        )

    @staticmethod
    def _build_version(number: int, comment: str | None) -> dict[str, Any]:
        return {"number": number, "message": comment} if comment else {"number": number}


class _DataCenterContentApi(_ContentApi):
    """Confluence Data Center: REST v1 (``rest/api/content``)."""

    def find(self, space: str, title: str, content_type: ContentType) -> ConfluenceContent | None:
        if content_type == "blogpost":
            # ``rest/api/content`` requires ``postingDay`` to look up blog posts by title.
            cql = f"space = {_quote_cql(space)} and type = blogpost and title = {_quote_cql(title)}"
            response = self._get(
                "rest/api/content/search", {"cql": cql, "expand": "version,space", "limit": 1}
            )
        else:
            response = self._get(
                "rest/api/content",
                {
                    "spaceKey": space,
                    "title": title,
                    "type": content_type,
                    "expand": "version,space",
                    "limit": 1,
                },
            )
        results = response.get("results") or []
        if not results:
            return None
        return self._summarize(
            results[0],
            space=space,
            content_type=content_type,
            action="skipped",
            list_base=(response.get("_links") or {}).get("base"),
        )

    def create(
        self, space: str, title: str, body: str, content_type: ContentType, parent_id: str | None
    ) -> ConfluenceContent:
        payload = self._build_payload(space, title, body, content_type)
        if parent_id:
            payload["ancestors"] = [{"id": parent_id}]
        item = self._post("rest/api/content", payload)
        return self._summarize(item, space=space, content_type=content_type, action="created")

    def update(
        self, existing: ConfluenceContent, body: str, version_comment: str | None
    ) -> ConfluenceContent:
        content_type = cast("ContentType", existing["type"])
        payload = self._build_payload(existing["space"], existing["title"], body, content_type)
        payload["id"] = existing["id"]
        payload["version"] = self._build_version(existing["version"] + 1, version_comment)
        item = self._put(f"rest/api/content/{existing['id']}", payload)
        return self._summarize(item, space=existing["space"], content_type=content_type, action="updated")

    def delete(self, content_id: str, content_type: ContentType) -> None:
        self._client.delete(f"rest/api/content/{content_id}")

    @staticmethod
    def _build_payload(space: str, title: str, body: str, content_type: ContentType) -> dict[str, Any]:
        return {
            "type": content_type,
            "title": title,
            "space": {"key": space},
            "body": {"storage": {"value": body, "representation": "storage"}},
        }


class _CloudContentApi(_ContentApi):
    """Confluence Cloud: REST v2 (``api/v2/pages`` and ``api/v2/blogposts``)."""

    _COLLECTIONS: dict[str, str] = {"page": "api/v2/pages", "blogpost": "api/v2/blogposts"}

    def __init__(self, client: ConfluenceServer | ConfluenceV2) -> None:
        super().__init__(client)
        self._space_ids: dict[str, str] = {}

    def find(self, space: str, title: str, content_type: ContentType) -> ConfluenceContent | None:
        response = self._get(
            self._COLLECTIONS[content_type],
            {"space-id": self._get_space_id(space), "title": title, "status": "current", "limit": 1},
        )
        results = response.get("results") or []
        if not results:
            return None
        return self._summarize(
            results[0],
            space=space,
            content_type=content_type,
            action="skipped",
            list_base=(response.get("_links") or {}).get("base"),
        )

    def create(
        self, space: str, title: str, body: str, content_type: ContentType, parent_id: str | None
    ) -> ConfluenceContent:
        payload: dict[str, Any] = {
            "spaceId": self._get_space_id(space),
            "status": "current",
            "title": title,
            "body": {"representation": "storage", "value": body},
        }
        if parent_id:
            payload["parentId"] = parent_id
        item = self._post(self._COLLECTIONS[content_type], payload)
        return self._summarize(item, space=space, content_type=content_type, action="created")

    def update(
        self, existing: ConfluenceContent, body: str, version_comment: str | None
    ) -> ConfluenceContent:
        content_type = cast("ContentType", existing["type"])
        payload = {
            "id": existing["id"],
            "status": "current",
            "title": existing["title"],
            "body": {"representation": "storage", "value": body},
            "version": self._build_version(existing["version"] + 1, version_comment),
        }
        item = self._put(f"{self._COLLECTIONS[content_type]}/{existing['id']}", payload)
        return self._summarize(item, space=existing["space"], content_type=content_type, action="updated")

    def delete(self, content_id: str, content_type: ContentType) -> None:
        self._client.delete(f"{self._COLLECTIONS[content_type]}/{content_id}")

    def _get_space_id(self, space_key: str) -> str:
        if space_key not in self._space_ids:
            results = self._get("api/v2/spaces", {"keys": space_key}).get("results") or []
            if not results:
                raise ConfluenceSpaceNotFoundError(f"Confluence space {space_key!r} was not found")
            self._space_ids[space_key] = str(results[0]["id"])
        return self._space_ids[space_key]


class ConfluenceHook(BaseHook):
    """
    Interact with Atlassian Confluence, Cloud or Data Center.

    The hook owns the operations that need provider logic: connecting, finding and publishing
    pages and blog posts, deleting content and CQL search. For everything else call
    :meth:`get_conn` and use the `atlassian-python-api
    <https://atlassian-python-api.readthedocs.io/confluence.html>`__ client directly.

    The client is created lazily on first use, so instantiating the hook never performs I/O
    and never requires a reachable Confluence instance. Data Center uses REST v1 and Cloud
    uses REST v2; the deployment is taken from the connection's ``deployment`` extra.

    :param confluence_conn_id: reference to a pre-defined Confluence Connection
    :param proxies: Proxies to make the Confluence REST API call. Optional
    """

    conn_name_attr = "confluence_conn_id"
    default_conn_name = "confluence_default"
    conn_type = "confluence"
    hook_name = "Confluence"

    def __init__(
        self,
        confluence_conn_id: str = default_conn_name,
        proxies: MutableMapping[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.confluence_conn_id = confluence_conn_id
        self.proxies = proxies
        self._client: ConfluenceServer | ConfluenceV2 | None = None

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to the Atlassian Confluence connection form."""
        # Only needed on Airflow 3.0 and 3.1; remove once the Airflow floor reaches 3.2, where
        # the ``conn-fields`` in provider.yaml take over.
        try:
            from wtforms import BooleanField, PasswordField, SelectField
        except ImportError:
            return {}

        return {
            "deployment": SelectField(
                "Deployment",
                choices=[("auto", "Auto-detect"), ("cloud", "Cloud"), ("data_center", "Data Center")],
                default="auto",
            ),
            "token": PasswordField("Data Center Personal Access Token"),
            "verify": BooleanField("Verify SSL", default=True),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for the Atlassian Confluence connection."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {"login": "Username / Email", "password": "Password / API token"},
            "placeholders": {"host": "https://example.atlassian.net/wiki"},
        }

    @cached_property
    def _settings(self) -> _ConnectionSettings:
        if not self.confluence_conn_id:
            raise ConfluenceConnectionError(
                "Failed to create Confluence client: confluence_conn_id is required"
            )
        return _resolve_connection_settings(self.get_connection(self.confluence_conn_id))

    @cached_property
    def is_cloud(self) -> bool:
        """Whether the connection points at Confluence Cloud rather than Data Center."""
        return self._settings.cloud

    @cached_property
    def _content_api(self) -> _ContentApi:
        client = self.get_conn()
        return _CloudContentApi(client) if self.is_cloud else _DataCenterContentApi(client)

    def get_conn(self) -> ConfluenceServer | ConfluenceV2:
        """
        Return the `atlassian-python-api` client, creating it on first call.

        The client is a ``ConfluenceV2`` on Cloud and a ``ConfluenceServer`` on Data Center.
        """
        if self._client is not None:
            return self._client

        settings = self._settings
        self.log.debug("Creating Confluence client for conn_id: %s", self.confluence_conn_id)

        if settings.cloud:
            # ConfluenceV2 warns about its own legacy v1 methods on every construction, which
            # this hook never calls.
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                self._client = ConfluenceV2(
                    url=settings.url,
                    username=settings.login,
                    password=settings.password,
                    verify_ssl=settings.verify,
                    proxies=self.proxies,
                )
        elif settings.token:
            self._client = ConfluenceServer(
                url=settings.url, token=settings.token, verify_ssl=settings.verify, proxies=self.proxies
            )
        else:
            self._client = ConfluenceServer(
                url=settings.url,
                username=settings.login,
                password=settings.password,
                verify_ssl=settings.verify,
                proxies=self.proxies,
            )
        return self._client

    def test_connection(self) -> tuple[bool, str]:
        """Check the credentials by fetching the current user."""
        try:
            user = cast("dict[str, Any]", self.get_conn().get("rest/api/user/current"))
        except Exception as e:
            return False, str(e)
        if user.get("type") == "anonymous":
            return False, "Connected, but the credentials were not accepted (anonymous user)"
        return True, "Connection successfully tested"

    def find_content(
        self, space: str, title: str, *, content_type: ContentType = "page"
    ) -> ConfluenceContent | None:
        """
        Find a page or blog post by title within a space.

        :param space: key of the space to search
        :param title: exact title of the content
        :param content_type: ``page`` or ``blogpost``
        :return: the content summary, or ``None`` when there is no match. Nothing is
            written, so ``action`` is always ``skipped``.
        """
        return self._content_api.find(space, title, content_type)

    def publish(
        self,
        space: str,
        title: str,
        body: str,
        *,
        content_type: ContentType = "page",
        parent_id: str | None = None,
        parent_title: str | None = None,
        if_exists: IfExists = "update",
        version_comment: str | None = None,
    ) -> ConfluenceContent:
        """
        Create a page or blog post, or handle the existing one with the same title.

        Existing content is found by title within ``space`` and, for ``if_exists="update"``,
        updated in place wherever it lives in the page tree. The parent therefore only
        applies when the content is created.

        :param space: key of the space to publish into
        :param title: title of the content
        :param body: body in Confluence storage format
        :param content_type: ``page`` or ``blogpost``
        :param parent_id: content ID of the parent page; pages only, mutually exclusive
            with ``parent_title``
        :param parent_title: title of the parent page, resolved within ``space``; pages
            only, mutually exclusive with ``parent_id``
        :param if_exists: what to do when the title exists: ``update`` writes a new version,
            ``skip`` returns the existing content untouched, ``fail`` raises
            :class:`~airflow.providers.atlassian.confluence.exceptions.ConfluenceContentExistsError`
        :param version_comment: comment recorded against the new version on update
        :raises ValueError: for invalid argument combinations
        :raises ConfluenceContentNotFoundError: when ``parent_title`` does not exist in ``space``
        :raises ConfluenceSpaceNotFoundError: when ``space`` does not exist (Cloud)
        :return: the content summary; ``action`` says what was done
        """
        if if_exists not in IF_EXISTS_CHOICES:
            raise ValueError(f"`if_exists` must be one of {', '.join(IF_EXISTS_CHOICES)}, got {if_exists!r}")
        if parent_id and parent_title:
            raise ValueError("Give at most one of `parent_id` and `parent_title`, not both")
        if content_type == "blogpost" and (parent_id or parent_title):
            raise ValueError("Blog posts cannot have a parent")

        api = self._content_api
        existing = api.find(space, title, content_type)
        if existing is not None:
            if if_exists == "fail":
                raise ConfluenceContentExistsError(
                    f"A {content_type} titled {title!r} already exists in space {space!r} (id {existing['id']})"
                )
            if if_exists == "skip":
                return existing
            result = api.update(existing, body, version_comment)
        else:
            if parent_title:
                parent = api.find(space, parent_title, "page")
                if parent is None:
                    raise ConfluenceContentNotFoundError(
                        f"Parent page titled {parent_title!r} was not found in space {space!r}"
                    )
                parent_id = parent["id"]
            result = api.create(space, title, body, content_type, parent_id)

        self.log.info("Confluence %s %r %s (id %s)", content_type, title, result["action"], result["id"])
        return result

    def delete_content(self, content_id: str, *, content_type: ContentType = "page") -> None:
        """
        Delete a page or blog post by ID.

        :param content_id: ID of the content to delete
        :param content_type: ``page`` or ``blogpost``
        """
        self._content_api.delete(content_id, content_type)

    def search_cql(self, cql: str, *, limit: int = 25) -> list[dict[str, Any]]:
        """
        Search content with the Confluence Query Language.

        :param cql: the CQL query, e.g. ``space = "DOCS" and type = page``
        :param limit: maximum number of results to return
        :return: the ``results`` of the search response
        """
        response = cast(
            "dict[str, Any]", self.get_conn().get("rest/api/search", params={"cql": cql, "limit": limit})
        )
        return response.get("results", [])

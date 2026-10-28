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

from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit, urlunsplit

from atlassian import Confluence

from airflow.providers.atlassian.confluence.exceptions import ConfluenceConnectionError
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection


class ConfluenceHook(BaseHook):
    """
    Confluence interaction hook, a wrapper around the Atlassian Confluence Python SDK.

    The client is created lazily on first use, so instantiating the hook never
    performs I/O and never requires a reachable Confluence instance.

    Authentication is taken from the Airflow Connection. When the ``token`` extra is
    set it is used as the sole credential (Confluence Cloud API token, or a Data
    Center Personal Access Token); otherwise the connection's login and password are
    used for basic authentication.

    :param confluence_conn_id: reference to a pre-defined Confluence Connection
    :param proxies: Proxies to make the Confluence REST API call. Optional
    :param api_root: root for the api requests. Optional
    :param api_version: Confluence api version to use. Optional
    """

    conn_name_attr = "confluence_conn_id"
    default_conn_name = "confluence_default"
    conn_type = "confluence"
    hook_name = "Confluence"

    def __init__(
        self,
        confluence_conn_id: str = default_conn_name,
        proxies: Any | None = None,
        api_root: str = "rest/api",
        api_version: str | int = "latest",
    ) -> None:
        super().__init__()
        self.confluence_conn_id = confluence_conn_id
        self.proxies = proxies
        self.api_root = api_root
        self.api_version = api_version
        self.client: Confluence | None = None

    def get_conn(self) -> Confluence:
        """Return an initialized Atlassian Confluence SDK client, creating it on first call."""
        if self.client:
            return self.client

        self.log.debug("Creating Confluence client for conn_id: %s", self.confluence_conn_id)

        if not self.confluence_conn_id:
            raise ConfluenceConnectionError(
                "Failed to create Confluence client: confluence_conn_id is required"
            )

        conn = self.get_connection(self.confluence_conn_id)
        extras = conn.extra_dejson if conn.extra else {}
        token = extras.get("token")
        self._validate_connection(conn, token=token)

        credentials: dict[str, Any] = (
            {"token": token} if token else {"username": conn.login, "password": conn.password}
        )
        # Omitted when unset so the SDK's own atlassian.net/jira.com host detection can run;
        # passing cloud= at all (even False) disables that detection.
        cloud_kwarg: dict[str, Any] = {"cloud": extras["cloud"]} if "cloud" in extras else {}

        self.client = Confluence(
            url=self._connection_url(conn),
            verify_ssl=extras.get("verify", True),
            proxies=self.proxies,
            api_version=self.api_version,
            api_root=self.api_root,
            **credentials,
            **cloud_kwarg,
        )
        return self.client

    @staticmethod
    def _validate_connection(conn: Connection, *, token: str | None) -> None:
        """Check that the connection carries a host and a usable set of credentials."""
        if not conn.host:
            raise ConfluenceConnectionError("Failed to create Confluence client: host is required")

        if not token and not (conn.login and conn.password):
            raise ConfluenceConnectionError(
                "Failed to create Confluence client: either the `token` extra or both "
                "login and password are required"
            )

    @staticmethod
    def _connection_url(conn: Connection) -> str:
        """Build a Confluence base URL from the connection's host, schema and port."""
        host = str(conn.host)
        url = host if "://" in host else f"{conn.schema or 'https'}://{host}"

        if not conn.port:
            return url

        parsed = urlsplit(url)
        # A port already spelled out in the host wins over the connection's port field.
        if ":" in parsed.netloc.rsplit("@", 1)[-1]:
            return url

        return urlunsplit(
            (
                parsed.scheme,
                f"{parsed.netloc}:{conn.port}",
                parsed.path,
                parsed.query,
                parsed.fragment,
            )
        )

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to the Atlassian Confluence connection form."""
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, PasswordField

        return {
            "verify": BooleanField(lazy_gettext("Verify SSL"), default=True),
            "token": PasswordField(lazy_gettext("API token / Personal Access Token")),
            "cloud": BooleanField(lazy_gettext("Confluence Cloud"), default=False),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for the Atlassian Confluence connection."""
        return {
            "hidden_fields": ["schema"],
        }

    # Pages

    def create_page(
        self,
        space: str,
        title: str,
        body: str,
        parent_id: str | None = None,
        representation: str = "storage",
        editor: str | None = None,
        full_width: bool = False,
        status: str = "current",
    ) -> dict[str, Any]:
        """Create a new page in a space."""
        return self.get_conn().create_page(
            space=space,
            title=title,
            body=body,
            parent_id=parent_id,
            representation=representation,
            editor=editor,
            full_width=full_width,
            status=status,
        )

    def update_page(
        self,
        page_id: str,
        title: str,
        body: str | None = None,
        parent_id: str | None = None,
        representation: str = "storage",
        minor_edit: bool = False,
        version_comment: str | None = None,
        always_update: bool = False,
        full_width: bool = False,
    ) -> dict[str, Any]:
        """Update an existing page."""
        return self.get_conn().update_page(
            page_id=page_id,
            title=title,
            body=body,
            parent_id=parent_id,
            representation=representation,
            minor_edit=minor_edit,
            version_comment=version_comment,
            always_update=always_update,
            full_width=full_width,
        )

    def append_page(
        self,
        page_id: str,
        title: str,
        append_body: str,
        parent_id: str | None = None,
        representation: str = "storage",
        minor_edit: bool = False,
    ) -> dict[str, Any]:
        """Append content to the end of an existing page."""
        return self.get_conn().append_page(
            page_id=page_id,
            title=title,
            append_body=append_body,
            parent_id=parent_id,
            representation=representation,
            minor_edit=minor_edit,
        )

    def get_page_by_id(
        self,
        page_id: str,
        expand: str | None = None,
        status: str | None = None,
        version: int | None = None,
    ) -> dict[str, Any]:
        """Return a page by its content ID."""
        return self.get_conn().get_page_by_id(page_id=page_id, expand=expand, status=status, version=version)

    def get_page_by_title(
        self,
        space: str,
        title: str,
        start: int = 0,
        limit: int = 1,
        expand: str | None = None,
    ) -> dict[str, Any] | None:
        """Return the first page in a space matching a title, or ``None`` when absent."""
        return self.get_conn().get_page_by_title(
            space=space, title=title, start=start, limit=limit, expand=expand, type="page"
        )

    def get_all_pages_from_space(
        self,
        space: str,
        start: int = 0,
        limit: int = 50,
        status: str | None = None,
        expand: str | None = None,
        content_type: str = "page",
    ) -> list[dict[str, Any]]:
        """Return a page of results listing the content of a space."""
        return self.get_conn().get_all_pages_from_space(
            space=space,
            start=start,
            limit=limit,
            status=status,
            expand=expand,
            content_type=content_type,
        )

    def remove_page(self, page_id: str, status: str | None = None, recursive: bool = False) -> dict[str, Any]:
        """Delete a page, optionally together with its descendants."""
        return self.get_conn().remove_page(page_id=page_id, status=status, recursive=recursive)

    def move_page(
        self,
        space_key: str,
        page_id: str,
        target_id: str | None = None,
        target_title: str | None = None,
        position: str = "append",
    ) -> dict[str, Any]:
        """Move a page relative to another page in the tree."""
        return self.get_conn().move_page(
            space_key=space_key,
            page_id=page_id,
            target_id=target_id,
            target_title=target_title,
            position=position,
        )

    def export_page(self, page_id: str) -> bytes:
        """Export a page as PDF and return the raw document bytes."""
        return self.get_conn().export_page(page_id)

    # Page lookup helpers

    def find_child_page_by_title(
        self, parent_page_id: str, title: str, *, page_type: str = "page"
    ) -> str | None:
        """
        Return the ID of the direct child of ``parent_page_id`` titled ``title``.

        Neither ``start`` nor ``limit`` is passed to the SDK, which makes it return a
        fully paginated generator over every child rather than a single capped page —
        so the search is not silently truncated on parents with many children.

        :param parent_page_id: content ID of the parent page
        :param title: exact title of the child to look for
        :param page_type: child content type to search, e.g. ``page`` or ``blogpost``
        :return: the child's content ID, or ``None`` when no child matches
        """
        children = self.get_conn().get_page_child_by_type(parent_page_id, type=page_type)
        for child in children or ():
            if child.get("title") == title:
                return child.get("id")
        return None

    def get_or_create_page(
        self,
        title: str,
        body: str,
        space: str | None = None,
        parent_id: str | None = None,
        representation: str = "storage",
        minor_edit: bool = False,
        version_comment: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a page, or update it in place when one with the same title already exists.

        Concurrent writes to the same page raise ``atlassian.errors.ApiConflictError``
        (HTTP 409) rather than being retried here; set ``retries`` on the task to have
        Airflow handle that.

        :param title: title of the page to create or update
        :param body: page body in the format given by ``representation``
        :param space: space key; required when ``parent_id`` is not given, and ignored
            when it is, since the space is then taken from the parent
        :param parent_id: content ID of the parent page; when omitted the page is
            created at the top level of ``space``
        :param representation: body markup format, either ``storage`` or ``wiki``
        :param minor_edit: when set, watchers are not notified about the change
        :param version_comment: comment recorded against the new page version
        """
        if parent_id is not None:
            return self.get_conn().update_or_create(
                parent_id=parent_id,
                title=title,
                body=body,
                representation=representation,
                minor_edit=minor_edit,
                version_comment=version_comment,
            )

        if not space:
            raise ValueError("`space` is required when `parent_id` is not given")

        existing = self.get_page_by_title(space=space, title=title)
        if existing:
            return self.update_page(
                page_id=existing["id"],
                title=title,
                body=body,
                representation=representation,
                minor_edit=minor_edit,
                version_comment=version_comment,
            )
        return self.create_page(space=space, title=title, body=body, representation=representation)

    # Blog posts

    def create_blog_post(
        self,
        space: str,
        title: str,
        body: str,
        representation: str = "storage",
        status: str = "current",
    ) -> dict[str, Any]:
        """Create a blog post in a space. Blog posts are flat — they have no parent."""
        return self.get_conn().create_page(
            space=space,
            title=title,
            body=body,
            type="blogpost",
            representation=representation,
            status=status,
        )

    def update_blog_post(
        self,
        page_id: str,
        title: str,
        body: str | None = None,
        representation: str = "storage",
        minor_edit: bool = False,
        version_comment: str | None = None,
    ) -> dict[str, Any]:
        """Update an existing blog post."""
        return self.get_conn().update_page(
            page_id=page_id,
            title=title,
            body=body,
            type="blogpost",
            representation=representation,
            minor_edit=minor_edit,
            version_comment=version_comment,
        )

    def get_blog_post_by_title(
        self, space: str, title: str, expand: str | None = None
    ) -> dict[str, Any] | None:
        """Return the first blog post in a space matching a title, or ``None`` when absent."""
        return self.get_conn().get_page_by_title(space=space, title=title, expand=expand, type="blogpost")

    def get_or_create_blog_post(
        self,
        space: str,
        title: str,
        body: str,
        representation: str = "storage",
        minor_edit: bool = False,
        version_comment: str | None = None,
    ) -> dict[str, Any]:
        """
        Create a blog post, or update it in place when one with the same title already exists.

        As with :meth:`get_or_create_page`, HTTP 409 conflicts are not retried here —
        use the task's ``retries`` instead.
        """
        existing = self.get_blog_post_by_title(space=space, title=title)
        if existing:
            return self.update_blog_post(
                page_id=existing["id"],
                title=title,
                body=body,
                representation=representation,
                minor_edit=minor_edit,
                version_comment=version_comment,
            )
        return self.create_blog_post(space=space, title=title, body=body, representation=representation)

    # Search

    def search_cql(
        self,
        cql: str,
        start: int = 0,
        limit: int | None = None,
        expand: str | None = None,
        include_archived_spaces: bool | None = None,
        excerpt: str | None = None,
    ) -> dict[str, Any]:
        """
        Search for content using the Confluence Query Language.

        :param cql: the CQL query to run
        :param start: index of the first result to return
        :param limit: maximum number of results to return
        :param expand: properties to expand on each result
        :param include_archived_spaces: whether to search archived spaces too
        :param excerpt: excerpt strategy, one of ``indexed``, ``highlight`` or ``none``
        """
        return self.get_conn().cql(
            cql=cql,
            start=start,
            limit=limit,
            expand=expand,
            include_archived_spaces=include_archived_spaces,
            excerpt=excerpt,
        )

    # Labels

    def get_page_labels(
        self,
        page_id: str,
        prefix: str | None = None,
        start: int | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Return the labels attached to a page."""
        return self.get_conn().get_page_labels(page_id=page_id, prefix=prefix, start=start, limit=limit)

    def set_page_label(self, page_id: str, label: str) -> dict[str, Any]:
        """Add a label to a page."""
        return self.get_conn().set_page_label(page_id=page_id, label=label)

    def remove_page_label(self, page_id: str, label: str) -> dict[str, Any]:
        """Remove a label from a page."""
        return self.get_conn().remove_page_label(page_id=page_id, label=label)

    # Comments

    def add_comment(self, page_id: str, text: str) -> dict[str, Any]:
        """Add a comment to a page."""
        return self.get_conn().add_comment(page_id=page_id, text=text)

    def get_page_comments(
        self,
        content_id: str,
        expand: str | None = None,
        parent_version: int | None = None,
        start: int = 0,
        limit: int = 25,
        location: str | None = None,
        depth: str | None = None,
    ) -> dict[str, Any]:
        """Return the comments on a page."""
        return self.get_conn().get_page_comments(
            content_id=content_id,
            expand=expand,
            parent_version=parent_version,
            start=start,
            limit=limit,
            location=location,
            depth=depth,
        )

    # Attachments

    def attach_file(
        self,
        filename: str,
        name: str | None = None,
        content_type: str | None = None,
        page_id: str | None = None,
        title: str | None = None,
        space: str | None = None,
        comment: str | None = None,
    ) -> dict[str, Any]:
        """Upload a file from disk as an attachment on a page."""
        return self.get_conn().attach_file(
            filename=filename,
            name=name,
            content_type=content_type,
            page_id=page_id,
            title=title,
            space=space,
            comment=comment,
        )

    def attach_content(
        self,
        content: bytes,
        name: str,
        content_type: str = "application/binary",
        page_id: str | None = None,
        title: str | None = None,
        space: str | None = None,
        comment: str | None = None,
    ) -> dict[str, Any]:
        """Upload in-memory content as an attachment on a page."""
        return self.get_conn().attach_content(
            content=content,
            name=name,
            content_type=content_type,
            page_id=page_id,
            title=title,
            space=space,
            comment=comment,
        )

    def download_attachments_from_page(
        self,
        page_id: str,
        path: str | None = None,
        start: int = 0,
        limit: int = 50,
        filename: str | None = None,
        to_memory: bool = False,
    ) -> Any:
        """
        Download a page's attachments to a directory, or into memory.

        With ``to_memory`` set, returns a mapping of filename to ``BytesIO``; otherwise
        writes the files under ``path`` and returns a summary of what was downloaded.
        """
        return self.get_conn().download_attachments_from_page(
            page_id=page_id,
            path=path,
            start=start,
            limit=limit,
            filename=filename,
            to_memory=to_memory,
        )

    def delete_attachment(self, page_id: str, filename: str, version: int | None = None) -> dict[str, Any]:
        """Delete an attachment from a page."""
        return self.get_conn().delete_attachment(page_id=page_id, filename=filename, version=version)

    # Content properties

    def get_page_property(self, page_id: str, page_property_key: str) -> dict[str, Any]:
        """Return a single content property of a page."""
        return self.get_conn().get_page_property(page_id=page_id, page_property_key=page_property_key)

    def set_page_property(self, page_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Create a content property on a page."""
        return self.get_conn().set_page_property(page_id=page_id, data=data)

    def update_page_property(self, page_id: str, data: dict[str, Any]) -> dict[str, Any]:
        """Update an existing content property of a page."""
        return self.get_conn().update_page_property(page_id=page_id, data=data)

    def delete_page_property(self, page_id: str, page_property: str) -> dict[str, Any]:
        """Delete a content property from a page."""
        return self.get_conn().delete_page_property(page_id=page_id, page_property=page_property)

    # Restrictions

    def get_all_restrictions_for_content(self, content_id: str) -> dict[str, Any]:
        """Return the restrictions on a piece of content, grouped by operation."""
        return self.get_conn().get_all_restrictions_for_content(content_id=content_id)

    # Folders, databases and whiteboards (Confluence Cloud v2 API)

    def create_folder(self, space_id: str, title: str, parent_id: str | None = None) -> dict[str, Any]:
        """Create a folder in a space."""
        data: dict[str, Any] = {"spaceId": space_id, "title": title}
        if parent_id is not None:
            data["parentId"] = parent_id
        return self.get_conn().post("/api/v2/folders", data=data)

    def get_folder(self, folder_id: str) -> dict[str, Any] | None:
        """Return a folder by its ID."""
        return self.get_conn().get(f"/api/v2/folders/{folder_id}")

    def delete_folder(self, folder_id: str) -> dict[str, Any] | None:
        """Delete a folder. Confluence returns an empty body, so the result is usually ``None``."""
        return self.get_conn().delete(f"/api/v2/folders/{folder_id}")

    def create_database(self, space_id: str, title: str, parent_id: str | None = None) -> dict[str, Any]:
        """Create a database in a space."""
        data: dict[str, Any] = {"spaceId": space_id, "title": title}
        if parent_id is not None:
            data["parentId"] = parent_id
        return self.get_conn().post("/api/v2/databases", data=data)

    def get_database(self, database_id: str) -> dict[str, Any] | None:
        """Return a database by its ID."""
        return self.get_conn().get(f"/api/v2/databases/{database_id}")

    def delete_database(self, database_id: str) -> dict[str, Any] | None:
        """Delete a database. Confluence returns an empty body, so the result is usually ``None``."""
        return self.get_conn().delete(f"/api/v2/databases/{database_id}")

    def create_whiteboard(
        self, space_id: str, title: str | None = None, parent_id: str | None = None
    ) -> dict[str, Any]:
        """Create a whiteboard in a space."""
        return self.get_conn().create_whiteboard(spaceId=space_id, title=title, parentId=parent_id)

    def get_whiteboard(self, whiteboard_id: str) -> dict[str, Any]:
        """Return a whiteboard by its ID."""
        return self.get_conn().get_whiteboard(whiteboard_id)

    def delete_whiteboard(self, whiteboard_id: str) -> dict[str, Any]:
        """Delete a whiteboard."""
        return self.get_conn().delete_whiteboard(whiteboard_id)

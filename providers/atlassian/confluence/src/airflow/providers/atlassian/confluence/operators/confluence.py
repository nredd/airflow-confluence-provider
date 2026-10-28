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

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.atlassian.confluence.hooks.confluence import (
    IF_EXISTS_CHOICES,
    ConfluenceContent,
    ConfluenceHook,
    IfExists,
)
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


def _check_if_exists(if_exists: str) -> None:
    if if_exists not in IF_EXISTS_CHOICES:
        raise ValueError(f"`if_exists` must be one of {', '.join(IF_EXISTS_CHOICES)}, got {if_exists!r}")


class ConfluenceOperator(BaseOperator):
    """
    Call an arbitrary method on the Atlassian Confluence SDK client.

    This is the escape hatch for anything the page and blog post operators do not cover. The
    method is looked up on ``ConfluenceHook.get_conn()``, which is a ``ConfluenceV2`` client
    on Confluence Cloud and a ``ConfluenceServer`` client on Data Center. For the available
    methods see https://atlassian-python-api.readthedocs.io/confluence.html

    The return value is pushed to XCom. Methods that return generators or bytes (e.g.
    exports) cannot be stored there: give a ``result_processor`` that turns the result into
    something serializable, or set ``do_xcom_push=False``.

    .. seealso::
        To publish pages or blog posts use :class:`ConfluencePageOperator` and
        :class:`ConfluenceBlogPostOperator`.

    :param confluence_method: name of the method to call on the SDK client
    :param confluence_method_args: keyword arguments for ``confluence_method`` (templated)
    :param result_processor: called with ``(context, result)``; its return value becomes the
        operator's return value
    :param confluence_conn_id: reference to a pre-defined Confluence Connection (templated)
    """

    template_fields: Sequence[str] = ("confluence_method_args", "confluence_conn_id")

    def __init__(
        self,
        *,
        confluence_method: str,
        confluence_method_args: dict | None = None,
        result_processor: Callable[[Context, Any], Any] | None = None,
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.confluence_method = confluence_method
        self.confluence_method_args = confluence_method_args or {}
        self.result_processor = result_processor
        self.confluence_conn_id = confluence_conn_id

    def execute(self, context: Context) -> Any:
        client = ConfluenceHook(confluence_conn_id=self.confluence_conn_id).get_conn()
        result = getattr(client, self.confluence_method)(**self.confluence_method_args)

        if self.result_processor:
            return self.result_processor(context, result)
        return result


class ConfluencePageOperator(BaseOperator):
    """
    Publish a Confluence page.

    The page is looked up by title within ``space``. A missing page is created, and an existing
    one is handled according to ``if_exists``: updated in place, left alone, or reported as
    an error. An update keeps the page wherever it already lives in the page tree, so the
    parent only applies when the page is created.

    Give the parent as ``parent_id`` or, when only its title is known, as ``parent_title``,
    which is resolved within ``space``. Give at most one of the two.

    The return value, pushed to XCom, is a ``ConfluenceContent`` dictionary with the keys
    ``id``, ``type``, ``title``, ``space``, ``version``, ``url`` and ``action`` (``created``,
    ``updated`` or ``skipped``).

    Conflicting concurrent writes surface as ``requests.HTTPError`` with status 409; set
    ``retries`` on the task to have Airflow retry them.

    :param space: key of the space to publish into (templated)
    :param title: title of the page (templated)
    :param body: page body in storage format (templated)
    :param parent_id: content ID of the parent page, used on creation (templated)
    :param parent_title: title of the parent page, resolved within ``space`` (templated);
        mutually exclusive with ``parent_id``
    :param if_exists: ``update`` (default), ``skip`` or ``fail``
    :param version_comment: comment recorded against the new version on update (templated)
    :param confluence_conn_id: reference to a pre-defined Confluence Connection (templated)
    """

    template_fields: Sequence[str] = (
        "space",
        "title",
        "body",
        "parent_id",
        "parent_title",
        "version_comment",
        "confluence_conn_id",
    )
    template_fields_renderers = {"body": "html"}

    def __init__(
        self,
        *,
        space: str,
        title: str,
        body: str,
        parent_id: str | None = None,
        parent_title: str | None = None,
        if_exists: IfExists = "update",
        version_comment: str | None = None,
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        _check_if_exists(if_exists)
        self.space = space
        self.title = title
        self.body = body
        self.parent_id = parent_id
        self.parent_title = parent_title
        self.if_exists = if_exists
        self.version_comment = version_comment
        self.confluence_conn_id = confluence_conn_id

    def execute(self, context: Context) -> ConfluenceContent:
        hook = ConfluenceHook(confluence_conn_id=self.confluence_conn_id)
        return hook.publish(
            self.space,
            self.title,
            self.body,
            content_type="page",
            parent_id=self.parent_id,
            parent_title=self.parent_title,
            if_exists=self.if_exists,
            version_comment=self.version_comment,
        )


class ConfluenceBlogPostOperator(BaseOperator):
    """
    Publish a Confluence blog post.

    Blog posts are flat within a space, so unlike :class:`ConfluencePageOperator` there is no
    parent. The post is looked up by title within ``space``; a missing post is created and an
    existing one is handled according to ``if_exists``.

    .. warning::
        Blog post titles are not unique: a recurring post such as "Weekly report" usually
        reuses its title. With the default ``if_exists="update"`` each run overwrites the
        earlier post with that title. Put a date in the title (e.g. ``"Report {{ ds }}"``) to
        publish one post per run, or choose ``skip`` or ``fail`` deliberately.

    The return value, pushed to XCom, is a ``ConfluenceContent`` dictionary; see
    :class:`ConfluencePageOperator`.

    :param space: key of the space to publish into (templated)
    :param title: title of the blog post (templated)
    :param body: post body in storage format (templated)
    :param if_exists: ``update`` (default), ``skip`` or ``fail``
    :param version_comment: comment recorded against the new version on update (templated)
    :param confluence_conn_id: reference to a pre-defined Confluence Connection (templated)
    """

    template_fields: Sequence[str] = (
        "space",
        "title",
        "body",
        "version_comment",
        "confluence_conn_id",
    )
    template_fields_renderers = {"body": "html"}

    def __init__(
        self,
        *,
        space: str,
        title: str,
        body: str,
        if_exists: IfExists = "update",
        version_comment: str | None = None,
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        _check_if_exists(if_exists)
        self.space = space
        self.title = title
        self.body = body
        self.if_exists = if_exists
        self.version_comment = version_comment
        self.confluence_conn_id = confluence_conn_id

    def execute(self, context: Context) -> ConfluenceContent:
        hook = ConfluenceHook(confluence_conn_id=self.confluence_conn_id)
        return hook.publish(
            self.space,
            self.title,
            self.body,
            content_type="blogpost",
            if_exists=self.if_exists,
            version_comment=self.version_comment,
        )

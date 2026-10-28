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

from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.atlassian.confluence.hooks.confluence import ConfluenceHook
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class ConfluenceOperator(BaseOperator):
    """
    Call an arbitrary Atlassian Confluence SDK method.

    This is the escape hatch for anything the more focused operators do not cover: it
    exposes the whole SDK surface without a per-method wrapper. For details of the
    available methods see
    https://atlassian-python-api.readthedocs.io/confluence.html

    .. seealso::
        For creating or updating pages prefer :class:`ConfluencePageOperator`, which
        resolves parent pages and upserts by title.

    :param confluence_method: name of the method to call on the Confluence SDK client
    :param confluence_conn_id: reference to a pre-defined Confluence Connection
    :param confluence_method_args: keyword arguments for ``confluence_method`` (templated)
    :param result_processor: called with ``(context, result)`` to post-process the
        response; its return value becomes the operator's return value
    :param get_confluence_resource_method: callable (or another
        :class:`ConfluenceOperator`) returning the object to call ``confluence_method``
        on, instead of the top-level SDK client
    """

    template_fields: Sequence[str] = ("confluence_method_args", "confluence_conn_id")

    def __init__(
        self,
        *,
        confluence_method: str,
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        confluence_method_args: dict | None = None,
        result_processor: Callable | None = None,
        get_confluence_resource_method: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.confluence_conn_id = confluence_conn_id
        self.method_name = confluence_method
        self.confluence_method_args = confluence_method_args or {}
        self.result_processor = result_processor
        self.get_confluence_resource_method = get_confluence_resource_method

    def execute(self, context: Context) -> Any:
        if self.get_confluence_resource_method is not None:
            if isinstance(self.get_confluence_resource_method, ConfluenceOperator):
                resource = self.get_confluence_resource_method.execute(**context)
            else:
                resource = self.get_confluence_resource_method(**context)
        else:
            resource = ConfluenceHook(confluence_conn_id=self.confluence_conn_id).get_conn()

        result: Any = getattr(resource, self.method_name)(**self.confluence_method_args)

        if self.result_processor:
            return self.result_processor(context, result)
        return result


class ConfluencePageOperator(BaseOperator):
    """
    Publish a Confluence page, creating it or updating it in place if the title exists.

    The page can be placed under a parent identified either by ``parent_id`` or, when
    only its title is known, by ``parent_title`` — which is resolved against the pages
    of ``space``. Give at most one of the two.

    Conflicting concurrent writes surface as ``atlassian.errors.ApiConflictError``
    (HTTP 409); set ``retries`` on the task to have Airflow retry them.

    :param space: key of the space to publish into
    :param title: title of the page to create or update (templated)
    :param body: page body in the format given by ``representation`` (templated)
    :param confluence_conn_id: reference to a pre-defined Confluence Connection
    :param parent_id: content ID of the parent page (templated)
    :param parent_title: title of the parent page, resolved to an ID within ``space``
        (templated); mutually exclusive with ``parent_id``
    :param representation: body markup format, either ``storage`` or ``wiki``
    :param minor_edit: when set, watchers are not notified about the change
    :param version_comment: comment recorded against the new page version
    """

    template_fields: Sequence[str] = (
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
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        parent_id: str | None = None,
        parent_title: str | None = None,
        representation: str = "storage",
        minor_edit: bool = False,
        version_comment: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.confluence_conn_id = confluence_conn_id
        self.space = space
        self.title = title
        self.body = body
        self.parent_id = parent_id
        self.parent_title = parent_title
        self.representation = representation
        self.minor_edit = minor_edit
        self.version_comment = version_comment

    def execute(self, context: Context) -> dict[str, Any]:
        # Checked here rather than in __init__ because both fields are templated, so
        # before rendering they may still hold Jinja expressions.
        if self.parent_id and self.parent_title:
            raise ValueError("Give at most one of `parent_id` and `parent_title`, not both")

        hook = ConfluenceHook(confluence_conn_id=self.confluence_conn_id)
        parent_id = self.parent_id
        if self.parent_title:
            parent_id = hook.get_conn().get_page_id(space=self.space, title=self.parent_title)
            if not parent_id:
                raise ValueError(
                    f"Parent page titled {self.parent_title!r} was not found in space {self.space!r}"
                )

        result = hook.get_or_create_page(
            space=self.space,
            title=self.title,
            body=self.body,
            parent_id=parent_id,
            representation=self.representation,
            minor_edit=self.minor_edit,
            version_comment=self.version_comment,
        )
        self.log.info("Published Confluence page %r (id %s)", self.title, result.get("id"))
        return result


class ConfluenceBlogPostOperator(BaseOperator):
    """
    Publish a Confluence blog post, creating it or updating it in place if the title exists.

    Blog posts are flat within a space, so unlike :class:`ConfluencePageOperator` there
    is no parent to resolve.

    Conflicting concurrent writes surface as ``atlassian.errors.ApiConflictError``
    (HTTP 409); set ``retries`` on the task to have Airflow retry them.

    :param space: key of the space to publish into
    :param title: title of the blog post to create or update (templated)
    :param body: post body in the format given by ``representation`` (templated)
    :param confluence_conn_id: reference to a pre-defined Confluence Connection
    :param representation: body markup format, either ``storage`` or ``wiki``
    :param minor_edit: when set, watchers are not notified about the change
    :param version_comment: comment recorded against the new version
    """

    template_fields: Sequence[str] = ("title", "body", "version_comment", "confluence_conn_id")
    template_fields_renderers = {"body": "html"}

    def __init__(
        self,
        *,
        space: str,
        title: str,
        body: str,
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        representation: str = "storage",
        minor_edit: bool = False,
        version_comment: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.confluence_conn_id = confluence_conn_id
        self.space = space
        self.title = title
        self.body = body
        self.representation = representation
        self.minor_edit = minor_edit
        self.version_comment = version_comment

    def execute(self, context: Context) -> dict[str, Any]:
        hook = ConfluenceHook(confluence_conn_id=self.confluence_conn_id)
        result = hook.get_or_create_blog_post(
            space=self.space,
            title=self.title,
            body=self.body,
            representation=self.representation,
            minor_edit=self.minor_edit,
            version_comment=self.version_comment,
        )
        self.log.info("Published Confluence blog post %r (id %s)", self.title, result.get("id"))
        return result

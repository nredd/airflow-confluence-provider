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

from airflow.providers.atlassian.confluence.hooks.confluence import ConfluenceHook, ContentType
from airflow.providers.common.compat.sdk import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class ConfluenceSensor(BaseSensorOperator):
    """
    Wait for an arbitrary Atlassian Confluence SDK call to report readiness.

    Each poke calls ``method_name`` on ``ConfluenceHook.get_conn()`` with ``method_params``.
    Without a ``result_processor`` the raw response decides the outcome, so the sensor
    succeeds as soon as the call returns anything truthy. Pass ``result_processor`` to test
    the response for anything more specific.

    .. seealso::
        To wait for a page or blog post to exist use :class:`ConfluencePageSensor`.

    :param method_name: name of the method to call on the SDK client
    :param method_params: keyword arguments for ``method_name`` (templated)
    :param result_processor: called with ``(context, result)``; return a truthy value to
        stop poking
    :param confluence_conn_id: reference to a pre-defined Confluence Connection (templated)
    """

    template_fields: Sequence[str] = ("method_params", "confluence_conn_id")

    def __init__(
        self,
        *,
        method_name: str,
        method_params: dict | None = None,
        result_processor: Callable[[Context, Any], Any] | None = None,
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.method_name = method_name
        self.method_params = method_params or {}
        self.result_processor = result_processor
        self.confluence_conn_id = confluence_conn_id

    def poke(self, context: Context) -> Any:
        client = ConfluenceHook(confluence_conn_id=self.confluence_conn_id).get_conn()
        result = getattr(client, self.method_name)(**self.method_params)
        if self.result_processor is None:
            return result
        return self.result_processor(context, result)


class ConfluencePageSensor(BaseSensorOperator):
    """
    Wait for a Confluence page or blog post with the given title to exist.

    :param space: key of the space to look in (templated)
    :param title: exact title of the content (templated)
    :param content_type: ``page`` (default) or ``blogpost``
    :param confluence_conn_id: reference to a pre-defined Confluence Connection (templated)
    """

    template_fields: Sequence[str] = ("space", "title", "confluence_conn_id")

    def __init__(
        self,
        *,
        space: str,
        title: str,
        content_type: ContentType = "page",
        confluence_conn_id: str = ConfluenceHook.default_conn_name,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.space = space
        self.title = title
        self.content_type = content_type
        self.confluence_conn_id = confluence_conn_id

    def poke(self, context: Context) -> bool:
        hook = ConfluenceHook(confluence_conn_id=self.confluence_conn_id)
        return hook.find_content(self.space, self.title, content_type=self.content_type) is not None

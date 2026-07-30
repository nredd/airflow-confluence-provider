 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Atlassian Confluence Operators
==============================

The Confluence provider publishes pages and blog posts to Atlassian Confluence, and reaches
the rest of the Confluence REST API through a generic operator. It wraps the
`atlassian-python-api <https://atlassian-python-api.readthedocs.io/confluence.html>`__ SDK.

Before using any of these, set up a :ref:`Confluence connection <howto/connection:confluence>`.

.. _howto/operator:ConfluencePageOperator:

ConfluencePageOperator
----------------------

:class:`~airflow.providers.atlassian.confluence.operators.confluence.ConfluencePageOperator`
publishes a page, creating it or updating the existing page of the same title in place. Its
``body`` field is templated, which is the point: a Dag renders report content with Jinja and
the operator publishes it.

The page's position in the tree is set by either ``parent_id`` or — when only the parent's
title is known — ``parent_title``, which is resolved within ``space``. Give at most one of
the two; omit both to publish at the top level of the space.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_operator_confluence_page]
    :end-before: [END howto_operator_confluence_page]

Concurrent writes to the same page surface as ``atlassian.errors.ApiConflictError``
(HTTP 409). The operator does not retry them; set ``retries`` on the task instead, so
retry behaviour stays visible in the Dag rather than hidden in the operator.

.. _howto/operator:ConfluenceBlogPostOperator:

ConfluenceBlogPostOperator
--------------------------

:class:`~airflow.providers.atlassian.confluence.operators.confluence.ConfluenceBlogPostOperator`
does the same for blog posts. Blog posts are flat within a space, so it takes no parent.

.. code-block:: python

    publish_notes = ConfluenceBlogPostOperator(
        task_id="publish_notes",
        space="AIRFLOW",
        title="Release notes {{ ds }}",
        body="{{ ti.xcom_pull(task_ids='render_notes') }}",
    )

.. _howto/operator:ConfluenceOperator:

ConfluenceOperator
------------------

:class:`~airflow.providers.atlassian.confluence.operators.confluence.ConfluenceOperator`
calls any method on the SDK client, which makes the whole Confluence API reachable without a
dedicated operator per endpoint. ``confluence_method_args`` is templated.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_operator_confluence]
    :end-before: [END howto_operator_confluence]

.. _howto/sensor:ConfluencePageSensor:

ConfluencePageSensor
--------------------

:class:`~airflow.providers.atlassian.confluence.sensors.confluence.ConfluencePageSensor`
pokes an SDK method until it reports readiness. Without a ``result_processor`` the raw
response decides the outcome, which is enough to wait for a page to appear; pass
``result_processor`` to test the response for something more specific.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_sensor_confluence_page]
    :end-before: [END howto_sensor_confluence_page]

Building page bodies
--------------------

Confluence pages are written in `storage format
<https://confluence.atlassian.com/doc/confluence-storage-format-790796544.html>`__, an XHTML
dialect. The
:mod:`~airflow.providers.atlassian.confluence.utils.storage_format` module builds the common
constructs — tables, panels, expandable sections, code blocks and status lozenges — escaping
all caller-supplied text. The builders are plain functions with no Confluence dependency, so
a Dag's rendering logic can be unit-tested without a live instance.

.. code-block:: python

    from airflow.providers.atlassian.confluence.utils.storage_format import (
        build_status_lozenge,
        build_table,
    )

    body = build_table(
        ["Check", "Status"],
        [["Ingest", build_status_lozenge("Done", color="green")]],
        cell_highlight_colors={(0, 1): "#e3fcef"},
    )

Confluence strips most inline styling when a page is saved, so ``build_table`` exposes only
the per-cell ``data-highlight-colour`` attribute — which does survive a round-trip — rather
than a general ``style`` passthrough that would silently be discarded.

Using the hook directly
-----------------------

:class:`~airflow.providers.atlassian.confluence.hooks.confluence.ConfluenceHook` wraps the
page, label, comment, attachment, content-property and restriction endpoints, adds CQL
search via
:meth:`~airflow.providers.atlassian.confluence.hooks.confluence.ConfluenceHook.search_cql`,
and covers Cloud v2 folders, databases and whiteboards. The client is built lazily, so
constructing the hook performs no I/O.

.. code-block:: python

    @task
    def find_stale_pages() -> list[dict]:
        hook = ConfluenceHook()
        results = hook.search_cql('space="AIRFLOW" and lastModified < now("-30d")')
        return results["results"]

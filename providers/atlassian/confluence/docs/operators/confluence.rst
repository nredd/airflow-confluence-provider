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

The Confluence provider publishes pages and blog posts to Atlassian Confluence, waits for
content to appear, and reaches the rest of the Confluence REST API through a generic operator
and the hook. It works with both Confluence Cloud and Data Center and wraps the
`atlassian-python-api <https://atlassian-python-api.readthedocs.io/confluence.html>`__ SDK.

Before using any of these, set up a :ref:`Confluence connection <howto/connection:confluence>`.

.. _howto/operator:ConfluencePageOperator:

ConfluencePageOperator
----------------------

:class:`~airflow.providers.atlassian.confluence.operators.confluence.ConfluencePageOperator`
publishes a page. Its ``space``, ``title``, ``body``, ``parent_id``, ``parent_title`` and
``version_comment`` fields are templated, so a Dag renders report content with Jinja and the
operator publishes it.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_operator_confluence_page]
    :end-before: [END howto_operator_confluence_page]

Publishing semantics:

- The page is looked up by title within ``space``. A missing page is created.
- An existing page is **updated in place**, wherever it currently lives in the page tree. The
  parent only applies when the page is created; it never moves an existing page.
- ``if_exists`` chooses what happens to an existing page: ``update`` (default) writes a new
  version, ``skip`` leaves it untouched, and ``fail`` raises
  :class:`~airflow.providers.atlassian.confluence.exceptions.ConfluenceContentExistsError`.
  An invalid value raises ``ValueError`` when the operator is constructed.
- The parent is given as ``parent_id`` or, when only its title is known, as ``parent_title``,
  which is resolved within the same space (a missing parent raises
  :class:`~airflow.providers.atlassian.confluence.exceptions.ConfluenceContentNotFoundError`).
  Give at most one of the two; omit both to create the page at the top level of the space.
- ``version_comment`` is recorded against the new version when the page is updated.

The operator returns a small dictionary, which is what ends up in XCom, rather than the full
API response:

.. code-block:: python

    {
        "id": "123456",
        "type": "page",
        "title": "Report",
        "space": "AIRFLOW",
        "version": 4,
        "url": "https://example.atlassian.net/wiki/spaces/AIRFLOW/pages/123456",
        "action": "updated",  # "created", "updated" or "skipped"
    }

Concurrent writes to the same page surface as ``requests.HTTPError`` with
``response.status_code == 409``. The operator does not retry them; set ``retries`` on the task instead, so retry
behavior stays visible in the Dag rather than hidden in the operator.

.. _howto/operator:ConfluenceBlogPostOperator:

ConfluenceBlogPostOperator
--------------------------

:class:`~airflow.providers.atlassian.confluence.operators.confluence.ConfluenceBlogPostOperator`
does the same for blog posts and returns the same dictionary. Blog posts are flat within a
space, so it takes no parent.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_operator_confluence_blog_post]
    :end-before: [END howto_operator_confluence_blog_post]

Blog post titles tend to repeat, e.g. a weekly "Status report". With the default
``if_exists="update"`` every run overwrites the earlier post that has the same title. Put a
date in the title (``"Status report {{ ds }}"``) to publish one post per run, or choose
``skip`` or ``fail`` deliberately.

.. _howto/operator:ConfluenceOperator:

ConfluenceOperator
------------------

:class:`~airflow.providers.atlassian.confluence.operators.confluence.ConfluenceOperator`
calls any method on the SDK client, which makes the rest of the Confluence API reachable
without a dedicated operator per endpoint. ``confluence_method_args`` is templated, and
``result_processor`` is called with ``(context, result)``.

The client is a ``ConfluenceV2`` on Cloud and a ``ConfluenceServer`` on Data Center, so the
available methods and their arguments differ between deployments; see the SDK documentation.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_operator_confluence]
    :end-before: [END howto_operator_confluence]

The result is pushed to XCom. Methods that return generators or bytes, such as page exports,
cannot be stored there: pass a ``result_processor`` that turns the result into something
serializable, or set ``do_xcom_push=False``.

.. _howto/sensor:ConfluencePageSensor:

ConfluencePageSensor
--------------------

:class:`~airflow.providers.atlassian.confluence.sensors.confluence.ConfluencePageSensor`
succeeds as soon as a page (or, with ``content_type="blogpost"``, a blog post) with the given
title exists in the space.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_sensor_confluence_page]
    :end-before: [END howto_sensor_confluence_page]

.. _howto/sensor:ConfluenceSensor:

ConfluenceSensor
----------------

:class:`~airflow.providers.atlassian.confluence.sensors.confluence.ConfluenceSensor` is the
generic counterpart of ``ConfluenceOperator``: each poke calls an SDK method on the client with
``method_params``. Without a ``result_processor`` any truthy response ends the wait. With one,
it is called with ``(context, result)`` and decides.

Building page bodies
--------------------

Confluence pages are written in `storage format
<https://confluence.atlassian.com/doc/confluence-storage-format-790796544.html>`__, an XHTML
dialect. The
:mod:`~airflow.providers.atlassian.confluence.utils.storage_format` module builds the common
constructs: tables, panels, expandable sections, code blocks and status lozenges. The builders
are plain functions with no Confluence dependency, so a Dag's rendering logic can be
unit-tested without a live instance.

Every builder returns a
:class:`~airflow.providers.atlassian.confluence.utils.storage_format.StorageFormat`, a ``str``
subclass that marks markup as already escaped. Text and body arguments go through
:func:`~airflow.providers.atlassian.confluence.utils.storage_format.escape_text`, which escapes
plain strings (and strips characters that are not valid in XML) but leaves ``StorageFormat``
untouched. This is what makes the builders composable: output from one can be passed to any
other, without being escaped twice.

.. exampleinclude:: /../tests/system/atlassian/confluence/example_confluence_page.py
    :language: python
    :start-after: [START howto_storage_format_builders]
    :end-before: [END howto_storage_format_builders]

To mix your own markup with the builders, wrap it in ``StorageFormat`` yourself, and only when
you know it is safe (see :doc:`../security`).

Confluence strips most inline styling when a page is saved, so ``build_table`` exposes only the
per-cell ``data-highlight-colour`` attribute, which does survive a round-trip, rather than a
general ``style`` passthrough that would silently be discarded.

Using the hook directly
-----------------------

:class:`~airflow.providers.atlassian.confluence.hooks.confluence.ConfluenceHook` has a small
surface: it owns the operations that need provider logic, and leaves everything else to the SDK
client returned by ``get_conn()``.

- ``find_content(space, title, content_type="page")`` returns the content summary or ``None``.
  Nothing is written, so ``action`` is always ``skipped``.
- ``publish(...)`` is what the operators call, with the semantics described above.
- ``delete_content(content_id, content_type="page")`` deletes a page or blog post.
- ``search_cql(cql, limit=25)`` runs a CQL search and returns the ``results`` list.
- ``test_connection()`` checks the credentials and backs the *Test* button in the UI.
- ``is_cloud`` tells which deployment the connection points at.

The client is built lazily, so constructing the hook performs no I/O.

.. code-block:: python

    @task
    def find_stale_pages() -> list[str]:
        hook = ConfluenceHook()
        results = hook.search_cql('space = "AIRFLOW" and lastModified < now("-30d")')
        return [result["title"] for result in results]


    @task
    def list_spaces() -> list[str]:
        # Anything else goes through the SDK client, here as a raw REST call.
        client = ConfluenceHook().get_conn()
        return [space["key"] for space in client.get("rest/api/space", params={"limit": 50})["results"]]

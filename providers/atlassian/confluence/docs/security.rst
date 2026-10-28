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

.. include:: /../../../../devel-common/src/sphinx_exts/includes/security.rst

Untrusted values in page bodies
-------------------------------

``ConfluencePageOperator`` and ``ConfluenceBlogPostOperator`` publish ``body`` as Confluence
storage format, an XHTML dialect that can contain macros. Several of them, such as ``include``
and ``html``, can pull in other pages or render arbitrary markup. Jinja does not escape
templated values, so interpolating untrusted text into ``body`` lets whoever controls that text
inject markup into the page.

Build the body with the helpers in
:mod:`airflow.providers.atlassian.confluence.utils.storage_format`, which escape every value
they receive, or escape values yourself with the Jinja ``e`` filter:

.. code-block:: python

    ConfluencePageOperator(
        task_id="publish",
        space="AIRFLOW",
        title="Report {{ ds }}",
        body="<p>Triggered by {{ dag_run.conf.get('requester', 'unknown') | e }}</p>",
    )

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

.. _howto/connection:confluence:

Atlassian Confluence Connection
===============================

The Confluence connection type enables connection to Atlassian Confluence, both Cloud and
Data Center.

Default Connection IDs
----------------------

Confluence Hook uses the parameter ``confluence_conn_id`` for Connection IDs and the value
of the parameter as ``confluence_default`` by default.

Authentication
--------------

How you authenticate depends on the deployment:

Confluence Cloud
    Cloud API tokens use basic authentication. Put the Atlassian account **email** in
    *Login* and the `API token <https://id.atlassian.com/manage-profile/security/api-tokens>`__
    in *Password*. The ``token`` extra is *not* accepted on Cloud: it sends a Bearer header, which
    Cloud rejects for API tokens, so the hook raises an error instead of failing later with a 401.

Confluence Data Center
    Either a `Personal Access Token
    <https://confluence.atlassian.com/enterprise/using-personal-access-tokens-1026032365.html>`__
    in the ``token`` extra, or *Login* and *Password* for basic authentication. When both are
    set, the token takes precedence.

Configuring the Connection
--------------------------

Host
    The full base URL of the Confluence site, including the scheme and any context path, for
    example ``https://example.atlassian.net/wiki`` for Cloud or
    ``https://confluence.example.com/confluence`` for Data Center. Without a scheme, ``https``
    is assumed. Trailing slashes are ignored.

Port
    Optional port. It is applied only when the host URL does not already contain one, so a
    port in the host always wins. IPv6 addresses work either way
    (``https://[2001:db8::1]/confluence``).

Login
    The account email on Cloud, or the username for basic authentication on Data Center.

Password
    The API token on Cloud, or the password for basic authentication on Data Center.

Schema
    Not used. The field is hidden in the UI and is ignored when set, e.g. through a connection
    URI. Put the scheme in *Host* instead.

Deployment (optional)
    The ``deployment`` extra: ``auto`` (default), ``cloud`` or ``data_center``. With ``auto``, a
    host that is, or ends with, ``atlassian.net``, ``jira.com`` or ``api.atlassian.com`` is
    treated as Cloud and everything else as Data Center. Set ``cloud`` explicitly for a Cloud
    site served from a custom domain. Data Center uses the Confluence REST API v1 and Cloud
    uses REST API v2.

Data Center Personal Access Token (optional)
    The ``token`` extra. Data Center only, see above.

Verify SSL (optional)
    The ``verify`` extra: ``true`` (default) or ``false``, as a JSON boolean or the string
    ``"true"`` / ``"false"`` (case-insensitive). Any other value is rejected. Custom CA bundles
    are not configurable here; use the standard ``REQUESTS_CA_BUNDLE`` environment variable.

Examples
--------

A Cloud connection, as an environment variable:

.. code-block:: bash

    export AIRFLOW_CONN_CONFLUENCE_DEFAULT='{
        "conn_type": "confluence",
        "host": "https://example.atlassian.net/wiki",
        "login": "me@example.com",
        "password": "your-api-token"
    }'

A Data Center connection using a Personal Access Token:

.. code-block:: bash

    export AIRFLOW_CONN_CONFLUENCE_DEFAULT='{
        "conn_type": "confluence",
        "host": "https://confluence.example.com/confluence",
        "extra": {"token": "your-personal-access-token"}
    }'

A Data Center connection using basic authentication on a non-default port:

.. code-block:: bash

    export AIRFLOW_CONN_CONFLUENCE_DEFAULT='{
        "conn_type": "confluence",
        "host": "https://confluence.example.com/confluence",
        "port": 8443,
        "login": "airflow",
        "password": "your-password"
    }'

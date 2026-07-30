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

The Confluence connection type enables connection to Atlassian Confluence, both Cloud
and Data Center.

Default Connection IDs
----------------------

Confluence Hook uses the parameter ``confluence_conn_id`` for Connection IDs and the value
of the parameter as ``confluence_default`` by default.

Configuring the Connection
--------------------------
Host
    The Confluence host. A scheme may be included (``https://confluence.example.com``); when
    it is omitted, ``https`` is assumed. Any path is preserved, so a Data Center instance
    served under a context path such as ``confluence.example.com/wiki`` works as written.

Port
    Optional port to connect to. A port already present in the host wins over this field.

Login
    The user authenticating against the Confluence API. Required unless ``token`` is set.

Password
    The password of that user. Required unless ``token`` is set.

Verify SSL
    Whether to verify SSL when connecting to the Confluence API (``True`` by default).

API token / Personal Access Token
    A Confluence Cloud API token or a Data Center Personal Access Token, stored in the
    connection's ``extra`` field as ``token``. When set, it is used as the sole credential
    and takes precedence over Login and Password.

Confluence Cloud
    Set for Confluence Cloud instances, stored in ``extra`` as ``cloud``. Atlassian-hosted
    URLs (``*.atlassian.net``) are detected automatically, so this is only needed for a
    Cloud instance served from a custom domain.

Examples
--------

A Cloud connection authenticating with an API token, as an environment variable:

.. code-block:: bash

    export AIRFLOW_CONN_CONFLUENCE_DEFAULT='{
        "conn_type": "confluence",
        "host": "https://your-domain.atlassian.net",
        "extra": {"token": "your-api-token", "cloud": true}
    }'

A Data Center connection using basic authentication:

.. code-block:: bash

    export AIRFLOW_CONN_CONFLUENCE_DEFAULT='{
        "conn_type": "confluence",
        "host": "https://confluence.example.com",
        "login": "airflow",
        "password": "your-password"
    }'

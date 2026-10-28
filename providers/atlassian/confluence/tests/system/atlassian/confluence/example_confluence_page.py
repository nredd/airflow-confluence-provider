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
"""Example Dag publishing a formatted report page and a blog post to Confluence."""

from __future__ import annotations

import os

from airflow.providers.atlassian.confluence.hooks.confluence import ConfluenceHook
from airflow.providers.atlassian.confluence.operators.confluence import (
    ConfluenceBlogPostOperator,
    ConfluenceOperator,
    ConfluencePageOperator,
)
from airflow.providers.atlassian.confluence.sensors.confluence import ConfluencePageSensor
from airflow.providers.atlassian.confluence.utils.storage_format import (
    build_panel_macro,
    build_status_lozenge,
    build_table,
)
from airflow.providers.common.compat.sdk import TriggerRule
from airflow.sdk import DAG, task

CONFLUENCE_CONN_ID = os.environ.get("CONFLUENCE_CONN_ID", "confluence_default")
CONFLUENCE_SPACE = os.environ.get("CONFLUENCE_SPACE", "AIRFLOW")

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_confluence_page"
PAGE_TITLE = f"Airflow example report ({ENV_ID})"
BLOG_POST_TITLE = f"Airflow example post ({ENV_ID})"

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    catchup=False,
    tags=["example"],
) as dag:
    # [START howto_storage_format_builders]
    @task
    def build_report_body() -> str:
        """Compose storage-format builders: a lozenge inside a table inside a panel."""
        table = build_table(
            ["Check", "Status"],
            [
                ["Ingest", build_status_lozenge("Done", color="green")],
                ["Transform", build_status_lozenge("Pending", color="yellow")],
            ],
            cell_highlight_colors={(0, 1): "#e3fcef"},
        )
        return build_panel_macro(table, panel_type="info", title="Pipeline checks")

    # [END howto_storage_format_builders]

    body = build_report_body()

    # [START howto_operator_confluence_page]
    publish_report = ConfluencePageOperator(
        task_id="publish_report",
        confluence_conn_id=CONFLUENCE_CONN_ID,
        space=CONFLUENCE_SPACE,
        title=PAGE_TITLE,
        body=body,
        if_exists="update",
        version_comment="Published by Airflow",
    )
    # [END howto_operator_confluence_page]

    # [START howto_sensor_confluence_page]
    wait_for_page = ConfluencePageSensor(
        task_id="wait_for_page",
        confluence_conn_id=CONFLUENCE_CONN_ID,
        space=CONFLUENCE_SPACE,
        title=PAGE_TITLE,
        poke_interval=10,
        timeout=120,
    )
    # [END howto_sensor_confluence_page]

    # [START howto_operator_confluence]
    get_report = ConfluenceOperator(
        task_id="get_report",
        confluence_conn_id=CONFLUENCE_CONN_ID,
        confluence_method="get_page_by_id",
        confluence_method_args={
            "page_id": "{{ ti.xcom_pull(task_ids='publish_report')['id'] }}",
        },
        do_xcom_push=False,
    )
    # [END howto_operator_confluence]

    # [START howto_operator_confluence_blog_post]
    publish_post = ConfluenceBlogPostOperator(
        task_id="publish_post",
        confluence_conn_id=CONFLUENCE_CONN_ID,
        space=CONFLUENCE_SPACE,
        title=BLOG_POST_TITLE,
        body="<p>Published by the Airflow Confluence provider example Dag.</p>",
        if_exists="update",
    )
    # [END howto_operator_confluence_blog_post]

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_page() -> None:
        hook = ConfluenceHook(confluence_conn_id=CONFLUENCE_CONN_ID)
        page = hook.find_content(CONFLUENCE_SPACE, PAGE_TITLE)
        if page:
            hook.delete_content(page["id"])

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def delete_blog_post() -> None:
        hook = ConfluenceHook(confluence_conn_id=CONFLUENCE_CONN_ID)
        post = hook.find_content(CONFLUENCE_SPACE, BLOG_POST_TITLE, content_type="blogpost")
        if post:
            hook.delete_content(post["id"], content_type="blogpost")

    publish_report >> wait_for_page >> get_report >> publish_post >> [delete_page(), delete_blog_post()]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)

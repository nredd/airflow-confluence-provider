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

import pytest

from airflow.providers.atlassian.confluence.utils.storage_format import (
    StorageFormat,
    build_code_block_macro,
    build_expand_macro,
    build_panel_macro,
    build_status_lozenge,
    build_table,
    escape_text,
)


class TestEscapeText:
    def test_plain_text_is_escaped(self):
        result = escape_text('<a href="x">\'&</a>')

        assert result == "&lt;a href=&quot;x&quot;&gt;&#x27;&amp;&lt;/a&gt;"
        assert isinstance(result, StorageFormat)

    def test_storage_format_is_returned_untouched(self):
        markup = StorageFormat("<b>bold</b>")

        assert escape_text(markup) is markup

    def test_non_strings_are_converted(self):
        assert escape_text(42) == "42"

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [
            ("a\x00b\x08c\x0bd\x0ce\x1ff", "abcdef"),
            ("tab\tnewline\ncr\r", "tab\tnewline\ncr\r"),
            ("a\ufffe\uffffb", "ab"),
            ("a\ud800b", "ab"),
        ],
    )
    def test_xml_invalid_characters_are_stripped(self, raw, expected):
        assert escape_text(raw) == expected

    def test_storage_format_supports_the_html_protocol(self):
        markup = StorageFormat("<b>bold</b>")

        assert markup.__html__() is markup


class TestBuildTable:
    def test_headers_and_rows(self):
        assert build_table(["A", "B"], [["1", "2"], ["3", "4"]]) == (
            "<table><tbody>"
            "<tr><th>A</th><th>B</th></tr>"
            "<tr><td>1</td><td>2</td></tr>"
            "<tr><td>3</td><td>4</td></tr>"
            "</tbody></table>"
        )

    def test_headers_are_omitted_when_empty(self):
        assert build_table([], [["1"]]) == "<table><tbody><tr><td>1</td></tr></tbody></table>"

    def test_rows_may_be_empty(self):
        assert build_table(["A"], []) == "<table><tbody><tr><th>A</th></tr></tbody></table>"

    def test_cell_highlights_target_body_cells_by_coordinate(self):
        assert build_table(
            ["A", "B"],
            [["1", "2"], ["3", "4"]],
            cell_highlight_colors={(1, 0): "#e3fcef"},
        ) == (
            "<table><tbody>"
            "<tr><th>A</th><th>B</th></tr>"
            "<tr><td>1</td><td>2</td></tr>"
            '<tr><td data-highlight-colour="#e3fcef">3</td><td>4</td></tr>'
            "</tbody></table>"
        )

    def test_text_is_escaped(self):
        assert build_table(["<h&>"], [['a"b']]) == (
            "<table><tbody><tr><th>&lt;h&amp;&gt;</th></tr><tr><td>a&quot;b</td></tr></tbody></table>"
        )

    def test_highlight_colour_drops_xml_invalid_characters(self):
        table = build_table(["A"], [["1"]], cell_highlight_colors={(0, 0): "#e3\x00fcef"})

        assert 'data-highlight-colour="#e3fcef"' in table

    def test_builder_output_nests_in_cells_without_double_escaping(self):
        table = build_table(["Check"], [[build_status_lozenge("Done", color="green")]])

        assert "<td><ac:structured-macro" in table
        assert "&lt;" not in table

    def test_highlight_colour_is_quoted_safely(self):
        table = build_table(["A"], [["1"]], cell_highlight_colors={(0, 0): 'red"; x'})

        assert "data-highlight-colour='red\"; x'" in table

    def test_rows_accept_any_iterable(self):
        assert build_table([], iter([["1"]])) == "<table><tbody><tr><td>1</td></tr></tbody></table>"


class TestBuildPanelMacro:
    @pytest.mark.parametrize("panel_type", ["info", "note", "warning", "tip"])
    def test_each_panel_type_emits_its_own_macro(self, panel_type):
        assert build_panel_macro("body", panel_type=panel_type) == (
            f'<ac:structured-macro ac:name="{panel_type}">'
            "<ac:rich-text-body>body</ac:rich-text-body>"
            "</ac:structured-macro>"
        )

    def test_title_is_emitted_as_a_parameter(self):
        assert build_panel_macro("body", panel_type="info", title="Heads up") == (
            '<ac:structured-macro ac:name="info">'
            '<ac:parameter ac:name="title">Heads up</ac:parameter>'
            "<ac:rich-text-body>body</ac:rich-text-body>"
            "</ac:structured-macro>"
        )

    def test_body_and_title_are_escaped(self):
        panel = build_panel_macro("a<b", panel_type="info", title="x&y")

        assert "a&lt;b" in panel
        assert "x&amp;y" in panel

    def test_builder_output_nests_in_the_body(self):
        table = build_table(["A"], [["1"]])

        panel = build_panel_macro(table, panel_type="note")

        assert f"<ac:rich-text-body>{table}</ac:rich-text-body>" in panel


class TestBuildExpandMacro:
    def test_builder_output_nests_in_the_body(self):
        panel = build_panel_macro("inner", panel_type="info")

        assert f"<ac:rich-text-body>{panel}</ac:rich-text-body>" in build_expand_macro("More", panel)

    def test_title_and_body(self):
        assert build_expand_macro("More", "hidden") == (
            '<ac:structured-macro ac:name="expand">'
            '<ac:parameter ac:name="title">More</ac:parameter>'
            "<ac:rich-text-body>hidden</ac:rich-text-body>"
            "</ac:structured-macro>"
        )


class TestBuildCodeBlockMacro:
    def test_code_goes_in_a_cdata_section(self):
        assert build_code_block_macro("print('hi')") == (
            '<ac:structured-macro ac:name="code">'
            "<ac:plain-text-body><![CDATA[print('hi')]]></ac:plain-text-body>"
            "</ac:structured-macro>"
        )

    def test_language_and_title_are_emitted_as_parameters(self):
        assert build_code_block_macro("x = 1", language="python", title="Snippet") == (
            '<ac:structured-macro ac:name="code">'
            '<ac:parameter ac:name="language">python</ac:parameter>'
            '<ac:parameter ac:name="title">Snippet</ac:parameter>'
            "<ac:plain-text-body><![CDATA[x = 1]]></ac:plain-text-body>"
            "</ac:structured-macro>"
        )

    def test_cdata_terminator_in_code_is_split_across_sections(self):
        assert "<![CDATA[a]]]]><![CDATA[>b]]>" in build_code_block_macro("a]]>b")

    def test_xml_invalid_characters_are_stripped_from_code(self):
        assert "<![CDATA[ab]]>" in build_code_block_macro("a\x00b")

    def test_code_itself_is_not_xml_escaped(self):
        assert "if a < b:" in build_code_block_macro("if a < b:")


class TestBuildStatusLozenge:
    def test_colour_is_title_cased_for_confluence(self):
        assert build_status_lozenge("Done", color="green") == (
            '<ac:structured-macro ac:name="status">'
            '<ac:parameter ac:name="colour">Green</ac:parameter>'
            '<ac:parameter ac:name="title">Done</ac:parameter>'
            "</ac:structured-macro>"
        )

    def test_every_builder_returns_storage_format(self):
        results = [
            build_table(["A"], [["1"]]),
            build_panel_macro("b", panel_type="info"),
            build_expand_macro("t", "b"),
            build_code_block_macro("x"),
            build_status_lozenge("s"),
        ]

        assert all(isinstance(result, StorageFormat) for result in results)

    def test_colour_defaults_to_grey(self):
        assert '<ac:parameter ac:name="colour">Grey</ac:parameter>' in build_status_lozenge("Pending")

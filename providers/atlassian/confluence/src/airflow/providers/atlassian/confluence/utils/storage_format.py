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
"""
Builders for Confluence storage-format (XHTML) page bodies.

These helpers are plain string builders with no dependency on the Confluence SDK or
on an Airflow Connection, so page bodies can be assembled and asserted on without a
live Confluence instance. All caller-supplied text is XML-escaped.

Confluence discards most inline styling when a page is saved, so the builders expose
only attributes that survive a round-trip — for tables, that is the per-cell
``data-highlight-colour`` attribute rather than a general ``style`` passthrough.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Literal
from xml.sax.saxutils import escape, quoteattr

__all__ = [
    "build_code_block_macro",
    "build_expand_macro",
    "build_panel_macro",
    "build_status_lozenge",
    "build_table",
]

PanelType = Literal["info", "note", "warning", "tip"]
LozengeColor = Literal["grey", "red", "yellow", "green", "blue"]


def _build_macro_parameter(name: str, value: str) -> str:
    return f"<ac:parameter ac:name={quoteattr(name)}>{escape(value)}</ac:parameter>"


def _build_rich_text_body(body: str) -> str:
    return f"<ac:rich-text-body>{escape(body)}</ac:rich-text-body>"


def build_table(
    headers: Sequence[str],
    rows: Iterable[Sequence[str]],
    *,
    cell_highlight_colors: Mapping[tuple[int, int], str] | None = None,
) -> str:
    """
    Build a storage-format table.

    :param headers: header cell texts, rendered as a ``<th>`` row
    :param rows: body rows, each a sequence of cell texts
    :param cell_highlight_colors: maps ``(row_index, column_index)`` to a background
        color (e.g. ``"#e3fcef"``), applied as ``data-highlight-colour`` on that body
        cell. Row and column indexes are zero-based and count body rows only, so the
        header row is not addressable.
    :return: the table as a storage-format XHTML string
    """
    highlights = cell_highlight_colors or {}

    parts = ["<table><tbody>"]
    if headers:
        header_cells = "".join(f"<th>{escape(header)}</th>" for header in headers)
        parts.append(f"<tr>{header_cells}</tr>")

    for row_index, row in enumerate(rows):
        cells = []
        for column_index, cell in enumerate(row):
            color = highlights.get((row_index, column_index))
            attribute = f" data-highlight-colour={quoteattr(color)}" if color else ""
            cells.append(f"<td{attribute}>{escape(cell)}</td>")
        parts.append(f"<tr>{''.join(cells)}</tr>")

    parts.append("</tbody></table>")
    return "".join(parts)


def build_panel_macro(body: str, *, panel_type: PanelType, title: str | None = None) -> str:
    """
    Build an ``info``, ``note``, ``warning`` or ``tip`` panel macro.

    :param body: panel body text
    :param panel_type: which of the four panel macros to emit
    :param title: optional panel title
    :return: the panel as a storage-format XHTML string
    """
    parameters = _build_macro_parameter("title", title) if title else ""
    return (
        f"<ac:structured-macro ac:name={quoteattr(panel_type)}>"
        f"{parameters}{_build_rich_text_body(body)}"
        "</ac:structured-macro>"
    )


def build_expand_macro(title: str, body: str) -> str:
    """
    Build an expand macro — a collapsed block that reveals ``body`` when clicked.

    :param title: the always-visible summary line
    :param body: the content revealed on expansion
    :return: the expand macro as a storage-format XHTML string
    """
    return (
        '<ac:structured-macro ac:name="expand">'
        f"{_build_macro_parameter('title', title)}{_build_rich_text_body(body)}"
        "</ac:structured-macro>"
    )


def build_code_block_macro(code: str, *, language: str | None = None, title: str | None = None) -> str:
    """
    Build a code block macro.

    :param code: the code to render, emitted inside a ``CDATA`` section
    :param language: syntax-highlighting language, e.g. ``python``
    :param title: optional title shown above the block
    :return: the code block as a storage-format XHTML string
    """
    parameters = ""
    if language:
        parameters += _build_macro_parameter("language", language)
    if title:
        parameters += _build_macro_parameter("title", title)

    # A CDATA section cannot contain "]]>", so split any occurrence across two sections.
    body = code.replace("]]>", "]]]]><![CDATA[>")
    return (
        '<ac:structured-macro ac:name="code">'
        f"{parameters}"
        f"<ac:plain-text-body><![CDATA[{body}]]></ac:plain-text-body>"
        "</ac:structured-macro>"
    )


def build_status_lozenge(text: str, *, color: LozengeColor = "grey") -> str:
    """
    Build a status lozenge — the small colored status pill.

    :param text: the lozenge label
    :param color: lozenge color
    :return: the lozenge as a storage-format XHTML string
    """
    return (
        '<ac:structured-macro ac:name="status">'
        f"{_build_macro_parameter('colour', color.title())}"
        f"{_build_macro_parameter('title', text)}"
        "</ac:structured-macro>"
    )

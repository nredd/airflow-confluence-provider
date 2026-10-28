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

These helpers have no dependency on the Confluence SDK or on an Airflow Connection, so
page bodies can be assembled and asserted on without a live Confluence instance.

Every builder returns a :class:`StorageFormat`, a ``str`` subclass marking markup that is
already escaped. Every text or body argument goes through :func:`escape_text`, which
escapes plain strings and leaves :class:`StorageFormat` untouched, so builder output nests
inside any other builder (a lozenge in a table cell, a table in a panel) without being
escaped twice.

Confluence discards most inline styling when a page is saved, so the builders expose
only attributes that survive a round-trip -- for tables, that is the per-cell
``data-highlight-colour`` attribute rather than a general ``style`` passthrough.
"""

from __future__ import annotations

import html
import re
from collections.abc import Iterable, Mapping, Sequence
from typing import Literal
from xml.sax.saxutils import quoteattr

__all__ = [
    "StorageFormat",
    "build_code_block_macro",
    "build_expand_macro",
    "build_panel_macro",
    "build_status_lozenge",
    "build_table",
    "escape_text",
]

PanelType = Literal["info", "note", "warning", "tip"]
LozengeColor = Literal["grey", "red", "yellow", "green", "blue"]

# Characters that are not legal in XML 1.0 (tab, newline and carriage return are).
# Confluence rejects a page body containing them with HTTP 400.
_INVALID_XML_CHARS = re.compile("[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]")


class StorageFormat(str):
    """A ``str`` holding storage-format markup that must not be escaped again."""

    __slots__ = ()

    def __html__(self) -> StorageFormat:
        """Support the ``__html__`` protocol used by Jinja and markupsafe."""
        return self


def _strip_invalid_xml_chars(value: str) -> str:
    return _INVALID_XML_CHARS.sub("", value)


def escape_text(value: object) -> StorageFormat:
    """
    Escape ``value`` for use as storage-format text, unless it is already markup.

    :param value: a :class:`StorageFormat` (returned untouched) or any other value, which
        is converted with ``str`` and XML-escaped after stripping characters XML forbids
    :return: markup that is safe to embed in a storage-format document
    """
    if isinstance(value, StorageFormat):
        return value
    return StorageFormat(html.escape(_strip_invalid_xml_chars(str(value)), quote=True))


def _build_macro_parameter(name: str, value: str) -> str:
    return f"<ac:parameter ac:name={quoteattr(name)}>{escape_text(value)}</ac:parameter>"


def _build_rich_text_body(body: str) -> str:
    return f"<ac:rich-text-body>{escape_text(body)}</ac:rich-text-body>"


def build_table(
    headers: Sequence[str],
    rows: Iterable[Sequence[str]],
    *,
    cell_highlight_colors: Mapping[tuple[int, int], str] | None = None,
) -> StorageFormat:
    """
    Build a storage-format table.

    :param headers: header cell texts, rendered as a ``<th>`` row
    :param rows: body rows, each a sequence of cell texts; a cell may be the output of
        another builder
    :param cell_highlight_colors: maps ``(row_index, column_index)`` to a background
        color (e.g. ``"#e3fcef"``), applied as ``data-highlight-colour`` on that body
        cell. Row and column indexes are zero-based and count body rows only, so the
        header row is not addressable.
    :return: the table as storage-format markup
    """
    highlights = cell_highlight_colors or {}

    parts = ["<table><tbody>"]
    if headers:
        header_cells = "".join(f"<th>{escape_text(header)}</th>" for header in headers)
        parts.append(f"<tr>{header_cells}</tr>")

    for row_index, row in enumerate(rows):
        cells = []
        for column_index, cell in enumerate(row):
            color = highlights.get((row_index, column_index))
            attribute = (
                f" data-highlight-colour={quoteattr(_strip_invalid_xml_chars(color))}" if color else ""
            )
            cells.append(f"<td{attribute}>{escape_text(cell)}</td>")
        parts.append(f"<tr>{''.join(cells)}</tr>")

    parts.append("</tbody></table>")
    return StorageFormat("".join(parts))


def build_panel_macro(body: str, *, panel_type: PanelType, title: str | None = None) -> StorageFormat:
    """
    Build an ``info``, ``note``, ``warning`` or ``tip`` panel macro.

    :param body: panel body text, or the output of other builders
    :param panel_type: which of the four panel macros to emit
    :param title: optional panel title
    :return: the panel as storage-format markup
    """
    parameters = _build_macro_parameter("title", title) if title else ""
    return StorageFormat(
        f"<ac:structured-macro ac:name={quoteattr(panel_type)}>"
        f"{parameters}{_build_rich_text_body(body)}"
        "</ac:structured-macro>"
    )


def build_expand_macro(title: str, body: str) -> StorageFormat:
    """
    Build an expand macro -- a collapsed block that reveals ``body`` when clicked.

    :param title: the always-visible summary line
    :param body: the content revealed on expansion, or the output of other builders
    :return: the expand macro as storage-format markup
    """
    return StorageFormat(
        '<ac:structured-macro ac:name="expand">'
        f"{_build_macro_parameter('title', title)}{_build_rich_text_body(body)}"
        "</ac:structured-macro>"
    )


def build_code_block_macro(
    code: str, *, language: str | None = None, title: str | None = None
) -> StorageFormat:
    """
    Build a code block macro.

    :param code: the code to render, emitted inside a ``CDATA`` section
    :param language: syntax-highlighting language, e.g. ``python``
    :param title: optional title shown above the block
    :return: the code block as storage-format markup
    """
    parameters = ""
    if language:
        parameters += _build_macro_parameter("language", language)
    if title:
        parameters += _build_macro_parameter("title", title)

    # A CDATA section cannot contain "]]>", so split any occurrence across two sections.
    body = _strip_invalid_xml_chars(code).replace("]]>", "]]]]><![CDATA[>")
    return StorageFormat(
        '<ac:structured-macro ac:name="code">'
        f"{parameters}"
        f"<ac:plain-text-body><![CDATA[{body}]]></ac:plain-text-body>"
        "</ac:structured-macro>"
    )


def build_status_lozenge(text: str, *, color: LozengeColor = "grey") -> StorageFormat:
    """
    Build a status lozenge -- the small colored status pill.

    :param text: the lozenge label
    :param color: lozenge color
    :return: the lozenge as storage-format markup
    """
    return StorageFormat(
        '<ac:structured-macro ac:name="status">'
        f"{_build_macro_parameter('colour', color.title())}"
        f"{_build_macro_parameter('title', text)}"
        "</ac:structured-macro>"
    )

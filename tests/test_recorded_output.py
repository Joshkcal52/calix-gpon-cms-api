"""
The E7 CLI backend's parsers against output recorded from real E7s
(tests/fixtures/e7/<release>/, written by python -m app.tools.e7_capture).

The mock E7 shares the parsers' guesses, so passing against it proves nothing
about a real E7. Each recording here must be read without a parse error, and
what the backend depends on (a serial number, a service's port and name) must
be found. Skipped until a capture has been committed.
"""

import asyncio
import re
from pathlib import Path

import pytest

from app.nbi.e7_backend import E7CliBackend
from app.nbi.e7_session import E7Node

RECORDINGS = sorted((Path(__file__).parent / "fixtures" / "e7").glob("*/*.txt"))


def recorded(path: Path):
    first, _, output = path.read_text(encoding="utf-8").partition("\n")
    return first.removeprefix("$ ").strip(), output


@pytest.mark.skipif(not RECORDINGS, reason="no E7 output recorded yet (see tests/fixtures/e7/README.md)")
@pytest.mark.parametrize("path", RECORDINGS, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_the_backend_reads_recorded_e7_output(path):
    command, output = recorded(path)
    backend = E7CliBackend([E7Node(name="E7", host="recorded")], sessions={"E7": object()})

    async def show(network, sent):
        return output

    backend._show = show
    if match := re.fullmatch(r"show ont (\d+) detail", command):
        ont = asyncio.run(backend._ont_detail("E7", int(match.group(1))))
        assert ont is not None and ont.serial_number
    elif match := re.fullmatch(r"show ont-port (\d+) eth-svc", command):
        asyncio.run(backend._services("E7", int(match.group(1))))  # raises NbiParseError on a row it cannot read
    elif command in ("show ont", "show ont detail"):
        asyncio.run(backend.list_onts("E7"))

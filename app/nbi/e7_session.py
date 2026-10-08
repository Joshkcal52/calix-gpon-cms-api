"""
E7 CLI Session
One SSH session to one E7 EXA node's command line, for the E7 CLI backend
(app/nbi/e7_backend.py). Used instead of CMS when the network has no NBI.

The session logs in once, turns the pager off, then runs one command at a
time: it sends the command and reads until the prompt comes back. Commands to
one E7 never overlap (a lock), because a CLI is a single conversation.

A session that times out or loses its connection mid-command is thrown away,
not reused: what the E7 prints next could belong to the abandoned command.
The next command logs in again.

The prompt pattern and the pager command are settings because they are not
verified against a real E7 (see E7_PROMPT_PATTERN and E7_PAGER_COMMAND).
"""

import asyncio
import logging
import re
from dataclasses import dataclass, field
from typing import Optional, Pattern, Sequence

from app.nbi.errors import NbiAuthError, NbiConnectionError

logger = logging.getLogger(__name__)

# Paging prompts answered with a space, in case the pager command is not accepted
MORE_PROMPTS = re.compile(r"(--More--|--more--|<--- More --->|Press any key to continue)\s*$")
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


@dataclass
class E7Node:
    """Where one E7 is and how to log in"""
    name: str  # the network name used in this API's URLs
    host: str
    port: int = 22
    username: str = ""
    password: str = field(default="", repr=False)
    known_hosts: Optional[str] = None  # a known_hosts file; "none" skips the host key check


class E7Session:
    def __init__(self, node: E7Node, *, prompt: str, pager_command: Optional[str],
                 connect_timeout: float = 10.0, setup_commands: Sequence[str] = ()):
        self.node = node
        self.prompt: Pattern[str] = re.compile(prompt)
        self.pager_command = pager_command
        self.connect_timeout = connect_timeout
        self.setup_commands = tuple(setup_commands)
        self._lock = asyncio.Lock()
        self._conn = None
        self._process = None

    async def run(self, command: str) -> str:
        """Run one command and return what it printed, without the echoed command or the prompt"""
        async with self._lock:
            try:
                if self._process is None:
                    await self._open()
                return await self._exchange(command)
            except BaseException:
                # Timed out, cancelled or disconnected: the session's state is unknown
                await self._drop()
                raise

    async def close(self) -> None:
        async with self._lock:
            await self._drop()

    # --- Internals ---------------------------------------------------------------

    async def _open(self) -> None:
        try:
            import asyncssh
        except ImportError as e:  # pragma: no cover - asyncssh is in requirements.txt
            raise NbiConnectionError("The E7 CLI backend needs the asyncssh package.") from e

        known_hosts = () if self.node.known_hosts is None else (
            None if self.node.known_hosts.lower() == "none" else self.node.known_hosts)
        options = {"known_hosts": known_hosts} if known_hosts != () else {}
        try:
            self._conn = await asyncio.wait_for(asyncssh.connect(
                self.node.host, port=self.node.port, username=self.node.username, password=self.node.password,
                client_keys=None, **options), timeout=self.connect_timeout)
        except asyncssh.PermissionDenied as e:
            raise NbiAuthError(f"E7 {self.node.name} refused this API's SSH login ({self.node.username}).") from e
        except asyncssh.HostKeyNotVerifiable as e:
            raise NbiConnectionError(
                f"E7 {self.node.name}'s SSH host key is not in the known hosts file. Add it (ssh-keyscan), "
                f"or set E7_KNOWN_HOSTS.") from e
        except Exception as e:  # OSError, asyncssh.Error, a timeout, or anything else that stops the login
            raise NbiConnectionError(f"Could not reach E7 {self.node.name} at {self.node.host}:{self.node.port}: "
                                     f"{e or type(e).__name__}") from e

        self._process = await self._conn.create_process(term_type="vt100", term_size=(512, 1000), encoding="utf-8")
        await self._read_until_prompt()  # the login banner
        for command in ((self.pager_command,) if self.pager_command else ()) + self.setup_commands:
            await self._exchange(command)
        logger.info(f"SSH session open to E7 {self.node.name}")

    async def _exchange(self, command: str) -> str:
        self._process.stdin.write(command + "\n")
        return _clean(await self._read_until_prompt(), command)

    async def _read_until_prompt(self) -> str:
        buffer = ""
        while True:
            chunk = await self._process.stdout.read(65536)
            if not chunk:
                raise NbiConnectionError(f"E7 {self.node.name} closed the SSH session.")
            buffer += chunk
            text = ANSI.sub("", buffer).replace("\r", "")
            if MORE_PROMPTS.search(text):
                buffer = MORE_PROMPTS.sub("", ANSI.sub("", buffer))
                self._process.stdin.write(" ")
                continue
            last_line = text.rsplit("\n", 1)[-1]
            if self.prompt.fullmatch(last_line):
                return text[: len(text) - len(last_line)]

    async def _drop(self) -> None:
        conn, self._conn, self._process = self._conn, None, None
        if conn is not None:
            conn.close()
            try:
                await asyncio.wait_for(conn.wait_closed(), timeout=2)
            except (asyncio.TimeoutError, Exception):
                pass


def _clean(output: str, command: str) -> str:
    """Remove the echoed command line from the start of the output"""
    lines = output.split("\n")
    if lines and lines[0].strip().endswith(command.strip()):
        lines = lines[1:]
    return "\n".join(lines).strip("\n")

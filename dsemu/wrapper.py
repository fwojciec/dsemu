import os
import shutil
import subprocess
import time
from types import TracebackType
from typing import Optional, Tuple, Type, Set
from urllib.error import URLError
from urllib.request import Request, urlopen

RESET_ENDPOINT = "/reset"
SHUTDOWN_ENDPOINT = "/shutdown"
HEALTHCHECK_ENDPOINT = ""


class EmulatorException(Exception):
    pass


class Emulator:
    """
    Emulator wraps the datastore emulator process enabling programmatic control
    of the emulator (start, stop, reset), thus making it possible to use the
    emulator for running tests.

    By default the wrapper runs the emulator using its in-memory storage and
    disables the eventual consistency simulation for the sake of avoiding
    random failures during tests.

    The wrapper will reuse the currently running instance of the emulator if it
    is available and configured correctly (i.e. if the required environment
    variables are present).
    """

    def __init__(self) -> None:
        self._host: Optional[str] = None
        self._project_id: Optional[str] = None
        self._gcloud: Optional[str] = None
        self._instance: Optional[subprocess.Popen] = None
        self._env_vars_set: Set[str] = set()

    def __enter__(self) -> "Emulator":
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] = None,
        value: BaseException = None,
        traceback: TracebackType = None,
    ):
        self.stop()

    def start(self) -> None:
        """
        Either starts a new emulator process or configures the environment to
        use the currently running emulator process.
        """
        if not self._is_already_running():
            self._start()

    def stop(self) -> None:
        """
        Performs a teardown of the emulator wrapper. If an instance of the
        emulator was started in the process of initializing the wrapper it will
        be stopped.
        """
        if self._instance is not None and self._is_healthy():
            self._request(SHUTDOWN_ENDPOINT, method="POST")
            os.unsetenv("DATASTORE_EMULATOR_HOST")
            os.unsetenv("DATASTORE_PROJECT_ID")
            self._instance.terminate()
            self._instance = None

        self._host = None
        self._project_id = None
        self._gcloud = None

    def reset(self) -> None:
        """
        Reset resets the in-memory emulator storage. This works only when
        in-memory option was set when starting the emulator instance.
        """
        self._request(RESET_ENDPOINT, method="POST")

    def _is_already_running(self) -> bool:
        host = os.getenv("DATASTORE_HOST")
        if host is None:
            return False

        project_id = os.getenv("DATASTORE_PROJECT_ID")
        if project_id is None:
            return False

        try:
            self._request(HEALTHCHECK_ENDPOINT, host=host)
        except (EmulatorException, URLError, RuntimeError):
            return False

        self._host = host
        self._project_id = project_id

        return True

    def _start(self) -> None:
        args = [
            self._get_binary(),
            "beta",
            "emulators",
            "datastore",
            "start",
            "--consistency=1.0",
            "--no-store-on-disk",
        ]
        self._host, self._project_id = self._init_env()
        self._instance = subprocess.Popen(
            args,
            stderr=subprocess.PIPE,
        )
        self._confirm_startup()

    def _command(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            (self._get_binary(), "beta", "emulators", "datastore") + args,
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )

    def _init_env(self) -> Tuple[str, str]:
        proc = self._command("env-init")
        host, emulator_host, project_id = self._parse_env_init_output(proc.stdout)
        self._set_env_var("DATASTORE_EMULATOR_HOST", emulator_host)
        self._set_env_var("DATASTORE_PROJECT_ID", project_id)
        return (host, project_id)

    def _is_healthy(self) -> bool:
        try:
            self._request(HEALTHCHECK_ENDPOINT)
        except URLError:
            return False
        return True

    def _request(self, path: str, method: str = "GET", host: Optional[str] = None):
        if host is None:
            host = self._get_host()

        with urlopen(Request(host + path, method=method)) as resp:
            if resp.status != 200:
                path = path.replace("/", "") if path != "" else "healthcheck"
                msg = f"emulator {path} request failed with status code {resp.status}"
                raise EmulatorException(msg)

    def _confirm_startup(self, timeout: int = 10) -> None:
        t = time.time()
        while True:
            if self._is_healthy():
                return
            if time.time() - t >= 10:
                raise EmulatorException("confirm startup timed out")
            time.sleep(0.5)

    def _set_env_var(self, key: str, val: str) -> None:
        os.environ[key] = val
        self._env_vars_set.add(key)

    def _unset_env_vars(self) -> None:
        while self._env_vars_set:
            os.unsetenv(self._env_vars_set.pop())

    def _get_host(self) -> str:
        if self._host is None:
            raise RuntimeError("host value is not set")
        return self._host

    def _get_binary(self) -> str:
        if self._gcloud is None:
            self._gcloud = self._find_binary()
        return self._gcloud

    def _find_binary(self) -> str:
        gcloud = shutil.which("gcloud")
        if gcloud is None:
            raise OSError(2, "binary not found", "gcloud")
        return gcloud

    def _parse_env_init_output(self, output: str) -> Tuple[str, str, str]:
        host = None
        emulator_host = None
        project_id = None

        for line in output.rstrip().split("\n"):
            key, val = line.split("=")
            if key.endswith("DATASTORE_HOST"):
                host = val
            if key.endswith("DATASTORE_EMULATOR_HOST"):
                emulator_host = val
            if key.endswith("DATASTORE_PROJECT_ID"):
                project_id = val

        if host is None or emulator_host is None or project_id is None:
            raise EmulatorException("failed to parse env-init output")

        return host, emulator_host, project_id

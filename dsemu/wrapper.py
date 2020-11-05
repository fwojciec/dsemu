import os
import shutil
import subprocess
import time
from types import TracebackType
from typing import Optional, Type, Set
from urllib.error import URLError
from urllib.request import Request, urlopen

RESET_ENDPOINT = "/reset"
SHUTDOWN_ENDPOINT = "/shutdown"
HEALTHCHECK_ENDPOINT = ""
DEFAULT_PROJECT = "test"
DEFAULT_HOST = "localhost:8088"


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

    def __init__(
        self,
        _default_project: str = DEFAULT_PROJECT,
        _default_host: str = DEFAULT_HOST,
        _reset_endpoint: str = RESET_ENDPOINT,
        _shutdown_endpoint: str = SHUTDOWN_ENDPOINT,
        _healthcheck_endpoint: str = HEALTHCHECK_ENDPOINT,
    ) -> None:
        self._host: Optional[str] = None
        self._project_id: Optional[str] = None
        self._gcloud: Optional[str] = None
        self._instance: Optional[subprocess.Popen] = None
        self._env_vars_set: Set[str] = set()

        # configurable defaults
        self._default_project = _default_project
        self._default_host = _default_host
        self._reset_endpoint = _reset_endpoint
        self._shutdown_endpoint = _shutdown_endpoint
        self._healthcheck_endpoint = _healthcheck_endpoint

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
            self._request(self._shutdown_endpoint, method="POST")
            self._unset_env_vars()
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
        self._request(self._reset_endpoint, method="POST")

    def _is_already_running(self) -> bool:
        host = os.getenv("DATASTORE_HOST")
        if host is None:
            return False

        project_id = os.getenv("DATASTORE_PROJECT_ID")
        if project_id is None:
            return False

        try:
            self._request(self._healthcheck_endpoint, host=host)
        except (EmulatorException, URLError, RuntimeError):
            return False

        self._host = host
        self._project_id = project_id
        return True

    def _start(self) -> None:
        args = [
            self._gcloud_binary,
            "beta",
            "emulators",
            "datastore",
            "start",
            "--consistency=1.0",
            "--no-store-on-disk",
            f"--host-port={self._default_host}",
            f"--project={self._default_project}",
        ]
        self._host = f"http://{self._default_host}"
        self._project_id = self._default_project
        self._instance = subprocess.Popen(
            args,
            stderr=subprocess.PIPE,
        )
        self._confirm_startup()
        self._set_env_var("DATASTORE_EMULATOR_HOST", self._default_host)
        self._set_env_var("DATASTORE_PROJECT_ID", self._default_project)

    def _is_healthy(self) -> bool:
        try:
            self._request(self._healthcheck_endpoint)
        except URLError:
            return False
        return True

    def _request(self, path: str, method: str = "GET", host: Optional[str] = None):
        if host is None:
            host = self._current_host
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

    @property
    def _current_host(self) -> str:
        if self._host is None:
            raise RuntimeError("host value is not set")
        return self._host

    @property
    def _gcloud_binary(self) -> str:
        if self._gcloud is None:
            gcloud = shutil.which("gcloud")
            if gcloud is None:
                raise OSError(2, "binary not found", "gcloud")
            self._gcloud = gcloud
        return self._gcloud

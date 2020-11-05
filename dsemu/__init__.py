import os
import shutil
import subprocess
import time
from types import TracebackType
from typing import Optional, Type
from urllib.error import URLError
from urllib.request import Request, urlopen
from urllib.parse import urlparse

DEFAULT_PROJECT = "test"
DEFAULT_HOST = "http://localhost:8088"
RESET_ENDPOINT = "/reset"
SHUTDOWN_ENDPOINT = "/shutdown"
HEALTHCHECK_ENDPOINT = ""
DEFAULT_TIMEOUT = 30


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
        project: str = DEFAULT_PROJECT,
        host: str = DEFAULT_HOST,
        reset_endpoint: str = RESET_ENDPOINT,
        shutdown_endpoint: str = SHUTDOWN_ENDPOINT,
        healthcheck_endpoint: str = HEALTHCHECK_ENDPOINT,
        timeout: int = DEFAULT_TIMEOUT,
    ) -> None:
        self._project: str = project
        self._host: str = host
        self._reset_endpoint = reset_endpoint
        self._shutdown_endpoint = shutdown_endpoint
        self._healthcheck_endpoint = healthcheck_endpoint
        self._timeout = timeout
        self._gcloud: Optional[str] = None
        self._instance: Optional[subprocess.Popen] = None

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
        if self._instance:
            self._teardown_instance()

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

        project = os.getenv("DATASTORE_PROJECT_ID")
        if project is None:
            return False

        try:
            self._request(self._healthcheck_endpoint, host=host)
        except (EmulatorException, URLError, RuntimeError):
            return False

        self._host = host
        self._project = project
        return True

    def _start(self) -> None:
        emulator_host = urlparse(self._host).netloc
        self._instance = subprocess.Popen(
            [
                self._gcloud_binary,
                "beta",
                "emulators",
                "datastore",
                "start",
                "--consistency=1.0",
                "--no-store-on-disk",
                f"--host-port={emulator_host}",
                f"--project={self._project}",
            ],
            stderr=subprocess.PIPE,
        )
        self._confirm_startup()
        os.environ["DATASTORE_EMULATOR_HOST"] = emulator_host
        os.environ["DATASTORE_PROJECT_ID"] = self._project

    def _teardown_instance(self) -> None:
        if self._is_healthy():
            self._request(self._shutdown_endpoint, method="POST")
        os.unsetenv("DATASTORE_EMULATOR_HOST")
        os.unsetenv("DATASTORE_PROJECT_ID")

    def _request(self, path: str, method: str = "GET", host: Optional[str] = None):
        if host is None:
            host = self._host
        with urlopen(Request(host + path, method=method)) as resp:
            if resp.status != 200:
                path = path.replace("/", "") if path != "" else "healthcheck"
                msg = f"emulator {path} request failed with status code {resp.status}"
                raise EmulatorException(msg)

    def _is_healthy(self) -> bool:
        try:
            self._request(self._healthcheck_endpoint)
        except URLError:
            return False
        return True

    def _confirm_startup(self) -> None:
        t = time.time()
        while True:
            if self._is_healthy():
                return
            if time.time() - t >= self._timeout:
                raise EmulatorException("confirm startup timed out")
            time.sleep(0.1)

    @property
    def _gcloud_binary(self) -> str:
        if self._gcloud is None:
            gcloud = shutil.which("gcloud")
            if gcloud is None:
                raise OSError(2, "binary not found", "gcloud")
            self._gcloud = gcloud
        return self._gcloud

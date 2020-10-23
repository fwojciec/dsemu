import pytest
from google.cloud import datastore
from dsemu.wrapper import Emulator


@pytest.fixture(scope="session")
def emulator():
    with Emulator() as emulator:
        yield emulator


@pytest.fixture(scope="session")
def session_client():
    client = datastore.Client()
    yield client


@pytest.fixture()
def client(emulator: Emulator, session_client: datastore.Client):
    emulator.reset()
    yield session_client

import pytest
from dsemu import Emulator
from google.cloud import datastore


@pytest.fixture(scope="session")
def emulator():
    with Emulator() as emulator:
        yield emulator


@pytest.fixture(scope="session")
def session_client():
    client = datastore.Client(project="test")
    yield client


@pytest.fixture()
def client(emulator: Emulator, session_client: datastore.Client):
    emulator.reset()
    yield session_client

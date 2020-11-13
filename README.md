# dsemu

## Description

`dsemu` is a simple library to help with testing GCP Datastore code written in
Python. The provided `Emulator` class wraps the [Datastore
emulator](https://cloud.google.com/datastore/docs/tools/datastore-emulator) and
provides basic functionality such as starting/stopping and reseting the
emulator instance from inside the test runner.

## Requirements

You must have the `gcloud` tool
[installed](https://cloud.google.com/datastore/docs/tools/datastore-emulator)
and available in `PATH`.

## Using existing instance of the emulator

If you're running tests that require datastore access frequently it might be
better to keep an instance of the emulator running at all time instead of
letting the wrapper start and stop it for the duration of the test run. If an
instance of the emulator is running and the required environment variables are
correctly set the `Emulator` wrapper will use the running instance instead of
starting a new one and will not tear it down at the end of the test run.

## Example usage with pytest

```python
# conftest.py
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
```

```python
# datastore_test.py
from google.cloud import datastore


def test_datastore_put_and_get(client: datastore.Client):
    kind = "Task"
    name = "sampletask1"
    task_key = client.key(kind, name)
    task = datastore.Entity(key=task_key)
    task["description"] = "Buy milk"
    client.put(task)

    res = client.get(task_key)
    assert res == task
```

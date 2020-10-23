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

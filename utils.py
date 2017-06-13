import json
import uuid


def write_json(filepath, obj):
    with open(filepath, 'w') as file:
        json.dump(obj, file)


def read_json(filepath):
    with open(filepath) as file:
        obj = json.load(file)
    return obj


def short_uid(length, exists_func):
    while True:
        uid = uuid.uuid4().hex[0:length]
        if not exists_func(uid):
            return uid

import os
import re
from typing import Generator, List, Set, Union
from dataclasses import dataclass
from flask import Flask, request
import marshmallow_dataclass
import marshmallow

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def file_reader(file_name: str) -> Generator:
    with open(file_name) as f:
        for row in f:
            yield row

def filter_func(data: Union[List, Set, Generator, map], param: str) -> filter:
    return filter(lambda x: param in x, data)

def map_func(data: Union[List, Set, Generator, filter], param: str) -> map:
    return map(lambda x: f'{x[int(param)]}\n', [el.split() for el in data])

def sorted_func(data: Union[List, Set, Generator, filter, map], param: str) -> List:
    return sorted(data, reverse=True if param == 'desc' else False)

def limit(data: Union[Generator, filter, map], param: str) -> List:
    return [next(data) for _ in range(int(param))]

def unique(data: Union[List, Generator, filter, map], param) -> Set:
    return set(data)

def regex(data: Union[List, Set, Generator, filter, map], param: str) -> Generator:
    for d in data:
        if re.findall(re.compile(param), d):
            yield d


FUNC_MAPPING = {'filter': filter_func,
              'map': map_func,
              'sort': sorted_func,
              'unique': unique,
              'limit': limit,
              'regex': regex}

@dataclass
class MyRequest:
    file_name: str
    cmd1: str
    value1: str
    cmd2: str
    value2: str = ''

    class Meta:
        unknown = marshmallow.EXCLUDE

MyRequest_Schema = marshmallow_dataclass.class_schema(MyRequest)

@app.route("/perform_query", methods=['POST'])
def perform_query():

    try:
        my_request = MyRequest_Schema().load(request.form.to_dict())
    except marshmallow.exceptions.ValidationError:
        return 'error: bad request 1', 400

    if my_request.cmd1 not in FUNC_MAPPING or my_request.cmd1 not in FUNC_MAPPING:
        print(my_request.cmd1, my_request.cmd2)
        return 'error: bad request', 400

    path = f'{DATA_DIR}/{my_request.file_name}'
    if not os.path.exists(path):
        return 'error: file not found', 400

    data: Generator = file_reader(path)
    data = FUNC_MAPPING[my_request.cmd1](data, my_request.value1)
    data = FUNC_MAPPING[my_request.cmd2](data, my_request.value2)

    return app.response_class(data, content_type="text/plain")

import os
import json

from flask import Flask, request

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def file_reader(file_name):
    with open(file_name) as f:
        for row in f:
            yield row

def filter_func(data, param:str):
    return filter(lambda x: param in x, data)

def map_func(data, param:str):
    return map(lambda x: f'{x[int(param)]}\n', [el.split() for el in data])

def sorted_func(data, param:str):
    return sorted(data, reverse=True if param == 'desc' else False)

def limit(data, param:int):


FUNC_MAPPING={'filter': filter_func,
              'map': map_func,
              'sort': sorted_func,
              'unic': 'unic',
              'limit': limit,}

@app.route("/perform_query", methods=['POST'])
def perform_query():
    res = request.form.to_dict()
    path = f'{DATA_DIR}/{res["file_name"]}'
    if not os.path.exists(path):
        return 'error: file not found', 400
    try:
        cmd1, value1, cmd2, value2 = res['cmd1'], res['value1'], res['cmd2'], res['value2']
    except KeyError:
        return 'error: not enough parametrs', 400
    data = file_reader(path)
    data = FUNC_MAPPING[cmd1](data, value1)
    data = FUNC_MAPPING[cmd2](data, value2)



    # получить параметры query и file_name из request.args, при ошибке вернуть ошибку 400
    # проверить, что файла file_name существует в папке DATA_DIR, при ошибке вернуть ошибку 400
    # с помощью функционального программирования (функций filter, map), итераторов/генераторов сконструировать запрос
    # вернуть пользователю сформированный результат
    return app.response_class(data, content_type="text/plain")
import requests
import psycopg2
from typing import Any
import json

# id = 1740, 3529, 7465, 1133404, 4235252, 8884, 1918903, 80, 2742952, 5049705


def get_hh_vacancies(id: str) -> list:
    '''парсинг до 100 вакансий от 10 работодателей через API headhunter'''
    # делаем запрос до 100 вакансий
    data_requests = []
    responses = requests.get(f"https://api.hh.ru/vacancies?{id}", params={'employer_id': {id}, 'per_page': 100}).json()['items']
    for data in responses:
        data_requests.append([data['employer']['id'],
                              data['employer']['name'],
                              data['name'],
                              data['alternate_url'],
                              data['salary']['from'] if data['salary'] is not None else None,
                              data['salary']['to'] if data['salary'] is not None else None,
                              data['salary']['currency'] if data['salary'] is not None else None])
    with open("hh.json", "w", encoding="utf-8") as f:
        json.dump(data_requests, f, indent=2, ensure_ascii=False)
    return data_requests


def save_data_to_database(self: list[dict[str, Any]], dbname: str, params: dict) -> None:
    '''сохранение данных в таблицу базы данных'''
    conn = psycopg2.connect(dbname=dbname, **params)

    with conn.cursor() as cur:
        for i in self:
            cur.execute('INSERT INTO employers VALUES (%s, %s, %s, %s, %s, %s, %s)', i)

    conn.commit()
    conn.close()


import psycopg2
from psycopg2 import errors


class DBManager:
    def __init__(self, dbname, params):
        self.dbname = dbname
        self.params = params

    def create_database(self):
        """Создает базу данных и инициирует подключение к ней с указанными параметрами"""
        conn = psycopg2.connect(dbname='postgres', **self.params)
        conn.autocommit = True
        cur = conn.cursor()

        try:
            cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}")  # затирает БД если она уже существует
            cur.execute(f"CREATE DATABASE {self.dbname}")  # создает новую БД
        except psycopg2.errors.ObjectInUse:
            cur.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) "
                        "FROM pg_stat_activity "
                        f"WHERE pg_stat_activity.datname = '{self.dbname}' ")
            cur.execute(f"DROP DATABASE IF EXISTS {self.dbname}")
            cur.execute(f"CREATE DATABASE {self.dbname}")

        finally:
            cur.close()
            conn.close()

        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        with conn:
            with conn.cursor() as cur:
                cur.execute('''
                    CREATE TABLE employers (
                        employer_id int,
                        employer_name varchar (50) NOT NULL,
                        vacancy_name varchar (100) NOT NULL,
                        url text,
                        salary_from int,
                        salary_to int,
                        currency varchar (50))
                ''')
        conn.close()

    def get_companies_and_vacancies_count(self) -> list:
        """Получает список всех компаний и количество вакансий у каждой компании"""
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT DISTINCT employer_name, COUNT(*) AS count_vacancies '
                                'FROM employers '
                                'GROUP BY employer_name '
                                'ORDER BY count_vacancies DESC')
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            conn.close()

    def get_all_vacancies(self) -> list:
        '''получает список всех вакансий с указанием названия компании,
        названия вакансии и зарплаты и ссылки на вакансию.'''
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT employer_name, vacancy_name, salary_from, salary_to, currency, url '
                                'FROM employers '
                                'ORDER BY employer_name')
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            conn.close()

    def get_avg_salary(self) -> list:
        '''получает среднюю зарплату по вакансиям.'''
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT ROUND(AVG(salary_to + salary_from / 2)) AS avg_salary '
                                'FROM employers '
                                'WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL')
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            conn.close()

    def get_vacancies_with_higher_salary(self) -> list:
        '''получает список всех вакансий, у которых зарплата выше средней по всем вакансиям.'''
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute('''SELECT vacancy_name, salary_to
                                FROM employers
                                WHERE salary_to > (SELECT AVG(salary_to) FROM employers)''')
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            conn.close()

    def get_vacancies_with_keyword(self, search_keyword) -> list:
        '''получает список всех вакансий, в названии которых содержатся переданные в метод слова.'''
        conn = psycopg2.connect(dbname=self.dbname, **self.params)
        try:
            with conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT vacancy_name, url "
                                "FROM employers "
                                f"WHERE vacancy_name ILIKE '%{search_keyword}%'")
                    rows = cur.fetchall()
                    for row in rows:
                        print(row)
        finally:
            conn.close()




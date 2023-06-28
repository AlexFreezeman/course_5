--создание базы данных
DROP DATABASE IF EXISTS head_hunter;
CREATE DATABASE head_hunter;
--ИЛИ
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = head_hunter;
DROP DATABASE IF EXISTS head_hunter;
CREATE DATABASE head_hunter;

--создание таблицы
CREATE TABLE employers (
        employer_id int,
        employer_name varchar (50) NOT NULL,
        vacancy_name varchar (100) NOT NULL,
        url text,
        salary_from int,
        salary_to int,
        currency varchar (50));


--get_companies_and_vacancies_count
SELECT DISTINCT employer_name, COUNT(*) AS count_vacancies FROM employers
GROUP BY employer_name
ORDER BY count_vacancies DESC;

--get_all_vacancies
SELECT employer_name, vacancy_name, salary_from, salary_to, currency, url FROM employers
ORDER BY employer_name;

--get_avg_salary
SELECT ROUND(AVG(salary_to + salary_from / 2)) AS avg_salary FROM employers
WHERE salary_from IS NOT NULL AND salary_to IS NOT NULL;

--get_vacancies_with_higher_salary
SELECT vacancy_name, salary_to FROM employers
WHERE salary_to > (SELECT AVG(salary_to) FROM employers);

--get_vacancies_with_keyword
SELECT vacancy_name, url FROM employers
WHERE vacancy_name ILIKE '%{search_keyword}%';
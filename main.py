from db_manager import DBManager
from config import config
from hh import get_hh_vacancies, save_data_to_database

id = '1740', '3529', '7465', '7172', '669587', '5516123', '3530', '80', '2742952', '890736'


def main():
    params = config()
    db = DBManager('head_hunter', params)
    db.create_database()
    print('Создаем базу данных и таблицы')
    print('Идёт загрузка данных. Это может занять какое-то время')
    for i in id:
        data_requests = get_hh_vacancies(i)
        save_data_to_database(data_requests, 'head_hunter', params)
        print(f"Загрузка данных по компании {i}")
    print(f'База данных и таблицы созданы')
    search_keyword = input("Введите слово, по которому будем искать вакансии (например, “python”)\n")
    #    search_keyword = 'Python'

    # вывод меню для пользователя
    print('Выберите действия с полученной информацией. Выберите и нажмите ENTER:\n'
          '1 - список всех компаний и количество вакансий у каждой компании\n'
          '2 - список всех вакансий с указанием названия компании, названия вакансии и зарплаты и ссылки на вакансию\n'
          '3 - средняя зарплата по вакансиям\n'
          '4 - список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
          '5 - список всех вакансий, в названии которых указанное Вами слово (например, “python”)\n'
          '0 - завершение программы')
    user_choice = input()

    while user_choice != '0':
        if user_choice == '1':
            # вывод списка всех компаний и количества вакансий у каждой компании
            db.get_companies_and_vacancies_count()
            user_choice = input('Введите следующую команду ')
        if user_choice == '2':
            # вывод списка всех вакансий с указанием названия компании,
            # названия вакансии, зарплаты и ссылки на вакансию
            db.get_all_vacancies()
            user_choice = input('Введите следующую команду ')
        if user_choice == '3':
            # вывод средней зарплаты по вакансиям
            db.get_avg_salary()
            user_choice = input('Введите следующую команду ')
        if user_choice == '4':
            # вывод списка всех вакансий, у которых зарплата выше средней по всем вакансиям
            db.get_vacancies_with_higher_salary()
            user_choice = input('Введите следующую команду ')
        if user_choice == '5':
            db.get_vacancies_with_keyword(search_keyword)
            user_choice = input('Введите следующую команду ')
        else:
            # обработка неверного значения, введенного пользователем
            print('Команда не распознана. Попробуйте еще раз или нажмите 0 для завершения программы')
            user_choice = input()

    print('Завершение программы')


if __name__ == "__main__":
    main()

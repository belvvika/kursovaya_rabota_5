from employers_and_vacancies import table, add_table
from DBM import DBManager

def user_interaction():
    list_employers = [9988511, 9861890, 108780, 6050637, 9020831, 3536162, 68411, 3348208, 1626611, 1694073]
    dbm = DBManager()
    table()
    add_table(list_employers)

    while True:

        new = input(
            '1 - список всех компаний и кол-во вакансий у каждой компании\n'
            '2 - список всех вакансий с указанием названия компании, вакансии, зарплаты и ссылки на вакансию\n'
            '3 - средняя зарплата по вакансиям\n'
            '4 - список всех вакансий, у которых зарплата выше средней по всем вакансиям\n'
            '5 - список вакансий, в названии которых содержится ключевое слово\n'
            'Выход - закончить\n'
        )

        if new == '1':
            print(dbm.get_companies_and_vacancies_count())
        elif new == '2':
            print(dbm.get_all_vacancies())
        elif new == '3':
            print(dbm.get_avg_salary())
        elif new == '4':
            print(dbm.get_vacancies_with_higher_salary())
        elif new == '5':
            keyword = str(input('Введите ключеве слово: '))
            print(dbm.get_vacancies_with_keyword(keyword))
        elif new == 'Выход':
            break

if __name__ == "__main__":
    user_interaction()
import psycopg2
from config import config
class DBManager:
    ''' Класс для подключения к базе данных. '''

    def get_companies_and_vacancies_count(self):
        ''' Метод, получающий список всех компаний и кол-во вакансий у каждой компании. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('select company_name, count(vacancies_name) as count_vacancies from employers join vacancies using (employer_id) group by employers.company_name')
                result = cur.fetchall()
            conn.commit()
        return result

    def get_all_vacancies(self):
        ''' Метод, получающий список всех вакансий с указанием названия компании, вакансии, зарплаты и ссылки. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('select employers.company_name, vacancies.vacancies_name, vacancies.payment, vacancies_url from employers join vacancies using (employer_id)')
                result = cur.fetchall()
            conn.commit()
        return result

    def get_avg_salary(self):
        ''' Метод, получающий среднюю зарплату по вакансиям. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('select avg(payment) as payment_avg from vacancies')
                result = cur.fetchall()
            conn.commit()
        return result

    def get_vacancies_with_higher_salary(self):
        ''' Метод, получающий список всех вакансий, у которых зарплата выше средней по всем вакансиям. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute('select * from vacancies where payment > (select avg(payment) from vacancies)')
                result = cur.fetchall()
            conn.commit()
        return result
    def get_vacancies_with_keyword(self, keywords):
        ''' Метод, получающий список всех вакансий в названии которых содержатся переданные в метод слова. '''

        cfg = config('database.ini', 'postgresql_01')
        conn = psycopg2.connect(**cfg)

        with conn:
            with conn.cursor() as cur:
                cur.execute(f'select * from vacancies where vacancies_name like \'%{keywords}%\'')
                result = cur.fetchall()
            conn.commit()
        return result
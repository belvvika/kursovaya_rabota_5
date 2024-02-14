import psycopg2
import requests
from config import config

def get_vacancies(employer_id):
    ''' Получение вакансий по АПИ. '''

    params = {
        'area': 1,
        'page': 0,
        'per_page': 10,
        'employer_id': employer_id
    }
    url = f'https://api.hh.ru/vacancies'

    vacancies = requests.get(url, params=params).json()

    data = []
    for item in vacancies['items']:
        vacancy = {
            'vacancy_id': item['id'],
            'vacancies_name': item['name'],
            'payment': item['salary']['from'] if item['salary'] else None,
            'requirement': item['snippet']['requirement'],
            'vacancies_url': item['alternate_url'],
            'employer_id': employer_id
        }
        if vacancy['payment'] is not None:
            data.append(vacancy)
    return data


def get_employers(employer_id):
    ''' Получение работодателей по АПИ. '''

    url = f"https://api.hh.ru/employers/{employer_id}"

    employers = requests.get(url).json()

    employe = {
        "employer_id": int(employer_id),
        "company_name": employers['name'],
        "open_vacancies": employers['open_vacancies']
    }

    return employe

def table():
    ''' Создание базы данных и таблиц в ней. '''

    conn = psycopg2.connect(
        host='localhost',
        database='kursovaya_rabota_5',
        user='postgres',
        password='12345'
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute('drop database if exists hh')
    cur.execute('create database hh')

    conn.close()

    cfg = config('database.ini', 'postgresql')
    conn = psycopg2.connect(**cfg)
    with conn:
        with conn.cursor() as cur:
            cur.execute('create table employers ('
                            'employer_id int primary key,'
                            'company_name varchar(255),'
                            'open_vacancies int)'
                        )
            cur.execute('create table vacancies ('
                            'vacancy_id serial primary key,'
                            'vacancies_name varchar(255),'
                            'payment int,'
                            'requirement text,'
                            'vacancies_url text,'
                            'employer_id int references employers(employer_id))'
                        )
    conn.commit()


def add_table(list_employers):
    ''' Заполнение таблиц. '''

    conn = psycopg2.connect(
        host='localhost',
        database='hh',
        user='postgres',
        password='12345'
    )
    with conn:
        with conn.cursor() as cur:
            cur.execute('truncate table employers, vacancies restart identity')

            for employer in list_employers:
                emp_list = get_employers(employer)
                cur.execute('insert into employers (employer_id, company_name, open_vacancies) values (%s, %s, %s) returning employer_id',
                            (emp_list['employer_id'], emp_list['company_name'], emp_list['open_vacancies']))

            for employer in list_employers:
                vac_list = get_vacancies(employer)
                for vac in vac_list:
                    cur.execute('insert into vacancies (vacancy_id, vacancies_name, payment, requirement, vacancies_url, employer_id) values (%s, %s, %s, %s, %s, %s)',
                                (vac['vacancy_id'], vac['vacancies_name'], vac['payment'], vac['requirement'], vac['vacancies_url'], vac['employer_id']))

    conn.commit()
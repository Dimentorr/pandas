import numpy as np
import pandas as pd
import asyncio

from multiprocessing import Process, cpu_count, Pool

from timeit import default_timer as timer
from tqdm import tqdm

import datetime


all_weekdays = {0: 'Понедельник',
                1: 'Вторник',
                2: 'Среда',
                3: 'Четверг',
                4: 'Пятница',
                5: 'Суббота',
                6: 'Воскресенье'}


def csv_read(name_csv: str, test=False):
    """
    :param name_csv:
    :param test: False/True - отвечает за путь к искомому csv
    :return: pandas.DataFrame
    """
    if test:
        return pd.read_csv(f'csv/test_data/{name_csv}.csv')
    else:
        return pd.read_csv(f'csv/{name_csv}.csv')


def csv_write(df: pd.DataFrame, name_csv: str):
    """
    :param df: имя pandas.DataFrame
    :param name_csv: имя создаваемого или перезаписываемого csv файла на содержание df
    """
    df.to_csv(f'csv/test_output/{name_csv}')


def find_top(main_df: pd.DataFrame, objects_to_sort: pd.DataFrame, name_col_main: str, condition_top='Количество',
                   name_col_linc_on_objects='Ссылка', products_from=False, from_df=pd.DataFrame(), city=False,
                   city_df=pd.DataFrame(), len_top=10, for_last_msg=''):
    """
    :param main_df: таблица со всей возможной информацией
    :param objects_to_sort: таблица, значения которой необходимо сортировать
    :param name_col_main: имя колонки в главной таблице, содержищей ссылку на значения сортируемой таблицы
    :param condition_top: имя колонки по которому осуществляется сортировка
    :param name_col_linc_on_objects: имя колонки из сортируемой таблицы с названиями объектов, а не ссылками
    :param products_from: флаг, необходимый для сортировки товаров, продаваемых на складах/магазинах
    :param from_df: таблица, со складами/магазинами
    :param city: флаг, на случай если нужна сортировка по городам
    :param city_df: таблица с городами
    :param len_top: количество лучших объектов сортировки необходимое для вывода
    :param for_last_msg: пояснительное сообщение, для вывода лучших объектов
    """
    start_func = timer()
    dict_for_answer = dict()
    for l in tqdm(objects_to_sort.index):
        # Нахождение всех строк,где продавался товар с index = l
        if not products_from:
            condition_data = (main_df.loc[main_df[name_col_main]
                              .isin([objects_to_sort[name_col_linc_on_objects][l]])
                              .pipe(lambda x: x.loc[pd.DataFrame(x)[name_col_main]].index)])
        else:
            condition_data = (main_df.loc[main_df[name_col_main]
                              .isin([objects_to_sort[name_col_linc_on_objects][l]])
                              .pipe(lambda x: x.loc[pd.DataFrame(x)[name_col_main]].index)]
                              .pipe(lambda x: x.loc[(x['Филиал'].isin(from_df['Ссылка']))]))

        sum_count = np.sum(condition_data[condition_top])
        if products_from:
            dict_for_answer[objects_to_sort['Наименование'][l]] = sum_count
        elif city:
            try:
                key = (city_df.loc[(city_df['Ссылка'].isin(
                    objects_to_sort.loc[(objects_to_sort['Ссылка'].isin(condition_data['Филиал']))]
                    ['Город']), ['Наименование'])].values[0][0])
            except IndexError:
                continue
            dict_for_answer[key] = (dict_for_answer[key] + sum_count if key in dict_for_answer else sum_count)
        else:
            key = objects_to_sort['Наименование'][l]
            dict_for_answer[key] = sum_count

    names, sumaries = list(), list()
    count_iter = 0
    for k in dict(sorted(dict_for_answer.items(), key=lambda item: item[1], reverse=True)):
        count_iter += 1
        names.append(k)
        sumaries.append(dict_for_answer[k])
        if count_iter >= len_top:
            break
    result = {
        'Наименование': names,
        'Сумма': sumaries
    }
    end_func = timer()
    print(f'''------------------------------------------------
Время выполнения: {end_func - start_func}''')
    print(f'''------------------------------------------------
{for_last_msg}
------------------------------------------------
{pd.DataFrame.from_dict(result)}''')


def find_top_time(sales_main_df):
    sales_data = pd.DataFrame({'weekday': [], 'hour': [], 'Количество': []})

    sales_data['weekday'] = (sales_main_df['Период']
                             .apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').weekday()))
    sales_data['hour'] = (sales_main_df['Период']
                          .apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').time().hour))
    sales_data['Количество'] = sales_main_df['Количество']

    data_hour = dict()
    data_weekday = dict()
    for h in range(sales_data['hour'].min(), sales_data['hour'].max() + 1):
        data_hour[h] = np.sum(sales_data['hour'].loc[(sales_data['hour'] == h)])
    for d in range(sales_data['weekday'].min(), sales_data['weekday'].max() + 1):
        data_weekday[d] = np.sum(sales_data['weekday'].loc[(sales_data['weekday'] == d)])

    for k in dict(sorted(data_weekday.items(), key=lambda item: item[1], reverse=True)):
        print(f'''День недели с наибольшим количеством продаж - {all_weekdays[k]}''')
        break

    print(f'''Статистика часов по продажам:''')
    for h in dict(sorted(data_hour.items(), key=lambda item: item[1], reverse=True)):
        print(f'''{h} ч. - {data_hour[h]}''')


if __name__ == '__main__':
    Pool(cpu_count())
    print("Чтение вводных csv-файлов...")
    start = timer()
    list_names_csv = ['t_branches', 't_cities', 't_products', 't_sales']
    dict_df_csv = dict()
    for i in tqdm(list_names_csv):
        # dict_df_csv[i[2:]] = csv_read(i, test=True)
        dict_df_csv[i[2:]] = csv_read(i)

    branches = dict_df_csv['branches']
    cities = dict_df_csv['cities']
    products = dict_df_csv['products']
    sales = dict_df_csv['sales']

    end = timer()
    print(f'''Время выполнения: {end - start}
    ------------------------------------------------''')

    # вариант с отбором по значению, без функции
    print("Нахождение уникальных складов/магазинов...")
    start = timer()
    branches_filter = branches.filter(['Ссылка', 'Наименование', 'Город']).sort_values('Наименование')
    branches_filter['warehouse'] = branches_filter['Наименование'].str.find('клад')
    branches_warehouse = branches_filter.loc[(
            branches_filter['warehouse'] != -1), ['Ссылка', 'Наименование']]
    branches_shop = branches_filter.loc[(
            branches_filter['warehouse'] == -1), ['Ссылка', 'Наименование']]
    end = timer()
    print(f'''Время выполнения: {end - start}
    ------------------------------------------------''')

    print('Начало выполнения алгоритмов:')
    start = timer()

    find_top(main_df=sales, objects_to_sort=branches_shop, name_col_main='Филиал', condition_top='Количество',
                 for_last_msg='Десять первых магазинов по количеству продаж')

    find_top(main_df=sales, objects_to_sort=branches_warehouse, name_col_main='Филиал', condition_top='Количество',
                 for_last_msg='Десять первых складов по количеству продаж')

    find_top(main_df=sales, objects_to_sort=products, name_col_main='Номенклатура', condition_top='Количество',
                 products_from=True, from_df=branches_warehouse,
                 for_last_msg='Десять самых продаваемых товаров по складам')

    find_top(main_df=sales, objects_to_sort=products, name_col_main='Номенклатура', condition_top='Количество',
                 products_from=True, from_df=branches_shop,
                 for_last_msg='Десять самых продаваемых товаров по магазинам')

    find_top(main_df=sales, objects_to_sort=branches, name_col_main='Филиал', condition_top='Количество',
                         city=True, city_df=cities,
                         for_last_msg='Десять городов, в которых больше всего продавалось товаров')
    find_top_time(sales)

    end = timer()
    print(f'''Общее время выполнения: {end - start}
    ------------------------------------------------''')
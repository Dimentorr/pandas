import numpy as np
import pandas
import pandas as pd

from multiprocessing import Process, cpu_count, Pool
import matplotlib.pyplot as plt

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

tqdm.pandas()


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
    df.to_csv(f'csv/test_output/{name_csv}.csv')


def definition_class(x, Qmin, Qmax):
    if x >= Qmax:
        return 'Наиболее продаваемый'
    elif x < Qmin:
        return 'Наименее продаваемый'
    else:
        return 'Средне продаваемый'


def class_products(products_df, sales_df):
    print('Началась классификация товаров...')
    answer = pd.DataFrame({'Номенклатура': [], 'КоличестваПродаж': [], 'КлассТовара': [], 'index': []})
    answer['Номенклатура'] = products_df['Наименование']
    answer['index'] = products_df.index
    answer['КоличестваПродаж'] = (answer['index']
                                  .progress_apply(lambda l: int(np.sum(sales_df.loc[sales_df['Номенклатура']
                                                          .isin([products_df['Ссылка'][l]])
                                                          .pipe(lambda x: x.loc[pd.DataFrame(x)['Номенклатура']]
                                                                .index)]
                                                          ['Количество']))))

    min_quantile = answer['КоличестваПродаж'].quantile(.3)
    max_quantile = answer['КоличестваПродаж'].quantile(.9)
    answer['КлассТовара'] = (answer['КоличестваПродаж']
                             .progress_apply(lambda x: definition_class(x, min_quantile, max_quantile)))

    csv_write(answer.drop(['КоличестваПродаж', 'index'], axis=1), 'product_classes')
    print('Классификация товаров завершена!')


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
    answer = pd.DataFrame({'Наименование': [], 'КоличестваПродаж': [], 'index': []})
    answer['Наименование'] = objects_to_sort['Наименование']
    answer['index'] = objects_to_sort.index
    if products_from:
        answer['КоличестваПродаж'] = (answer['index']
                                      .progress_apply(lambda l: np.sum(main_df.loc[main_df[name_col_main]
                                                              .isin([objects_to_sort[name_col_linc_on_objects][l]])
                                                              .pipe(lambda x: x.loc[pd.DataFrame(x)[name_col_main]]
                                                                    .index)]
                                                              .pipe(lambda x: x.loc[(x['Филиал']
                                                                                     .isin(from_df['Ссылка']))])
                                                              [condition_top])))
    elif city:
        dict_for_answer = dict()
        for l in tqdm(objects_to_sort.index):
            # Нахождение всех строк,где продавался товар с index = l
            condition_data = (main_df.loc[main_df[name_col_main]
            .isin([objects_to_sort[name_col_linc_on_objects][l]])
            .pipe(lambda x: x.loc[pd.DataFrame(x)[name_col_main]].index)])

            sum_count = np.sum(condition_data[condition_top])
            try:
                key = (city_df.loc[(city_df['Ссылка'].isin(
                    objects_to_sort.loc[(objects_to_sort['Ссылка'].isin(condition_data['Филиал']))]
                    ['Город']), ['Наименование'])].values[0][0])
            except IndexError:
                continue
            dict_for_answer[key] = (dict_for_answer[key] + sum_count if key in dict_for_answer else sum_count)
            answer = pd.DataFrame({'Наименование': dict_for_answer.keys(),
                                   'КоличестваПродаж': dict_for_answer.values()})
    else:
        answer['КоличестваПродаж'] = (answer['index']
                                      .progress_apply(lambda l: np.sum(main_df.loc[main_df[name_col_main]
                                                              .isin([objects_to_sort[name_col_linc_on_objects][l]])
                                                              .pipe(lambda x: x.loc[pd.DataFrame(x)[name_col_main]]
                                                                    .index)]
                                                              [condition_top])))
    try:
        result = answer.sort_values(['КоличестваПродаж'], ascending=False).drop('index', axis=1).head(len_top)
    except KeyError:
        result = answer.sort_values(['КоличестваПродаж'], ascending=False).head(len_top)
    print(f'''======================================================
{for_last_msg}
======================================================
{result}''')
    end_func = timer()
    print(f'''======================================================
Время выполнения: {end_func - start_func}''')


def find_top_time(sales_main_df: pd.DataFrame):
    start_func = timer()
    print('Началась аналитика по времени')
    sales_data = pd.DataFrame({'weekday': [], 'hour': [], 'Количество': []})
    print('Определение лучшего дня недели...')
    sales_data['weekday'] = (sales_main_df['Период']
                             .progress_apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').weekday()))
    print('Определение лучших часов...')
    sales_data['hour'] = (sales_main_df['Период']
                          .progress_apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').time().hour))
    sales_data['Количество'] = sales_main_df['Количество']

    data_hour = dict()
    data_weekday = dict()
    for h in range(sales_data['hour'].min(), sales_data['hour'].max() + 1):
        data_hour[h] = np.sum(sales_data['Количество'].loc[(sales_data['hour'] == h)])
    for d in range(sales_data['weekday'].min(), sales_data['weekday'].max() + 1):
        data_weekday[d] = np.sum(sales_data['Количество'].loc[(sales_data['weekday'] == d)])

    data_hour = pd.DataFrame.from_dict(data_hour, orient='index').reset_index()
    data_hour.columns = ['Час', 'Количество']

    data_weekday = pd.DataFrame.from_dict(data_weekday, orient='index').reset_index()
    data_weekday.columns = ['ДеньНедели', 'Количество']

    print(f'''День недели с наибольшим количеством продаж - {(all_weekdays[int(data_weekday
                                                                               .sort_values('Количество',
                                                                                            ascending=False)
                                                                               .head(1).values[0][0])])}''')

    print(f'''Статистика часов по продажам:
{data_hour.sort_values('Количество', ascending=False).head()}''')
    end_func = timer()
    print(f'''======================================================
Время выполнения: {end_func - start_func}''')
    data_hour.plot(x='Час', y=['Количество'])
    data_weekday.plot(x='ДеньНедели', y=['Количество'])
    plt.show()


if __name__ == '__main__':
    print("Чтение вводных csv-файлов...")
    Pool(cpu_count())
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

    class_products(products_df=products, sales_df=sales)

    find_top(main_df=sales, objects_to_sort=branches_shop, name_col_main='Филиал', condition_top='Количество',
             for_last_msg='Десять первых магазинов по количеству продаж')

    find_top(main_df=sales, objects_to_sort=branches_warehouse, name_col_main='Филиал', condition_top='Количество',
             for_last_msg='Десять первых складов по количеству продаж')

    find_top(main_df=sales, objects_to_sort=products, name_col_main='Номенклатура', condition_top='Количество',
             products_from=True, from_df=branches_warehouse, for_last_msg='Десять самых продаваемых товаров по складам')

    find_top(main_df=sales, objects_to_sort=products, name_col_main='Номенклатура', condition_top='Количество',
             products_from=True, from_df=branches_shop, for_last_msg='Десять самых продаваемых товаров по магазинам')

    find_top(main_df=sales, objects_to_sort=branches, name_col_main='Филиал', condition_top='Количество', city=True,
             city_df=cities, for_last_msg='Десять городов, в которых больше всего продавалось товаров')
    find_top_time(sales)

    end = timer()
    print(f'''Общее время работы(включая просмотр графиков): {end - start}
    ------------------------------------------------''')

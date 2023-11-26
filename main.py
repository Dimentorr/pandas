import numpy as np
import pandas as pd
import asyncio

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

# Необходимо передать название файла из папки csv
def csv_read(name_csv, test=False):
    if test:
        return pd.read_csv(f'csv/test_data/{name_csv}.csv')
    else:
        return pd.read_csv(f'csv/{name_csv}.csv')


def csv_write(df, name_csv):
    df.to_csv(f'csv/test_output/{name_csv}')


async def async_part(main_df, second_df, name_col_main, condition_top, name_col_second='Ссылка',
                     products_from=False, from_df=pd.DataFrame(), city=False, city_df=pd.DataFrame(),
                     original_dict=dict()):
    dict_for_answer = dict()
    for l in second_df.index:
        # Нахождение всех index(id), соответствующим условию в основной таблице
        condition_id = main_df[name_col_main].isin([second_df[name_col_second][l]])
        condition_id = condition_id.loc[pd.DataFrame(condition_id)[name_col_main]].index
        # Нахождение в основной таблице всех строк с необходимыми index(id)
        # Нахождение всех данных о прадажах для определённого магазина/склада
        condition_data = main_df.loc[main_df.index.isin(condition_id)]
        if products_from:
            condition_data = condition_data.loc[(condition_data['Филиал'].isin(from_df['Ссылка']))]
        sum_count = np.sum(condition_data[condition_top])
        if products_from:
            dict_for_answer[second_df['Наименование'][l]] = sum_count
        else:
            # second_df - branches
            # main - sales
            # к этому моменту в condition_data должны быть строки для отдельного склада/магазина
            if city:
                # сортируем поля из branches
                # print(second_df['Ссылка'].isin(condition_data['Филиал']))
                temp = second_df.loc[(second_df['Ссылка'].isin(condition_data['Филиал']))]
                # print(city_df.loc[(city_df['Ссылка'].isin(temp['Город']), ['Наименование'])].values[0][0])
                try:
                    key = city_df.loc[(city_df['Ссылка'].isin(temp['Город']), ['Наименование'])].values[0][0]
                except IndexError:
                    continue
                if key in dict_for_answer:
                    dict_for_answer[key] = dict_for_answer[key] + sum_count
                else:
                    dict_for_answer[key] = sum_count
            else:
                key = second_df['Наименование'][l]
                dict_for_answer[key] = sum_count
        main_df.drop(labels=condition_id, axis=0)
    original_dict.update(dict_for_answer)


# --------------------------------------------------------------------|
# Main_df - dataframe, должен содержать name_col_main                 |
# --------------------------------------------------------------------|
# Second_df - dataframe, должен содержать name_col_second             |
# --------------------------------------------------------------------|
# Name_col_main - string, название столбца в dataframe (main_df),     |
# где необходимо искать значения из dataframe (second_df)             |
# --------------------------------------------------------------------|
# Name_col_second - string, название столбца, по значениям в котором  |
# будет происходить поиск нусжных строк в main_df                     |
# --------------------------------------------------------------------|
# Condition_top - string, название столбца, по значениям которого     |
# необходимо произвести сортировку                                    |
# --------------------------------------------------------------------|
# --------------------------------------------------------------------|
# Возвращает Dataframe Наименование : Сумма                           |
# --------------------------------------------------------------------|
async def find_top(main_df, objects_to_sort, name_col_main, condition_top, name_col_linc_on_objects='Ссылка',
                   products_from=False, from_df=pd.DataFrame(), city=False, city_df=pd.DataFrame(),
                   len_top=10, for_last_msg=''):
    COUNT_SLICE = 100
    start_func = timer()
    dict_for_answer = dict()
    remains = 1 if len(objects_to_sort) % COUNT_SLICE != 0 else 0
    min_point = 0
    max_point = COUNT_SLICE
    for j in tqdm(range((len(objects_to_sort) // COUNT_SLICE) + remains)):
        await asyncio.create_task(async_part(main_df,
                                  objects_to_sort[min_point + (j * COUNT_SLICE):max_point + (j * COUNT_SLICE)],
                                  name_col_main, condition_top, products_from=products_from,
                                  from_df=from_df, city=city, city_df=city_df, original_dict=dict_for_answer))
    dict_for_answer.update(dict_for_answer)

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


def week_day(x):
    return datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').weekday()


def hour(x):
    return datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S').time().hour


async def find_top_time(sales_main_df):
    sales_df = sales_main_df[['Период', 'Количество']]

    data_hour = dict()
    data_weekday = dict()
    sales_df['weekday'] = sales_df['Период'].apply(week_day)
    sales_df['Период'] = sales_df['Период'].apply(hour)

    for h in range(sales_df['weekday'].min(), sales_df['weekday'].max() + 1):
        sum_count_hour = np.sum(sales_df['weekday'].loc[(sales_df['weekday'] == h)])
        data_hour[h] = sum_count_hour
    for d in range(sales_df['weekday'].min(), sales_df['weekday'].max() + 1):
        sum_count_weekday = np.sum(sales_df['weekday'].loc[(sales_df['weekday'] == d)])
        data_weekday[d] = sum_count_weekday
    for k in dict(sorted(data_weekday.items(), key=lambda item: item[1], reverse=True)):
        print(f'''День недели с наибольшим количеством продаж - {all_weekdays[k]}''')
        break

    print(f'''Статистика часов по продажам:''')
    for h in dict(sorted(data_hour.items(), key=lambda item: item[1], reverse=True)):
        print(f'''{h} ч. - {data_hour[h]}''')


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

# asyncio.run(find_top(main_df=sales, objects_to_sort=branches_shop, name_col_main='Филиал', condition_top='Количество',
#          for_last_msg='Десять первых магазинов по количеству продаж'))
#
# asyncio.run(find_top(main_df=sales, objects_to_sort=branches_warehouse, name_col_main='Филиал', condition_top='Количество',
#          for_last_msg='Десять первых складов по количеству продаж'))
#
# asyncio.run(find_top(main_df=sales, objects_to_sort=products, name_col_main='Номенклатура', condition_top='Количество',
#          products_from=True, from_df=branches_warehouse, for_last_msg='Десять самых продаваемых товаров по складам'))
#
# asyncio.run(find_top(main_df=sales, objects_to_sort=products, name_col_main='Номенклатура', condition_top='Количество',
#          products_from=True, from_df=branches_shop, for_last_msg='Десять самых продаваемых товаров по магазинам'))
#
# asyncio.run(find_top(main_df=sales, objects_to_sort=branches, name_col_main='Филиал', condition_top='Количество',
#          city=True, city_df=cities, for_last_msg='Десять городов, в которых больше всего продавалось товаров'))
asyncio.run(find_top_time(sales))

end = timer()
print(f'''Общее время выполнения: {end - start}
------------------------------------------------''')
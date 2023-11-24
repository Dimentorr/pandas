import numpy as np
import pandas as pd
import asyncio

from timeit import default_timer as timer
from tqdm import tqdm


# Необходимо передать название файла из папки csv
def csv_read(name_csv, test=False):
    if test:
        return pd.read_csv(f'csv/test_data/{name_csv}.csv')
    else:
        return pd.read_csv(f'csv/{name_csv}.csv')


def csv_write(df, name_csv):
    df.to_csv(f'csv/test_output/{name_csv}')


def async_part(main_df, second_df, name_col_main, name_col_second, condition_top,
                     products_from=False, from_df=pd.DataFrame(), city=False, city_df=pd.DataFrame(),
                     original_dict=dict()):
    dict_for_answer = dict()
    for l in second_df.index:
        # Нахождение всех index(id), соответствующим условию в основной таблице
        tmp = main_df[name_col_main].isin([second_df[name_col_second][l]])
        tmp = tmp.loc[pd.DataFrame(tmp)[name_col_main]]

        # Нахождение в основной таблице всех строк с необходимыми index(id)
        # Нахождение всех данных о прадажах для определённого магазина/склада
        tmp_csv = main_df.loc[main_df.index.isin(tmp.index)]
        if products_from:
            tmp_csv = tmp_csv.loc[(tmp_csv['Филиал'].isin(from_df['Ссылка']))]
        # if city or products_from:
        #     sum_count = np.sum(tmp_csv[condition_top])
        # else:
        sum_count = np.sum(tmp_csv[condition_top])
        if products_from:
            dict_for_answer[second_df['Наименование'][l]] = sum_count
        else:
            # second_df - branches
            # main - sales
            # к этому моменту в tmp_csv должны быть строки для отдельного склада/магазина
            if city:
                # сортируем поля из branches
                temp = second_df.loc[(second_df['Ссылка'].isin(tmp_csv['Филиал']))]
                key = city_df.loc[(city_df['Ссылка'].isin(temp['Город']), ['Наименование'])].values[0][0]
                if key in dict_for_answer:
                    dict_for_answer[key] = dict_for_answer[key] + sum_count
                else:
                    dict_for_answer[key] = sum_count
            else:
                key = second_df['Наименование'][l]
                dict_for_answer[key] = sum_count
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
async def find_top(main_df, second_df, name_col_main, name_col_second, condition_top,
                   products_from=False, from_df=pd.DataFrame(), city=False, city_df=pd.DataFrame(),
                   len_top=10, for_last_msg=''):
    COUNT_SLICE = 100
    start_func = timer()
    dict_for_answer = dict()
    remains = 1 if len(second_df) % COUNT_SLICE != 0 else 0
    min_point = 0
    max_point = COUNT_SLICE
    for j in tqdm(range((len(second_df) // COUNT_SLICE) + remains)):
        async_part(main_df, second_df[min_point + (j * COUNT_SLICE):max_point + (j * COUNT_SLICE)],
                   name_col_main, name_col_second, condition_top, products_from=products_from,
                   from_df=from_df, city=city, city_df=city_df, original_dict=dict_for_answer)
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
branches_shop.to_csv('csv/test_output/shops.csv')
branches_warehouse.to_csv('csv/test_output/warehouses.csv')
end = timer()
print(f'''Время выполнения: {end - start}
------------------------------------------------''')


async def main():
    await asyncio.create_task(find_top(main_df=sales, second_df=branches_shop, name_col_main='Филиал',
                                       name_col_second='Ссылка', condition_top='Количество',
                                       for_last_msg='Десять первых магазинов по количеству продаж'))

    await asyncio.create_task(find_top(main_df=sales, second_df=branches_warehouse, name_col_main='Филиал',
                                       name_col_second='Ссылка', condition_top='Количество',
                                       for_last_msg='Десять первых складов по количеству продаж'))

    await asyncio.create_task(find_top(main_df=sales, second_df=products, name_col_main='Номенклатура',
                                       name_col_second='Ссылка', condition_top='Количество', products_from=True,
                                       from_df=branches_warehouse,
                                       for_last_msg='Десять самых продаваемых товаров по складам'))

    await asyncio.create_task(find_top(main_df=sales, second_df=products, name_col_main='Номенклатура',
                                       name_col_second='Ссылка', condition_top='Количество', products_from=True,
                                       from_df=branches_shop,
                                       for_last_msg='Десять самых продаваемых товаров по магазинам'))

    await asyncio.create_task(find_top(main_df=sales, second_df=branches, name_col_main='Филиал',
                                       name_col_second='Ссылка', condition_top='Количество', city=True, city_df=cities,
                                       for_last_msg='Десять городов, в которых больше всего продавалось товаров'))


print('Начало выполнения алгоритмов:')
start = timer()

asyncio.run(main())

end = timer()
print(f'''Общее время выполнения: {end - start}
------------------------------------------------''')
import numpy as np
import pandas as pd

from timeit import default_timer as timer


# Необходимо передать название файла из папки csv
def csv_read(name_csv):
    return pd.read_csv(f'csv/{name_csv}')


def csv_write(df, name_csv):
    df.to_csv(f'csv/{name_csv}')


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
def find_top(main_df, second_df, name_col_main, name_col_second, condition_top,
             from_df=False, city=False, third_df=pd.DataFrame(), len_top=10, products_df=None):
    dict_for_answer = dict()
    if from_df:
        iter_df = products_df
    else:
        iter_df = second_df
    for i in iter_df.index:
        # Нахождение всех index(id), соответствующим условию в основной таблице
        tmp = main_df[name_col_main].isin([second_df[name_col_second][i]])
        tmp = tmp.loc[pd.DataFrame(tmp)[name_col_main]]

        # Нахождение в основной таблице всех строк с необходимыми index(id)
        # Нахождение всех данных о прадажах для определённого магазина/склада
        tmp_csv = main_df.loc[main_df.index.isin(tmp.index)]
        print(f'''Before 
{tmp_csv}
range: {products_df['Ссылка']}''')
        if from_df:
            tmp_csv = tmp_csv.loc[(
                    tmp_csv['Номенклатура'].isin(products_df['Ссылка']))]
        sum_count = np.sum(tmp_csv[condition_top])
        print(f'''----------------------
{i}
{products_df['Ссылка'][i]}
{products_df['Ссылка'].values}
        
        tmp_csv = 
{tmp_csv[condition_top]}
        sum_count =
{sum_count}
        ----------------------''')
        print(f'''After
        {tmp_csv}''')
        # if city:
        #     sum_count = np.sum(tmp_csv[condition_top])
        # else:
        #     sum_count = np.sum(tmp_csv[condition_top])
        if from_df:
            dict_for_answer[products_df['Наименование'][i]] = sum_count
        else:
            # second_df - branches
            # main - sales
            # к этому моменту в tmp_csv должны быть строки для отдельного склада/магазина
            if city:
                # сортируем поля из branches
                temp = second_df.loc[(second_df['Ссылка'].isin(tmp_csv['Филиал']))]
                # print(list(temp))
                key = third_df.loc[(third_df['Ссылка'].isin(temp['Город']), ['Наименование'])].values[0][0]
                if key in dict_for_answer:
                    dict_for_answer[key] = dict_for_answer[key] + sum_count
                else:
                    dict_for_answer[key] = sum_count
                # print(key)
            else:
                key = second_df['Наименование'][i]
                dict_for_answer[key] = sum_count
    names, sumaries = list(), list()
    count_iter = 0
    for i in dict(sorted(dict_for_answer.items(), key=lambda item: item[1], reverse=True)):
        count_iter += 1
        names.append(i)
        sumaries.append(dict_for_answer[i])
        if count_iter >= len_top:
            # print('All')
            break
    result = {
        'Наименование': names,
        'Сумма': sumaries
    }
    return pd.DataFrame.from_dict(result)


print("Чтение вводных csv-файлов...")
start = timer()
# branches = csv_read('t_branches.csv')
wrehouse = csv_read('t_warehouse.csv')
cities = csv_read('t_cities.csv')
products = csv_read('t_products.csv')
sales = csv_read('t_sales.csv')
end = timer()
print(f'''{end - start}
------------------------------------------------''')

# вариант с отбором по значению, без функции
print("Нахождение уникальных складов/магазинов...")
start = timer()
# branches_filter = branches.filter(['Ссылка', 'Наименование', 'Город']).sort_values('Наименование')
# branches_filter['warehouse'] = branches_filter['Наименование'].str.find('клад')
# branches_warehouse = branches_filter.loc[(
#         branches_filter['warehouse'] != -1), ['Ссылка', 'Наименование']]
# branches_shop = branches_filter.loc[(
#         branches_filter['warehouse'] == -1), ['Ссылка', 'Наименование']]
# branches_shop.to_csv('csv/shops')
# branches_warehouse.to_csv('csv/warehouses.csv')
end = timer()
print(f'''{end - start}
------------------------------------------------''')

# print(f'''Десять первых магазинов по количеству продаж...
# ------------------------------------------------''')
# start = timer()
# top_shops = find_top(main_df=sales,
#                      second_df=branches_shop,
#                      name_col_main='Филиал',
#                      name_col_second='Ссылка',
#                      condition_top='Количество')
# end = timer()
# print(f'''{end - start}''')
# print(top_shops)
#
# print(f'''Десять первых складов по количеству продаж...
# ------------------------------------------------''')
# start = timer()
# top_warehouse = find_top(main_df=sales,
#                          second_df=branches_warehouse,
#                          name_col_main='Филиал',
#                          name_col_second='Ссылка',
#                          condition_top='Количество')
# end = timer()
# print(f'''{end - start}''')
# print(top_warehouse)
# print(branches_shop)
# print(branches_warehouse)
print(f'''Десять самых продаваемых товаров по складам...
------------------------------------------------''')
start = timer()
top_products_warehouse = find_top(main_df=sales,
                                  second_df=wrehouse,
                                  name_col_main='Филиал',
                                  name_col_second='Ссылка',
                                  condition_top='Количество',
                                  from_df=True,
                                  products_df=products)
end = timer()
print(f'''{end - start}''')
print(top_products_warehouse)

# print(f'''Десять самых продаваемых товаров по магазинам...
# ------------------------------------------------''')
# start = timer()
# top_products_shop = find_top(main_df=sales,
#                              second_df=branches_shop,
#                              name_col_main='Филиал',
#                              name_col_second='Ссылка',
#                              condition_top='Количество',
#                              from_df=True,
#                              products_df=products)
# end = timer()
# print(f'''{end - start}''')
# print(top_products_shop)
#
# print(f'''Десять городов, в которых больше всего продавалось товаров...
# ------------------------------------------------''')
# start = timer()
# top_cities = find_top(main_df=sales,
#                              second_df=branches,
#                              name_col_main='Филиал',
#                              name_col_second='Ссылка',
#                              condition_top='Количество',
#                              city=True,
#                              third_df=cities)
# end = timer()
# print(f'''{end - start}''')
# print(top_cities)

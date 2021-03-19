"""
файл с примерами исполльзования классов и функций модуля

классы создаются в функции main
пример работы с ними - в функции test_features
"""
from os import path

import source_data.src as sd
import source_data.prepare as prep


_strBasePath=path.join('/home', 'egor', 'git', 'jupyter', 'AIGK', 'DB')
_strSQLpath1=path.join(_strBasePath, 'year.sqlite3')

_strEXCELdb = path.join(_strBasePath, 'bd.xlsx')
_strEXCELexsog_r = path.join(_strBasePath, 'EXOG.xlsx')
_strEXCELmodel = path.join(_strBasePath, 'svod.xlsx')

_lstQuery = ['CPIAv', 'LevelRate', 'loan_rate', 'not_in_source']

def test_features(xsrc:sd.abcDataSource):
    """

    :param xsrc: sd.abcDataSource
        указатель на тестируемый класс (любой потомок класса sd.abcDataSource)
    :return: None
    """
    print('пример для класса', xsrc.name)
    # тут будет ошибка: фрейм не прочитан
    print('тут будет ошибка - фрейма еще нет -> ')
    try:
        print(xsrc.dataset_val)
        print('ЛОЖКА ЕСТЬ!!! ')
    except AttributeError as err:
        print(err)

    pdf = xsrc.make_frame()
    # теперь ошибки не будет
    try:
        print('фрейм - >')
        print(xsrc.dataset_val)
        print('=' * 50)
    except AttributeError as err:
        print(err)

    # для предподготовки данных настраиваем лист функций
    # !!! ФУНКЦИИ ПРИМЕНЯЮТЯ ДЛЯ ПРИМЕРА, СМЫСЛОВОГО ЗНАЧЕНИЯ В ДАННОМ СЛУЧАЕ НЕТ НИКАКОГО !!!
    xsrc.prepare = [{'func': prep.scale, 'list_fields': ['CPIAv', ], 'param': 100},  # колонку CPIAv умножить на 100
                  {'func': prep.add, 'list_fields': ['LevelRate', 'loan_rate'], 'param': 50},
                  # LevelRate и loan_rate и увеличить на 50
                  ]
    print('фрейм с предподготовкой - >')
    print(xsrc.make_frame())
    print('=' * 50)
    print('фрейм с описаниями рядов - >')
    print(xsrc.dataset_pass[['name', 'name', 'unit']])
    print('=' * 50)
    print('поля запрошены - ', xsrc.fields)
    print('поля запрошены - в базе даннх отсутствют (фрейм формируется из тех, которые есть, ошибки не возникает):',
          xsrc.fields_not_in_source)
    print('=' * 50)
    print('check db struct:', xsrc.check(), 'row type:', xsrc.row_type, 'source type:', xsrc.source_type)
    print(xsrc.source_path)
    print('done for', str(xsrc))

    print('=' * 50)

def main():
    # источник - ФАКТИЧЕСКИЕ данные (в случае с данными из sqlite3 разница ТОЛЬКО "идеологическая")
    x1 = sd.db_source(_strSQLpath1, sd.RowTypes.FACT, _lstQuery)
    test_features(x1)

    # источник - ФАКТИЧЕСКИЕ данные (в ЭТОМ случае ВАЖНО)
    xd = sd.excel_source(_strEXCELdb, sd.RowTypes.FACT, _lstQuery) # по умолчанию лист-исходник - year
    test_features(xd)

    # источник - ЭКЗОГЕННЫЕ РЯДЫ
    xer = sd.excel_source(_strEXCELexsog_r, sd.RowTypes.EXOG_R, _lstQuery) # по умолчанию лист-исходник - year
    test_features(xer) # так получилось :)

    # источник - РАСЧСЕТНЫЕ РЯДЫ
    xsv = sd.excel_source(_strEXCELmodel, sd.RowTypes.MODEL, _lstQuery, sheet_name=0)
    test_features(xsv)



if __name__ == '__main__':
    main()

    print('*' * 100)
    print('All done')
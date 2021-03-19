"""Классы для чтения данных из разных источников для моделей АИЭК

Классы загрузку данных из разных источников (sqlite3 и файлы ms excel в определенных форматах).
В один рабочий фрейм данные сводятся в модулях (или страницах Юпитер) непосредственно моделей.

Состав:
 :RowTypes - перечисление  для удбоства задания типа ряда (фактические, экзогенные и т.д.)
 :SourceTypes -  перечисление  для удобства задания типа источника (sqlite или excel, возможны другие источники,
 например, из памяти или сsv-файлов)
 :abcDataSource - абстрактный класс, общий предок для классов загрузки данных. Основная функция, реализуемая в потомках: - маке_frame,
 формирует и возвращает фрейм данных в форматах Питон-моделей
 :db_source - класс для чтения данных из файлов sqlite3, основного источника данных для Питон-моделей
 :excel_source - класс  для чтения данных из файлов MS Excel

"""

import pandas as pd
from sqlalchemy import create_engine, MetaData

from abc import ABC, abstractmethod
from enum import Enum
from os import path
import re

class RowTypes(Enum):
    """Перечисление задает константы-флаги для удобства установки или определения типа загруженных рядов"""

    # флаг радов из базы данныз, фактических
    FACT = 0

    # флаг экзогенных ПАРАМЕТРОВ
    EXOG_P = 1

    # флаг экзогенных РЯДОВ
    EXOG_R = 2

    # флаг источника - результатов расчета других моделей
    MODEL = 3

class SourceTypes(Enum):
    """Перечисление задает константы-флаги для удобства установки или определения физического источника загруженных рядов"""

    SQLITE = 0
    EXCEL = 1

class abcDataSource(ABC):
    """класс-предок для классов источников данных разных форматов

    ...

    Атрибуты (класс абстрактный, но некоторые атрибуты используются в общих для всей цепочки наследования функциях)
    --------
    name : str
        имя класса
    _row_type : RowTypes
        тип читаемого ряда - фактические, экзогенные ряды или параметры, расчетные. Важный параметр для последующего сведения в рабочий фрейм
    _source_type : SourceTypes
        источник данных, файлы sqlite3 или MS Excel. Информационный атрибут
    _srcSourcePath : str
        пусть к файлу-источнику
    _lstFields : list
        список запрашиваемых полей. Хранит полный список, включая те, которых нет в источнике (запрашиваемые поля)
    _prepare : list(dict('func':<указатель на функцию>, 'list_fields':[<список полей, по которым функцияотработает>], 'param':<параметр функции>))
        список с функциями, выполняемыми последовательно над заданными полями подготовленного фрейма с данными
        функции использовать ТОЛЬКО из файла prepare.py этого модуля!!!!
    TODO: МЕХАНИЗМ ПРЕДПОДГОТОВКИ ДАННЫХ НУЖНО ДОРАБОТАТЬ
    _pdf : pandas DataFrame
        подготовленный считаный из источника ряд

    Свойства
    ---------
    table : str
        уточнение источника данных: для sqlite - SQL-запрос к базе, для Excel - имя листа
    dataset_pass : pandas DataFrame
        фрейм с описанием прочитанных рядов
    dataset_val : pandas DataFrame
        конечный готовый фрейм с данными
    fields_not_in_source : list
        список запрошенных полей, отсутствующих в источнике
    fields : list
        список запрошенных полей
    source_path : str
        путь к файлу источнику

    Функции
    -------
    check : bool
        проверка формата источника
    make_frame : pandas DataFrame
        конечный подготовленный и отформатированный ряд с данными

    """
    @property
    @abstractmethod
    def table(self)->str:
        """для sqlite - запрос, для excel - имя листа"""
        pass

    @abstractmethod
    def check(self)->bool:
        """проверка формата данных"""
        pass

    @property
    def fields_not_in_source(self):
        return list(set(self._lstFields) - set(self.dataset_val.columns.tolist()))

    @property
    @abstractmethod
    def dataset_pass(self)->pd.DataFrame:
        """возврат описаний рядов"""
        pass

    @property
    def dataset_val(self)->pd.DataFrame:
        """возврат конечного ряда"""
        assert type(self._pdf) is not None, 'фрейм не считан: для чтения фрейма необходимо вызвать функцию make_frame'

        return self._pdf

    @abstractmethod
    def make_frame(self)->pd.DataFrame:
        """формирование рабочего фрейма"""
        pass

    @property
    def source_type(self)->SourceTypes:
        return self._source_type

    @property
    def fields(self) -> list:
        return self._lstFields

    @property
    def row_type(self)->RowTypes:
        return self._row_type

    @property
    def source_path(self) -> str:
        return self._srcSourcePath

    @property
    def prepare(self)->list:
        return self._prepare

    @prepare.setter
    def prepare(self, list_func_params):
        """задает список функций предподготовки данных

        Свойство сохраняет список функций, применяемых по выбранным полям с определенными параметрами
        список должен бысть состоять из строго заданных словарей с ключами
        :func - указатель на функцию
        :list_fields - список полей окончательного фрейма, по которым выполняется функция
        :param - параметр функции

        например: {'func': np.log, 'list_fields=['a', 'b', 'c'], 'param':None}
        при формировании рабочего фрейма к полям f, b, c применится функция np.log без параметра
        """
        if type(list_func_params)==list:
            self._prepare=list_func_params
        else:
            self._prepare = [list_func_params,]

    def __str__(self)->str:
        return '''{_name}: 
    data from {_from}, 
    row type {_type}, 
    data source {_source}'''.format(_from=self.source_type.name, _type=self.row_type.name,
                                    _source=self.source_path, _name=self.name)

class db_source(abcDataSource):
    """класс для чтения данных из sqlite

    Предполагается, что фактические, экзогенные и модельные данные хранятся в отдельных файлах sqlite с идентичной структурой:
     - таблица datas с данными. Поля
       code : int - внешний ключь для связи с таблицей заголовков
       data : int - год точки
       value : float - значение

    - таблица headers c заголовками рядов. Поля
       code2 : str - код ряда (машинное название - соответствует коду ряда в модели Ексел)
       code : int - первичный ключь
       mgroup_id : int- id группы
       name : text- человеческое название ряда
       unit : str - единица измерений
       source : text- источник данных (интернет адрес или просто название источника)
       params :json - параметры расчета сезонности

    Класс читает данные, формирует рабочий фрейм в широком формате, последовательно применяет к выбраннымполям функции из списка prepare

    Атрибуты
    --------
    _strQuery : str
        строка-шаблон SQL-запроса к базе данных, статический
    _strDataTable : str
        строка-имя таблицы со значениями в базе данных, статический
    _strHearedsTable : str
        строка-имя таблицы сописаниями рядов в базе данных, статический
    _lstDataTableColumns : list
        список полей таблицы значений в бд (используется для проверки формата), статический
    _lstHeaderTableColumns : list
        список полей таблицы описания радов в бд (используется для проверки формата), статический
    sql_engine : sqlalchemy engine
        одключение к базе даных
    _whereCond : str
        строка с условием WHERE SQL запроса

    Свойства
    --------
    table : str
        готовый SQL-запрос к базе даных

    """

    # шаблон запросов к бд
    _strQuery='''select {data_table}.date, {data_table}.value, {headers_table}.code2 
from {data_table} join {headers_table} on {data_table}.code = {headers_table}.code {where_condition}'''
    strQueryPass='''select {headers_table}.* from {headers_table} {where_condition}'''

    # названия таблиц - константы
    _strDataTable='datas'
    _strHearedsTable='headers'

    # списки полей - константы
    _lstDataTableColumns=['code', 'date', 'value']
    _lstHeaderTableColumns = ['code', 'mgroup_id', 'name', 'unit', 'code2', 'source', 'params']

    def __init__(self, strPath:str, row_type:RowTypes, lstFields:list):
        """

        :param strPath: str
            пусть к файлу источнику, используется для создания подключения (engine
        :param row_type: RowTypes
            тип ряда данных - фактический, экзогенный или модельный
        :param lstFields: list
            список кодов (поле code2 таблицы headers бд) для выборки. Может быть строкой - выборка одного ряда
        """
        assert isinstance(row_type, RowTypes), 'wrong type for param row_type'
        assert type(lstFields) in (str, list, type), 'wrong type for params lstFileds - must be code2 for sqlite'
        assert path.isfile(strPath), 'file {} not found'.format(strPath)

        self.name = 'AIGK sqlite-data source class'
        self._row_type=row_type
        self._srcSourcePath=strPath
        self._sql_engine=create_engine('sqlite+pysqlite:///{}'.format(self.source_path))
        self._lstFields=lstFields
        self._source_type = SourceTypes.SQLITE
        self._prepare = None
        if type(lstFields)==str:
            self._whereCond='where {headers_table}.code2 == "{code2}"'.format(headers_table=db_source._strHearedsTable,
                                                                            code2=lstFields)
        else:
            self._whereCond = 'where {headers_table}.code2 in {codes2}'.format(headers_table=db_source._strHearedsTable,
                                                                           codes2=tuple(lstFields))
    def check(self):
        """проверка структуры файла бд по наличию таблиц и полей в таблицах"""

        # на самом деле проверка не очень нужна - при неправильной структуре будет ошибка
        MD=MetaData()
        MD.reflect(bind=self._sql_engine)
        try:
            cond1 = {c.name for c in MD.tables[db_source._strDataTable].columns} == set(db_source._lstDataTableColumns)
            cond2 = {c.name for c in MD.tables[db_source._strHearedsTable].columns} == set(db_source._lstHeaderTableColumns)
            return cond1 and cond2
        except KeyError:
            return False


    @property
    def table(self):
        """возвращает подготовленный sql-запрос к базе даных"""
        return db_source._strQuery.format(data_table=db_source._strDataTable,
                                     headers_table=db_source._strHearedsTable,
                                     where_condition=self._whereCond)

    @property
    def dataset_pass(self):
        """возвращает фрейм с заголовками выбранных рядов - описания рядов"""
        return pd.read_sql(db_source.strQueryPass.format(headers_table=db_source._strHearedsTable,
                                                         where_condition=self._whereCond),  con=self._sql_engine).set_index('code2')

    def make_frame(self):
        """возвращает фрейм подготовленный данных

        Разворачивает данные в широкую форму, ставит индексом даты (год точки),
        последовательно применяет функции из списка prepare к заданным полям"""
        self._pdf = pd.read_sql(self.table, con=self._sql_engine).set_index(['date', 'code2']).unstack().reset_index().set_index('date')
        self._pdf.columns=[c[1] for c in self._pdf.columns]
        if self._prepare:
            for i in self._prepare:
                self._pdf=self._pdf.pipe(i['func'], i['list_fields'], i['param'])
        return self._pdf


class excel_source(abcDataSource):
    """класс для чтения данных из MS Excel

       класс работает с двумя форматами листов моделей в форме Эксел: "старый" формат, используемый в "базеданных" и
       "новый формат" последних версий моделей. В обоих форматах данные "растут" вправо (широкий формат), первые колонки содержат описания рядов
       Для "старого формата" это:
            (программные аналоги русских наименований) 'code', 'name', 'unit', 'code2', 'comments', 'last_date', т.е.
            код (простой счетчик), наименование, единица измерения, символьный код ряда, комментарии, последняя точка ряда
       Для "нового формата" это
            (программные аналоги русских наименований) 'code', 'name', 'code2', 'source', 'type', 'bd', 'unit' т.е.
            код (простой счетчик), наименование, символьный код ряда, источник данных, тип переменных, база данных, единица измерения

        - таблица datas с данными. Поля
          code : int - внешний ключь для связи с таблицей заголовков
          data : int - год точки
          value : float - значение

       - таблица headers c заголовками рядов. Поля
          code2 : str - код ряда (машинное название - соответствует коду ряда в модели Ексел)
          code : int - первичный ключь
          mgroup_id : int- id группы
          name : text- человеческое название ряда
          unit : str - единица измерений
          source : text- источник данных (интернет адрес или просто название источника)
          params :json - параметры расчета сезонности

       Класс читает данные, формирует рабочий фрейм в широком формате, последовательно применяет к выбраннымполям функции из списка prepare

       Атрибуты
       --------
       _sheet_name : str | int
            имя листа или номер листа-источника книги MS Excel

       Свойства
       --------
       table : str | int
           параметр для pd.read_excel, sheet_name - имя листа или номер листа источника

        Функции
        -------
        _read_db_format : pandas DataFrame
            читает ряд из Эксел "старого формата", возвращает считанный (НЕ окончательный) фрейм
        _read_work_format : pandas DataFrame
            читает ряд из Эксел "нового формата", возвращает считанный (НЕ окончательный) фрейм

       """
    def __init__(self,  strPath:str, row_type:RowTypes, lstFields:list, sheet_name='YEAR'):
        """

        :param strPath: str
            пусть к файлу источнику, используется для создания подключения (engine
        :param row_type: RowTypes
            тип ряда данных - фактический, экзогенный или модельный
        :param lstFields: list
            список кодов (поле code2 таблицы headers бд) для выборки. Может быть строкой - выборка одного ряда
        :param sheet_name: str | int
            имя или номер листа-итсоника книги эксел
        """
        assert isinstance(row_type, RowTypes), 'wrong type for param row_type'
        assert type(lstFields) in (str, list, type), 'wrong type for params lstFileds - must be code2 for sqlite'
        assert path.isfile(strPath), 'file {} not found'.format(strPath)

        self.name = 'AIGK excel-data source class'
        self._row_type = row_type
        self._srcSourcePath = strPath
        self._lstFields = lstFields
        self._source_type = SourceTypes.EXCEL
        self._prepare = None
        self._sheet_name=sheet_name

    def check(self):
        """проверка структуры файла бд по наличию таблиц и полей в таблицах"""
        # на самом деле проверка не очень нужна - при неправильной структуре будет ошибка
        pass

    @property
    def table(self):
        """возвращает имя или номер листа-источника данных из книги Ексел"""
        return self._sheet_name

    @property
    def dataset_pass(self):
        """возвращает фрейм с заголовками выбранных рядов - описания рядов"""
        return self._pdf_heads

    def _read_db_format(self):
        """читаем ексел-файл базы данных - формат отличается от рабочего
            - сверху данные смещены на 2 строки
            - другие название описательных колонок, и
            - другое их количество
        """
        _head_columns = ['code', 'name', 'unit', 'code2', 'comments', 'last_date']
        _pdf = pd.read_excel(self.source_path, sheet_name=self.table, skiprows=2)

        data_cols = [int(c) for c in _pdf.columns if re.search('\d{4}', str(c))]
        _pdf.columns = _head_columns + data_cols

        self._pdf_heads = _pdf.loc[_pdf['code2'].isin(self.fields), _head_columns[:-2]].set_index('code2')
        return _pdf

    def _read_work_format(self):
        """читаем ексел-файл рабочего формата - отличается от базы данных"""

        _head_columns = ['code', 'name', 'code2', 'source', 'type', 'bd', 'unit']
        _pdf = pd.read_excel(self.source_path, sheet_name=self.table)

        data_cols = [int(c) for c in _pdf.columns if re.search('\d{4}', str(c))]
        _pdf.columns = _head_columns + data_cols

        self._pdf_heads = _pdf.loc[_pdf['code2'].isin(self.fields), _head_columns].set_index('code2')
        return _pdf

    def make_frame(self):
        """возвращает фрейм данных. Разворачивает данные в широкую форму, ставит индексом даты (год точки)"""

        if self.row_type==RowTypes.FACT:
            _pdf = self._read_db_format()
        else:
            _pdf =  self._read_work_format()

        data_cols = [int(c) for c in _pdf.columns if re.search('\d{4}', str(c))]
        lstFields = _pdf.loc[_pdf['code2'].isin(self.fields), 'code2'].tolist()
        self._pdf = _pdf.rename(columns={'code2': 'date'}).set_index('date').loc[lstFields, data_cols].T
        return self._pdf


def read_sql():
    x1=db_source(path.join(r'/home/egor/git/jupyter/AIGK', 'DB', 'year.sqlite3'), RowTypes.FACT, ['CPIAv', 'LevelRate', 'loan_rate', 'not_in_sheet'])
    # x1.prepare = {'func':prep.scale, 'list_field': ['CPIAv', ], 'param':10}
    print(x1.make_frame())
    print(x1.dataset_pass.columns)
    print('not in query', x1.fields_not_in_source)
    print(x1.check())

def read_excel():
    x1=excel_source(path.join(r'/home/egor/git/jupyter/AIGK', 'DB', 'svod.xlsx'),
                    RowTypes.MODEL, ['CPIAv', 'LevelRate', 'loan_rate', 'GDP_Iq', 'not_in_sheet'], sheet_name=0)
    # x1.prepare = {'func':prep.scale, 'list_field': ['CPIAv', ], 'param':10}
    pdf=x1.make_frame()
    print(pdf)
    print(x1.dataset_pass)
    print('not in query', x1.fields_not_in_source)
    print(x1.check())


if __name__=='__main__':
    read_excel()
    # read_sql()
    print('All done')


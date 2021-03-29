import sqlalchemy as sa
import warnings
import os
import sys
from os import path, getcwd
import datetime as dt
from scipy.stats import rv_continuous
from scipy.integrate import quad
import numpy as np
import pandas as pd
from source_data.src import DBSource, RowTypes
import source_data.prepare as prep

warnings.filterwarnings('ignore')

module_path = os.path.abspath(os.path.join('..', 'PY'))

if module_path not in sys.path:
    sys.path.append(module_path)


class Common:
    """

    """

    def __init__(self, str_year_db_path=path.join('..', 'DB', 'year.sqlite3'),
                 str_ex_year_db_path=path.join('..', 'DB', 'exog_year.sqlite3'),
                 str_ex_param_db_path=path.join('..', 'DB', 'exog_param.sqlite3'),
                 str_svod_db_path=path.join('..', 'DB', 'svod.sqlite3'),
                 str_data_table='datas',
                 str_header_table='headers', ):
        """

        :param str_year_db_path:путь к рабочей базе данных фактических рядов SQLite
        :param str_ex_year_db_path:путь к рабочей базе данных экзогенных переменных
        :param str_ex_param_db_path:путь к рабочей базе данных экзогенных параметров, задаваемых вручную
        :param str_svod_db_path:путь к рабочей базе данных итоговых рядов SQLite
        # константы базы данных SQLite3
        :param str_data_table:название таблицы с данными в базе данных SQLite
        :param str_header_table:название таблицы с текстовыми и кодовыми именами рядов в базе данных SQLite
        """
        self.str_year_db_path = str_year_db_path
        self.str_ex_year_db_path = str_ex_year_db_path
        self.str_ex_param_db_path = str_ex_param_db_path
        self.str_svod_db_path = str_svod_db_path

        self.str_data_table = str_data_table
        self.str_header_table = str_header_table

    def make_select_year_string(self, row_codes, ):
        """

        :param row_codes:
        :return:
        """
        data_table = self.str_data_table
        header_table = self.str_header_table
        assert type(row_codes) in (
        str, list, tuple), 'make_select_year_string from() COMMON module: error in param type'
        if type(row_codes) == str:
            where_condition = 'where {0}.code2=="{1}"'.format(header_table, row_codes)
        elif type(row_codes) in (tuple, list):
            where_condition = 'where {0}.code2 in {1}'.format(header_table, tuple(row_codes))

        str_select = '''select {0}.date, {0}.value, {1}.code2 from {0} join {1} on {0}.code={1}.code {2}'''.format(
            data_table, header_table, where_condition)

        return str_select

    @staticmethod
    def make_frame(pdf):
        """

        :param pdf: Pandas DataFrame
        :return: Pandas DataFrame with correct index & columns
        """
        k = pdf.set_index(['date', 'code2']).unstack().reset_index().set_index('date')
        k.columns = [c[1] for c in k.columns]
        return k

    @staticmethod
    def scale(pdf, list_fields=None, multiplier=1):
        """

        :param pdf: Pandas DataFrame
        :param list_fields: list of column names
        :param multiplier: multiplier for the specified columns
        :return: Pandas DataFrame with the specified columns scaled
        """
        if list_fields is None:
            list_fields = []
        pdf[list_fields] *= multiplier
        return pdf

    @staticmethod
    def add(pdf, list_fields=None, param=0):
        """

        :param pdf: Pandas DataFrame
        :param list_fields: list of column names
        :param param: term to be added to the specified columns
        :return: Pandas DataFrame with the specified columns adjusted
        """
        if list_fields is None:
            list_fields = []
        pdf[list_fields] += param
        return pdf

    @staticmethod
    def combine_frames(*lst_source_data: DBSource):
        """
        правильно соединяет все источники данных для моделей
        основной фрейм - база данных, если в нем что-либо равно np.nan то
        для таких точек значения берутся из фрейма экзогенных рядов. Если после этого в результате есть np.nan то
        для таких точек значения берутся из фрейма экзогенных параметров. Если после этого в результате есть np.nan
        то для таких точек значения берутся из фрейма модельных расчетов
        если какой-либо фрейм остуствует, он, понятное дело, не комбинируется


        :param lst_source_data: переменные класса source_data.src.DBSource (не меньше одного, не больше 4)
        :return: pandas DataFrame, собранный и готовый
        """

        def _combine(pdf, dct, new_frame_key):
            try:
                return pdf.combine_first(dct[new_frame_key].make_frame())
            except KeyError:
                return pdf

        lst_keys = [i.row_type.name for i in lst_source_data]

        if len(lst_keys) != len(set(lst_keys)):
            _e = [(v.row_type.name, i) for i, v in enumerate(lst_source_data)]
            raise KeyError('Несколько входных переменный имеют одинаковый row_type: {}'.format(_e))

        _dct = {i.row_type.name: i for i in lst_source_data}
        _pdf = pd.DataFrame(None)
        _pdf = _combine(_pdf, _dct, 'FACT')
        _pdf = _combine(_pdf, _dct, 'EXOG_R')
        _pdf = _combine(_pdf, _dct, 'EXOG_P')
        _pdf = _combine(_pdf, _dct, 'MODEL')

        return _pdf

    @staticmethod
    def update_dt_s(datetime_value=dt.datetime.now()):
        return datetime_value.strftime('%Y-%m-%d %H:%M:%S %f')

    @staticmethod
    def update_dt_d(string_value=None):
        assert string_value is not None, r"string_value - строго datetime в формате %Y-%m-%d %H:%M:%S %f"
        s = string_value.split(' ')
        l_dt = s[0].split('-')
        l_tm = s[1].split(':')
        i_msec = int(s[2])
        return dt.datetime(year=int(l_dt[0]), month=int(l_dt[1]), day=int(l_dt[2]),
                           hour=int(l_tm[0]), minute=int(l_tm[1]), second=int(l_tm[2]), microsecond=int(i_msec))


class KolmakovGen(rv_continuous):
    """
    Распределения доходов населения по уровню
    среднедушевого среднемесячного денежного дохода в денежном выражении
    """

    def _pdf(self, x, sigma, mu):
        Px = np.exp(-(np.log(x) - mu) ** 2 / (2 * sigma ** 2))
        return Px / (sigma * np.sqrt(2 * np.pi))

    #         return lognorm.pdf(x, s=sigma, scale=np.exp(mu)) * x

    def _cdf(self, x, sigma, mu):
        return quad(self._pdf, 0, x, args=(sigma, mu))[0]


if __name__ == "__main__":
    # %%script false --no-raise-error

    list_year_codes = ['squareddy_ss_x', 'CPIAv', 'Pop_x', 'HHAv', 'Unmpl_s', 'DispInc_rI', 'Inc_x', 'p_ProbDef',
                       'UZUss_x', 'MEPop', 'DispPop', 'LivMin', 'p_superrich', 'sdelkiddy_ss_x', 'sdelkikp_ss_x',
                       'squarekp_ss_x', 'DispInc_I', 'oldpravaddy_x', 'longarenda', 'izavarijnogo',
                       'kapremont', 'partinZSK', 'Badzhilfond_x',
                       'sdelkikp_x']  # список экзогенных и фактических данных

    list_svod = ['price1mddy_alt_x', 'price1mall_alt_x', 'AvSqDdy', 'AvSqVtor', 'VvodyIZDunits', 'VvodyMKD',
                 'VvodyMKD_inst', 'VvodyMKD_gov', 'VvodyIZD', 'VvodyMKDunits', 'sdelkikp_x', 'sdelkiddy_x']  # ре
    common = Common(path.join('..', 'DB', 'year.sqlite3'))
    source_active = DBSource(common.str_year_db_path, RowTypes.FACT, list_year_codes)
    source_active.prepare = [{'func': prep.scale, 'list_fields': ['Inc_x', ], 'param': 1e3},
                             {'func': prep.scale, 'list_fields': ['sdelkiddy_ss_x', ], 'param': 1e6}]

    source_exogenous = DBSource(common.str_ex_year_db_path, RowTypes.EXOG_R, list_year_codes)

    source_exog_parameters = DBSource(common.str_ex_param_db_path, RowTypes.EXOG_P, list_year_codes)

    srcSvod = DBSource(common.str_svod_db_path, RowTypes.MODEL, list_svod)
    srcSvod.prepare = {'func': prep.scale, 'list_fields': ['price1mddy_alt_x', 'price1mall_alt_x'], 'param': 1e3}

    pdfWork = (source_active.make_frame().combine_first(source_exogenous.make_frame())
               .combine_first(source_exog_parameters.make_frame())
               .combine_first(srcSvod.make_frame()))

    common.combine_frames(source_active, source_exogenous, source_exog_parameters, srcSvod)

    kolmakov = KolmakovGen(a=0.0, name='kolmakov', shapes='sigma, mu')

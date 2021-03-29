import pandas as pd
import numpy as np
import sqlalchemy as sa
import patsy
import statsmodels.api as sm
import statsmodels.formula.api as smf
from common import Common
import os
from scipy.stats import norm


class DemandOM:
    # Размышления  о static class variables, instances variables & belongs to class vs belongs to object instance
    # Меня очень смущает ваш подход с отказом от сущности объекта, принадлежащему классу (object instance)
    # Концептуально, классы (типы) — это объектные фабрики.
    # Их главная задача — создавать объекты, обладающие определенным поведением.
    # Ваш код же использует фабрику для решения типовых задач -
    # в результате нельзя пользоваться несколькими сущностями одного класса, для разных условий.
    # В частности, этот подход по сути убивает возможность инициализироваться из разных источников.
    # Другой момент - полная смесь namespaces классов, функций и сущностей.
    # Переписываемое значение static class variables меняется во всех сущностях этого класса, затрудняя дебаг и тесты
    pdfWork = None

    lstYearCodes = ['squareddy_ss_x', 'CPIAv', 'Pop_x', 'HHAv', 'Unmpl_s', 'DispInc_rI', 'Inc_x', 'p_ProbDef',
                    'UZUss_x', 'MEPop', 'DispPop', 'LivMin', 'p_superrich', 'sdelkiddy_ss_x', 'sdelkikp_ss_x',
                    'squarekp_ss_x', 'DispInc_I', 'oldpravaddy_x', 'longarenda', 'izavarijnogo',
                    'kapremont', 'partinZSK', 'Badzhilfond_x', 'sdelkikp_x']  # список экзогенных и фактических данных

    lstSvod = ['price1mddy_alt_x', 'price1mall_alt_x', 'AvSqDdy', 'AvSqVtor', 'VvodyIZDunits', 'VvodyMKD',
               'VvodyMKD_inst', 'VvodyMKD_gov', 'VvodyIZD', 'VvodyMKDunits', 'sdelkikp_x',
               'sdelkiddy_x']  # результаты из блока "Цены и себестоимость"

    def __init__(self):
        assert False, r'you can\'t create variables of DemandOM class!'

    @staticmethod
    def MakeWorkFrame(common,
                      conWork,# connection к рабочей базе данных
                      conWorkEx,# connection к к рабочей базе данных экзогенных переменных
                      conWorkExH, # connection к рабочей базе данных экзогенных параметров
                      conWorkSvod, # connection к рабочей базе данных СВОД
                      ):
        ''' Загрузка исходных данных
            Исходные данные:
                Фактические значения - из базы данных year.sqlite3;
                Экзогенные - из базы данных exog_year.sqlite3
                Задаваемые вручную - из базы данных exog_param.sqlite3
                Результаты других моделей - из базы данных svod.sqlite3.
        '''

        pdfAct = (pd.read_sql(common.make_select_year_string(DemandOM.lstYearCodes), con=conWork)
                  .pipe(common.make_frame)
                  .pipe(common.scale, list_fields=['Inc_x', ], multiplier=1e3)
                  .pipe(common.scale, list_fields=['sdelkiddy_ss_x', ], multiplier=1e-6))
        pdfExog = pd.read_sql(common.make_select_year_string(DemandOM.lstYearCodes), con=conWorkEx).pipe(
            common.make_frame)
        PdfExogHandle = pd.read_sql(common.make_select_year_string(DemandOM.lstYearCodes), con=conWorkExH).pipe(
            common.make_frame)

        pdfSvod = (pd.read_sql(common.make_select_year_string(DemandOM.lstSvod), con=conWorkSvod)
                   .pipe(common.make_frame)
                   .pipe(common.scale, list_fields=['price1mddy_alt_x', 'price1mall_alt_x'], multiplier=1e3))

        DemandOM.pdfWork = pdfAct.combine_first(pdfExog).combine_first(PdfExogHandle).combine_first(pdfSvod).pipe(
            common.scale, list_fields=['Unmpl_s', ], multiplier=100)
        DemandOM.pdfWork['_cnt_deal_om'] = DemandOM.pdfWork['sdelkiddy_ss_x'] + DemandOM.pdfWork['sdelkikp_ss_x']
        return DemandOM.pdfWork


if __name__ == '__main__':
    common = Common(str_year_db_path=r'DB/year.sqlite3',
                    str_ex_year_db_path=r'DB/exog_year.sqlite3',
                    str_ex_param_db_path=r'DB/exog_param.sqlite3',
                    str_svod_db_path=r'DB/svod.sqlite3')
    full_db_path = 'sqlite+pysqlite:///C:/Users/amsergeev/Documents/Модель_ЦМАКП/{}'
    conWork = sa.create_engine(full_db_path.format(common.str_year_db_path))
    conWorkEx = sa.create_engine(full_db_path.format(db_name=common.str_ex_year_db_path))
    conWorkExH = sa.create_engine(full_db_path.format(db_name=common.str_ex_year_db_path))
    conWorkSvod = sa.create_engine(full_db_path.format(db_name=common.str_svod_db_path))

    iFirstFactYear = 2008
    iLastFactYear = 2019

    iFirstForecastYear = iLastFactYear + 1
    iLastForecastYear = 2030

    pdfAct = (
        pd.read_sql(common.make_select_year_string(DemandOM.lstYearCodes), con=conWork))  # .pipe(common.make_frame))
    pdfAct[pdfAct[['date', 'code2']].duplicated(keep=False)]

    DemandOM.MakeWorkFrame(common, conWork, conWorkEx, conWorkExH, conWorkSvod)

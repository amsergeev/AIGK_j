import unittest
from source_data.src import db_source, excel_source, RowTypes, SourceTypes
from os import path

class UT_sourcedata(unittest.TestCase):
    _strDBPath = path.join('/home', 'egor', 'git', 'jupyter', 'AIGK', 'DB')

    strSQLitePath = path.join(_strDBPath, 'year.sqlite3')
    strXLSX_BDPath   = path.join(_strDBPath,  'bd.xlsx')
    strXLSX_EXOGPath = path.join(_strDBPath, 'EXOG.xlsx')
    strXLSX_SVODPath = path.join(_strDBPath,  'svod.xlsx')

    lstReadFields = ['CPIAv', 'LevelRate', 'loan_rate', 'not_in_sheet']

    def test_read_sqlite(self):
        x1 = db_source(UT_sourcedata.strSQLitePath, RowTypes.FACT, UT_sourcedata.lstReadFields)
        x1.make_frame()
        self.assertEqual(x1.fields_not_in_source[0], UT_sourcedata.lstReadFields[-1])

    def test_read_excel_bd(self):
        x1 = excel_source(UT_sourcedata.strXLSX_BDPath, RowTypes.FACT, UT_sourcedata.lstReadFields)
        x1.make_frame()
        self.assertEqual(x1.fields_not_in_source[0], UT_sourcedata.lstReadFields[-1])

    def test_read_excel_exog(self):
        x1 = excel_source(UT_sourcedata.strXLSX_EXOGPath, RowTypes.EXOG_R, UT_sourcedata.lstReadFields)
        x1.make_frame()
        self.assertEqual(set(x1.fields_not_in_source), set(UT_sourcedata.lstReadFields[-2:]))

    def test_read_model(self):
        x1 = excel_source(UT_sourcedata.strXLSX_SVODPath, RowTypes.MODEL, UT_sourcedata.lstReadFields, sheet_name=0)
        x1.make_frame()
        self.assertEqual(set(x1.fields_not_in_source), set(['not_in_sheet', 'CPIAv', 'GDP_Iq', 'LevelRate']))
    #
    # def read_excel():
    #
    #     # x1.prepare = {'func':prep.scale, 'list_field': ['CPIAv', ], 'param':10}
    #     pdf = x1.make_frame()
    #     print(pdf)
    #     print(x1.dataset_pass)
    #     print('not in query', x1.fields_not_in_source)
    #     print(x1.check())


if __name__ == '__main__':
    unittest.main()

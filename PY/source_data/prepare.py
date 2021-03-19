"""
модуль предподготовки данных для рабочего фрейма модели АИЖК
в модуле собраны функции применяемые по полям готового рабочего фрейма перед его окончательным использованием
"""

def scale(pdf, list_fields=[], param=1):
    pdf[list_fields] *=  param
    return pdf

def add(pdf, list_fields=[], param=0):
    pdf[list_fields] +=  param
    return pdf


def main():
    pass

if __name__=='__main__':
    main()
    print('All done')
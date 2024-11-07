# -*- coding: utf-8 -*-
import pandas as pd
from src.support import Support
import os

class DataframesInput:

    max_recs = None # None = all recs
    sep = None
    descricao = None
    composicao = None
    referencia_temporal = None
    cenario = None
    valores = None
    proporcionalidades = None
    pre_processing = None



    @classmethod
    def loadData(cls, input_file: dict):
        try:
            fname = fr"{Support.path}{os.sep}{input_file['name']}.{Support.extension}"
            if Support.verbose:
                print(f'Carregando {fname}... '),
            if Support.extension == 'csv':
                df = pd.read_csv(fname, nrows=cls.max_recs)
            else:
                df = pd.read_excel(fname, nrows=cls.max_recs)
            if len(df) == 0:
                if not input_file['mandatory']:
                    return None
                else:
                    raise Exception(f"Obrigatório o fornecimento de dados de {input_file['name']}")
            else:
                if Support.verbose:
                    print(f'Carregado.')
                return df
        except Exception as e:
            if not input_file['mandatory']:
                return None
            else:
                raise Exception(f"Erro obtendo os dados de {input_file['name']}: {e}")

    @classmethod
    def loadSep(cls):
        cls.sep = cls.loadData(Support.input_files[0])
        Support.sep_id = cls.sep.iloc[0].id
        pass

    @classmethod
    def loadDescricao(cls):
        cls.descricao = cls.loadData(Support.input_files[1])

    @classmethod
    def loadComposicao(cls):
        cls.composicao = cls.loadData(Support.input_files[2])

    @classmethod
    def loadReferenciaTemporal(cls):
        cls.referencia_temporal = cls.loadData(Support.input_files[3])

    @classmethod
    def loadCenario(cls):
        cls.cenario = cls.loadData(Support.input_files[4])

    @classmethod
    def loadValores(cls):
        cls.valores = cls.loadData(Support.input_files[5])

    @classmethod
    def loadProporcionalidades(cls):
        cls.proporcionalidades = cls.loadData(Support.input_files[6])

    @classmethod
    def loadDataFrames(cls):
        #cls.loadSep()
        cls.loadDescricao()
        cls.loadComposicao()
        cls.loadReferenciaTemporal()
        cls.loadCenario()
        cls.loadValores()
        cls.loadProporcionalidades()

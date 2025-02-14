import pandas as pd
import numpy as np
from numpy import dtype
import os

cnpq_dtypes = {'ANO-PAGAMENTO': 'category',
 'CODIGO-DO-PROCESSO': dtype('O'),
 'MODALIDADE-DO-PROCESSO-ID': dtype('int64'),
 'NATUREZA-JURIDICA-ID': dtype('int64'),
 'NOME-COMPLETO-ID': dtype('int64'),
 'NOME-CURSO-ID': dtype('int64'),
 'NOME-DA-AREA-DO-CONHECIMENTO-ID': dtype('int64'),
 'NOME-DA-ESPECIALIDADE-ID': dtype('int64'),
 'NOME-DA-SUB-AREA-DO-CONHECIMENTO-ID': dtype('int64'),
 'NOME-GRANDE-AREA-DO-CONHECIMENTO-ID': dtype('int64'),
 'NRO-ID-CNPQ': dtype('O'),
 'PAIS-INSTITUICAO-ID': dtype('int64'),
 'PAIS-NASCIMENTO-ID': dtype('int64'),
 'QUANTIDADE-BOLSA-ANO': dtype('float64'),
 'QUANTIDADE-MESES-PAGOS': dtype('float64'),
 'SEXO': 'category',
 'SIGLA-INSTITUICAO-ID': dtype('int64'),
 'SIGLA-UF-INSTITUICAO': 'category',
 'TITULO-DO-PROCESSO-ID': dtype('int64'),
 'VALOR-PAGO': dtype('float64')}

basepath = os.path.dirname(os.path.abspath(__file__))

def read_cnpq(basepath=basepath):

    # leitura de arquivo .CSV
    cnpq = pd.read_csv(os.path.join(basepath, "dataset.csv"), 
                      decimal=".", 
                      dtype=cnpq_dtypes)

    # reconstitui as colunas normalizadas
    for fk in (col for col in cnpq.columns if col.endswith("-ID")):
        
        filepath = os.path.join(basepath, 
                                "{}.csv".format(fk.rsplit("-", maxsplit=1)[0]))
        col_df = pd.read_csv(filepath)
        cnpq = pd.merge(cnpq, col_df)
        cnpq.drop(fk, axis=1, inplace=True)
    
    return cnpq


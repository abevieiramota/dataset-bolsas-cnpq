from lxml import etree
from lxml import objectify
import pandas as pd
import os
import re
import logging
import sys
from sklearn.preprocessing import LabelEncoder
import zipfile
import io

class BolsasCNPqExtractor:

    COLS = ['ANO-PAGAMENTO',
            'MODALIDADE-DO-PROCESSO',
            #'MOEDA', removido por, à época da criação do script ser tudo REAL
            'NOME-COMPLETO',
            'NOME-CURSO',
            'NOME-DA-AREA-DO-CONHECIMENTO',
            'NOME-DA-ESPECIALIDADE',
            'NOME-DA-SUB-AREA-DO-CONHECIMENTO',
            'NOME-GRANDE-AREA-DO-CONHECIMENTO',
            'PAIS-INSTITUICAO',
            'PAIS-NASCIMENTO',
            'QUANTIDADE-MESES-PAGOS',
            'QUANTIDADE-BOLSA-ANO',
            'SEXO',
            'SIGLA-INSTITUICAO',
            'SIGLA-UF-INSTITUICAO',
            'TITULO-DO-PROCESSO',
            'VALOR-PAGO']

    CAT_COLS = ['NOME-CURSO',
                'NOME-DA-AREA-DO-CONHECIMENTO',
                'NOME-DA-ESPECIALIDADE',
                'NOME-DA-SUB-AREA-DO-CONHECIMENTO',
                'NOME-GRANDE-AREA-DO-CONHECIMENTO',
                'PAIS-INSTITUICAO',
                'PAIS-NASCIMENTO',
                'TITULO-DO-PROCESSO',
                'SIGLA-INSTITUICAO',
                'MODALIDADE-DO-PROCESSO',
                'NOME-COMPLETO']

    CAT_ID_COLS = [colname + "-ID" for colname in CAT_COLS]

    FILENAME_PATTERN = re.compile(r'hst_pgt_bolsas_cnpq_\d{4}\.zip')


    def __init__(self, include_filter = lambda x: True):

        self.logger = logging.getLogger("BolsasCNPqExtractor")

        self.include_filter = include_filter

        self.files = [file for file in os.listdir('.') if BolsasCNPqExtractor.FILENAME_PATTERN.match(file)]

        self.all_data = []
        self.encoders = {}
        self.df = None


    def read_zip_xml(self, filepath):

        self.logger.info('>Processando arquivo {}'.format(filepath))

        iso_8859_1_parser = etree.XMLParser(encoding="iso-8859-1")

        with zipfile.ZipFile(filepath, "r") as zip_ref:

            # TODO: assume que há apenas um arquivo
            zip_filename = zip_ref.namelist()[0]

            with zip_ref.open(zip_filename) as zip_file:

                parsed = objectify.parse(io.TextIOWrapper(zip_file, "iso-8859-1"), parser=iso_8859_1_parser)

            bolsa_elements = parsed.getroot().getchildren()

            data = []

            for bolsa_element in bolsa_elements:
                
                attributes = [att for att in bolsa_element.getchildren() if att.tag in BolsasCNPqExtractor.COLS]

                bolsa_data = {attribute.tag: attribute.text for attribute in attributes}

                if self.filter_in(bolsa_data):

                    data.append(bolsa_data)
            
        return data

    def filter_in(self, data):

        return self.include_filter(data)

   
    def extract_data(self):

        self.logger.info("CONVERTENDO XML PARA CSV")

        for filename in self.files:

            self.logger.info(">{}".format(filename))

            data = self.read_zip_xml(filename)
            
            self.all_data.extend(data)

        self.df = pd.DataFrame(self.all_data).fillna("NÃO-ESPECIFICADO")
        self.all_data = []


    def make_encoders(self):

        for (colname, id_colname) in zip(BolsasCNPqExtractor.CAT_COLS, BolsasCNPqExtractor.CAT_ID_COLS):

            self.logger.info(">Normalizando coluna {}".format(colname))

            encoder = LabelEncoder().fit(self.df[colname].unique())
        
            encoder_df = pd.DataFrame(encoder.classes_, columns=[colname])
            encoder_df.index.name = id_colname
            encoder_df.to_csv(colname + ".csv", encoding='utf-8')
            
            self.encoders[colname] = encoder


    def normalize(self):

        self.logger.info("NORMALIZANDO COLUNAS")

        self.make_encoders()

        self.df[BolsasCNPqExtractor.CAT_ID_COLS] = self.df[BolsasCNPqExtractor.CAT_COLS]\
        .apply(lambda col: self.encoders[col.name].transform(col))
        self.df.drop(BolsasCNPqExtractor.CAT_COLS, axis=1, inplace=True)


    def save_df(self):

        self.logger.info("SALVANDO O DATASET FINAL")

        self.df.to_csv("dataset.csv", encoding='utf-8', index=False)


    def processar(self):

        self.logger.info("CRIANDO DATASET FINAL")

        self.extract_data()
        self.normalize()
        self.save_df()


    def read_dataset(filepath):

        df = pd.read_csv(filepath)

        df['ANO-PAGAMENTO'] = pd.to_numeric(df['ANO-PAGAMENTO'])
        df['QUANTIDADE-BOLSA-ANO'] = pd.to_numeric(df['QUANTIDADE-BOLSA-ANO'])
        df['QUANTIDADE-MESES-PAGOS'] = pd.to_numeric(df['QUANTIDADE-MESES-PAGOS'])
        df['VALOR-PAGO'] = pd.to_numeric(df['VALOR-PAGO'])

        return df


if __name__ == '__main__':

    log_fmt = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    bce = BolsasCNPqExtractor()
    bce.processar()

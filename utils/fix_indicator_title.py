from src.support import Support
from src.db import dbThings
import pandas as pd
from glob import glob

if __name__ == '__main__':
    dbThings.conn_variable_name = 'IMPORT_ADAPTA_CONN_STR'
    for filename in glob(r'D:\Atrium\Projects\AdaptaBrasil\Data\DADOS_2014_11_04_Adapta\**\descricao.xlsx', recursive=True):
        print(f'Processando {filename}...')
        df = pd.read_excel(filename)
        df['fname'] = filename
        df.to_sql("_descricoes", dbThings.get_connection(), schema=Support.schema, if_exists='append', index=True)
    print('Thats all folks!')
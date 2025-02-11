# -*- coding: utf-8 -*-
from pandas import DataFrame
from src.dfs import DataframesInput
from src.support import Support
from src.db import dbThings
from numpy import isnan
import pandas as pd
import math
import numpy as np

class Inserts:


    @classmethod
    def getValueWithDatatype(self, value, data_type):
        return str('NULL,' if value == 'NULL' or type(value) == float and isnan(value) \
            else (f"{int(value)}," if data_type == 'int' or type(value) == int else f"'{str(value).replace(chr(39),f'{chr(39)}{chr(39)}')}',"))

    @classmethod
    def processInserts(cls, table: str, data: DataFrame):
        if table in Support.params.keys():
            table_params = Support.params[table]
        else:
            table_params = {}
        init_sql = f'INSERT INTO {Support.schema}.{table} ('
        fields_corrrespondency = Support.fields_corrrespondency[table]
        fields_datatype = Support.fields_datatype[table]
        for table_field, file_field in fields_corrrespondency.items():
            if type(file_field) == str and file_field not in data.columns:
                if file_field == "_sep_id_":
                    table_field = "sep_id"
                elif file_field == "_geoobjecttype_id_":
                    table_field = "geoobjecttype_id"
                elif file_field.startswith('_'):
                    pass
                elif Support.verbose:
                    print(f"Warning: Field {file_field} not in input file of {table} table.")
                    continue
            init_sql += f"{table_field},"
        init_sql = f"{init_sql[:-1]}) VALUES ("
        try:
            index = 0
            for index, row in data.iterrows():
                sql = init_sql
                values = ''
                for db_field, file_field in fields_corrrespondency.items():
                    if type(file_field) == str and not file_field.startswith("_") and file_field not in data.columns:
                        continue
                    if db_field in table_params.keys():
                        min_value = int(table_params[db_field]['min_value'])
                        value = row[file_field] + min_value - 1
                    elif type(file_field) == dict:
                         value = file_field['value']
                    elif file_field == "_sep_id_":
                        value = int(Support.sep_id)
                    elif file_field == '_geoobjecttype_id_':
                        value = int(Support.geoobjecttype_id)
                    elif file_field.startswith('_max_'):
                        value = dbThings.getValue(f"SELECT {file_field.replace('_max_(','max(')} from {Support.schema}.{table}") + 1
                    else:
                        value = row[file_field]
                    values += cls.getValueWithDatatype(value, fields_datatype[db_field])
                sql += f"{values[:-1]})"
                dbThings.executeSQL(sql)
        except Exception as e:
            print('Error in import_value. Error: {0}'.format(str(e)))
            print()
            print(row)
            print(sql)
            return
        print(f"Registros a inserir: {len(data)}")
        print(f"Registros inseridos: {index+1}")
    @classmethod
    def preProcessing(cls, event: str):
        dbThings.executeSQL(Support.params[event])

    @classmethod
    def processScenario(cls):
        cls.processInserts('scenario', DataframesInput.cenario)

    @classmethod
    def processSep(cls):
        # unnecessary to create id_import in this case
        #dbThings.createIdImport(schema=Support.schema, table_name='sep')
        cls.processInserts('sep', DataframesInput.sep)
        pass

    @classmethod
    def processIndicator(cls):
        cls.processInserts('indicator', DataframesInput.descricao)

    @classmethod
    def processIndicator_Indicator(cls):
        cls.processInserts('indicator_indicator', DataframesInput.composicao)

    @classmethod
    def processYear(cls):
        cls.processInserts('year', DataframesInput.referencia_temporal)

    @classmethod
    def processValues(cls):
        i = 1
        data = []
        DataframesInput.valores.columns = map(str.upper, DataframesInput.valores.columns)
        for index, row in DataframesInput.valores.iterrows():
            try:
                j = 0
                geocod = int(row['ID'])
                for column in DataframesInput.valores.columns[1:]:
                    if column.startswith('UNNAMED'):
                        continue
                    j += 1
                    parts = column.split('-')
                    indicator_id = parts[0].replace(":", "")
                    year = parts[1]
                    scenario_id = None if len(parts) == 2 \
                        else parts[2]

                    if (type(row[column]) == float) or (type(row[column]) == int) or (type(row[column]) == np.float64):
                        value = row[column]
                    elif pd.isna(row[column]) or pd.isna(row[column]) == 'DI':
                        value = None
                    else:
                        try:
                            v = row[column].replace(',', '.')
                            value = v if not math.isnan(float(v)) else None
                        except:
                            value = None
                    data.append({'geocod': geocod,
                                 'indicator_id': indicator_id,
                                 'year': year,
                                 'scenario': scenario_id,
                                 'value': value
                                 })
                if (index % 10) == 0:
                    print(index)

            except Exception as e:
                print('Error in import_value. Error: {0}'.format(str(e)))
                print()
                print(index, j)
                print(row)
                return
        print('Gerando df...')
        df = pd.DataFrame(data)
        print(f"Linhas na planilha: {len(DataframesInput.valores)}")
        print(f"Dados na planilha: {len(DataframesInput.valores) * (len(DataframesInput.valores.columns)-1)}")
        df.to_sql("_valores", dbThings.get_connection(), schema=Support.schema, if_exists='replace', index=False)
        print('Temporária _valores criada.')
        if 'insert_values' in Support.params.keys():
            sql = f"select count(1) from {Support.schema}._valores"
            # print(f"Registros na temporária: {dbThings.executeSQL(sql).first()[0]}")
            dbThings.executeSQL(Support.params['insert_values'])

    @classmethod
    def processContribution(cls):
        try:
            proporcionalidades_values = DataframesInput.proporcionalidades.iloc[1:]
            proporcionalidades_header = ''
            colnames = []
            for colname in DataframesInput.proporcionalidades.iloc[0].values:
                colnames.append(colname.upper())
            proporcionalidades_values.columns = colnames

            data = []
            j = 0
            filecount = 0
            for index, row in proporcionalidades_values.iterrows():
                j += 1
                if math.isnan(row['ID']):
                    break
                for i in range(1, len(proporcionalidades_values.columns)):
                    detail_colname = proporcionalidades_values.columns[i]
                    if detail_colname.find('.') > -1:
                        detail_colname = detail_colname[:detail_colname.find('.')]
                    parts = detail_colname.split('-')
                    detail_indicator_id = parts[0].strip()
                    detail_year = parts[1]
                    detail_scenario_id = None if len(parts) == 2 \
                        else parts[2]
                    if isinstance(DataframesInput.proporcionalidades.columns[i], str) \
                            and not(DataframesInput.proporcionalidades.columns[i].startswith('Unnamed')):
                        partsh = DataframesInput.proporcionalidades.columns[i].replace(':', '').split('-')
                    master_indicator_id = partsh[0].strip()
                    master_year = partsh[1]
                    master_scenario_id = None if len(partsh) == 2 \
                        else partsh[2]
                    value = row[i]
                    if type(value) != np.float64 and type(value) != float:
                        try:
                            if type(value) == str and value == 'DI':
                                value = None
                            else:
                                value = value.replace(',', '.') if type(value) == str or not math.isnan(value) else None
                        except:
                            value = None
                    filecount += 1
                    data.append({'geocod': row['ID'],
                                 'master_indicator_id': master_indicator_id,
                                 'master_year': master_year,
                                 'master_scenario_id': master_scenario_id,
                                 'detail_indicator_id': detail_indicator_id,
                                 'detail_year': detail_year,
                                 'detail_scenario_id': detail_scenario_id,
                                 'value': value
                                 })
                if (index % 10) == 0:
                    print(index)

        except Exception as e:
            print('Error importing table. Error: {0}'.format(str(e)))
            print(i, index, DataframesInput.proporcionalidades.columns[i])
            return False
        df = pd.DataFrame(data)
        print(f"Linhas no dataframe: {len(df)}")
        df.to_sql("_proporcionalidades", dbThings.get_connection(), schema=Support.schema, if_exists='replace',
                  index=False)
        print(f"Dados no dataframe: {(len(DataframesInput.proporcionalidades) - 2) * (len(df.columns)-1)}")
        sql = f"select count(1) from {Support.schema}._proporcionalidades"
        print(f"Registros na temporária: {dbThings.executeSQL(sql).first()[0]}")
        if 'insert_contributions' in Support.params.keys():
            print(f"Registros inseridos na contributions: "),
            dbThings.executeSQL(Support.params['insert_contributions'])


    @classmethod
    def postProcessing(cls):
        if 'after_all_insert' in Support.params.keys():
            dbThings.executeSQL(Support.params['after_all_insert'])

    @classmethod
    def process(cls):
        #
        # Fazer importação de referencia temporal (cenário)
        #
        Support.removeOldData()
        cls.processScenario()
        cls.processIndicator()
        cls.processIndicator_Indicator()
        cls.processYear()
        cls.processValues()
        if not DataframesInput.proporcionalidades is None:
            cls.processContribution()
        cls.postProcessing()






# -*- coding: utf-8 -*-
from glob import glob
import src.db as db
from src.db import dbThings
from json import loads

class Support:
    sep_id = None
    path = None
    extension = None
    schema = None
    verbose = True
    params = None

    input_files = [{'name': 'setor_estrategico', 'mandatory': True, 'destination': 'sep'},
                   {'name': 'descricao', 'mandatory': True, 'destination': 'indicator'},
                   {'name': 'composicao', 'mandatory': True, 'destination': 'indicator_indicator'},
                   {'name':'referencia_temporal', 'mandatory': True, 'destination': 'year'},
                   {'name': 'cenarios', 'mandatory': False, 'destination': 'scenario'},
                   {'name': 'valores', 'mandatory': True, 'destination': 'values'},
                   {'name': 'proporcionalidades', 'mandatory': False, 'destination': 'contribution'}]

    fields_corrrespondency = {'sep':{'id': 'íd',
                                     'name': 'nome',
                                     'description': 'descricao',
                                     'clipping_resolution_group_id': {'value': 1},
                                     'visible': {'value': 1},
                                     'order_by':{'value': 1},
                                     'image_url': 'url_imagem'
                                     },
                              'scenario': {'scenario_id': '_max_(scenario_id)',
                                           'name':'nome',
                                           'description': 'descricao',
                                           'symbol': 'simbolo',
                                           'sep_id': '_sep_id_'
                                           },
                              'indicator': {'id': 'codigo',
                                            'level':'nivel',
                                            'name': 'nome_simples',
                                            'title': 'nome_simples',
                                            'shortname': 'nome_simples',
                                            'simple_description': 'desc_simples',
                                            'complete_description': 'desc_completa',
                                            'measurement_unit': 'unidade',
                                            'pessimist': 'relacao',
                                            'source': 'fontes',
                                            'targets': 'meta',
                                            'geoobjecttype_id': '_geoobjecttype_id_',
                                            'sep_id':   "_sep_id_"
                                            },
                              'indicator_indicator': {'indicator_id_master': 'codigo_pai',
                                                      'indicator_id_detail': 'codigo_filho'
                                            },
                              'year': {'year': 'simbolo',
                                       'label': 'nome',
                                       'description': 'descricao',
                                       'sep_id': '_sep_id_'
                                       }
                              }

    fields_datatype =  {'sep':{'id': 'int',
                               'name': 'varchar',
                              'description': 'varchar',
                              'clipping_resolution_group_id': 'int',
                              'visible': 'int',
                              'order_by':'int',
                              'image_url': 'varchar'
                                     },
                        'scenario': {'scenario_id': 'int',
                                     'name': 'varchar',
                                     'description': 'varchar',
                                     'sep_id': 'int',
                                     'symbol': 'varchar'},
                              'indicator': {'id': 'int',
                                            'level':'int',
                                            'name': 'varchar',
                                            'title': 'varchar',
                                            'shortname': 'varchar',
                                            'simple_description': 'varchar',
                                            'complete_description': 'varchar',
                                            'measurement_unit': 'varchar',
                                            'pessimist': 'int',
                                            'source': 'varchar',
                                            'targets': 'varchar',
                                            'geoobjecttype_id': 'int',
                                            'sep_id':   "int"
                                            },
                              'indicator_indicator': {'indicator_id_master': 'int',
                                                      'indicator_id_detail': 'int'
                                            },
                              'year': {'year': 'varchar',
                                       'label': 'varchar',
                                       'description': 'varchar',
                                       'sep_id': 'int'
                                       }
                              }

    @classmethod
    def replace_strings_in_json(cls, json_obj: dict, old_new_strings: dict):
        for key, value in json_obj.items():
            if not isinstance(value, dict):
                for _key, _value in old_new_strings.items():
                    if isinstance(value, str):
                        value = value.replace(_key, str(_value))
                    else:
                        if value == _key:
                            value = _value
                json_obj[key] = value
            else:
                cls.replace_strings_in_json(value, old_new_strings)
    @classmethod
    def defines(cls, path: str,  conn_variable_name:str, schema: str, sep_id: int, verbose: bool):
        cls.path = path
        cls.extension = Support.getFileExtension(path)
        cls.schema = schema
        cls.sep_id = sep_id
        cls.verbose = verbose
        dbThings.conn_variable_name = conn_variable_name

        dbThings.verbose = verbose
        with open(fr"{Support.path}\params.json") as f:
            cls.params = loads(f.read())
        cls.geoobjecttype_id = cls.params['geoobjecttype_id']
        cls.replace_strings_in_json(cls.params,
                                    {'_geoobjecttype_id_': int(cls.geoobjecttype_id),
                                    '_min_value_': int(cls.params['min_value']),
                                    '_schema_': cls.schema,
                                    '_sep_id_': int(cls.sep_id)})

    @classmethod
    def getFileExtension(cls, path: str)->str:
        files = glob(f'{path}/**/*.csv', recursive=True)
        xlsx = True
        for file in files:
            xlsx = xlsx and file.split('.')[-1].upper() == 'XLSX'
        return 'xlsx' if xlsx else 'csv'

    @classmethod
    def removeOldData(cls):
        indicator_ids = dbThings.getSQLasDataframe(f"SELECT id, sep_id, name FROM {cls.schema}.indicator WHERE sep_id = {cls.sep_id}")
        for _, row in indicator_ids.iterrows():
            id = row['id']
            if cls.verbose:
                print(f"Indicator id: {id}, sep_id, {row['sep_id']} {row['name']}")
            dbThings.executeSQL(
                f"""DELETE FROM {cls.schema}.contribution 
                    WHERE master_indicator_id = {id}
                    OR detail_indicator_id = {id}""")
            dbThings.executeSQL(
                f"""DELETE FROM {cls.schema}.value 
                    WHERE indicator_id = {id}""")
            dbThings.executeSQL(
                f"""DELETE FROM {cls.schema}.indicator_indicator 
                    WHERE indicator_id_master = {id}
                    OR indicator_id_detail = {id}""")
            dbThings.executeSQL(
                f"""DELETE FROM {cls.schema}.image 
                    WHERE indicator_id = {id}""")
            dbThings.executeSQL(
                f"""DELETE FROM {cls.schema}.indicator
                    WHERE id = {id}"""
            )

        dbThings.executeSQL(
             f"""DELETE FROM {cls.schema}.scenario
                 WHERE sep_id = {cls.sep_id}""")
        dbThings.executeSQL(
            f"""DELETE FROM {cls.schema}.year
                WHERE sep_id = {cls.sep_id}""")
        # dbThings.executeSQL(
        #     f"""DELETE FROM {cls.schema}.sep
        #         WHERE id = {cls.sep_id}""")
        pass


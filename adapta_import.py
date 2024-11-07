# -*- coding: utf-8 -*-
from argparse import ArgumentParser, BooleanOptionalAction
from src.dfs import DataframesInput
from src.support import Support
from src.import_process import Inserts

def process_command_line():
    parser = ArgumentParser(description="Importar dados Setor Estratégico AdaptaBrasil.")
    parser.add_argument("--schema", type=str, required=True, help="schema destinatário dos dados.")
    parser.add_argument("--conn_variable_name", type=str, required=True, help="Variável de ambiente com os parâmetros de conexão.")
    parser.add_argument("--sep_id", type=int, required=True, help="Id do Setor Estratégico no banco.")
    parser.add_argument("--input_folder", type=str, required=True, help="Caminho para a pasta de entrada.")
    parser.add_argument('--verbose', action=BooleanOptionalAction)
    return parser.parse_args()

def removeSeps():
    for sep_id in range(1,10):
        Support.sep_id = sep_id
        Support.removeOldData()


if __name__ == '__main__':
    args=process_command_line()
    Support.defines(args.input_folder, args.conn_variable_name, args.schema, args.sep_id, args.verbose)

    DataframesInput.loadDataFrames()
    Inserts.process()
    print('That''s all, folks!')
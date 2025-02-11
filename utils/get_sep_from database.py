# -*- coding: utf-8 -*-
from argparse import ArgumentParser, BooleanOptionalAction
from pandas import DataFrame, concat
import json
import urllib.request

values = None

def process_command_line():
    parser = ArgumentParser(description="Obtém dados Adapta usando as URLs.")
    parser.add_argument("--url_base", type=str, required=True, help="URL base de acesso aos dados Adapta.")
    parser.add_argument("--schema", type=str, required=True, help="schema destinatário dos dados.")
    parser.add_argument("--sep_id", type=int, required=True, help="Id do Setor Estratégico no banco.")
    parser.add_argument("--destination_folder", type=str, required=True, help="Caminho para geração dos CSV.")
    parser.add_argument('--verbose', action=BooleanOptionalAction)
    return parser.parse_args()

def addValues(url: str):
    global values
    f = urllib.request.urlopen(url)
    j = json.loads(f.read())
    if values is None:
        values = DataFrame(j)
    else:
        values = concat([values], axis=0)
    print(len(values),url)


if __name__ == '__main__':
    args = process_command_line()
    url_hierarchy = f'{args.url_base}api/hierarquia/{args.schema}'
    f = urllib.request.urlopen(url_hierarchy)
    j = json.loads(f.read())
    indicators = DataFrame(j)
    indicators = indicators[indicators['sep_id'] == args.sep_id]
    for index, indicator in indicators.iterrows():
        if indicator.level < 2:
            continue
        years = indicator['years'].split(',')
        scenarios = DataFrame(indicator['scenarios'])
        for year in years:
            if int(year) < 2024:
                url_values = f'{args.url_base}api/mapa-dados/BR/trechorodovia/{indicator["id"]}/{year}/null/adaptabrasil'
                addValues(url_values)
            else:
                for _, scenario in scenarios.iterrows():
                    url_values = f'{args.url_base}api/mapa-dados/BR/trechorodovia/{indicator["id"]}/{year}/{scenario["value"]}/adaptabrasil'
                    addValues(url_values)
    pass
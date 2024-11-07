# Adapta Import

## Introdução

Esse repositório contém a ferramenta responsável por importar dados de Setores Socio-econômicos para a plataforma [AdaptaBrasil](https://sistema.adaptabrasil.mcti.gov.br/).
Os dados de entrada são em formato XLSX ou CSV, e devem ter sido antes validados pelo Canoa, portal de validação dos dados a serem importados.

## Características Técnicas

### Linguagem de Programação
- **Python**: O Adapta Parser é desenvolvido em Python, aproveitando sua versatilidade e as extensas bibliotecas disponíveis para manipulação de dados.

### Dependências
- **Pandas**: Utilizado para a leitura, manipulação e análise de dados em arquivos de planilhas.
- **Argparse**: Facilita a criação de interfaces de linha de comando, permitindo a passagem de argumentos para o script.
- **SQLAlchemy**: Usada para fazer o acesso a banco de dados, fornecendo recursos de conexão,obtenção e atualização de dados.
- **NumPy**: Provê a validação de números de ponto flutuante.

### Dependências de produção
- **Python 3.6+**: A versão mínima do Python necessária para executar o Adapta Import.
- Essa app foi testada apenas em ambiente Windows, apsear de ter sido feita pensando também no ambiente Linux.

```shell
    pip install -r requirements.txt
```

### Funcionalidades
A partir da indicação do diretório onde existe um conjunto de determinados arquivos XLS, XLSX ou CSV, 
outros parâmetros passados em linha de comando e um arquivo json contendo outras informações, os dados são importados 
para um determinado banco de dados cuja string de conexão é indicada por uma variável de ambiente. 
O nome dessa variável é também indicado na linha de comando.

### Argumentos de execução
- **--schema**: (obrigatório) Schema destinatário dos dados.
- **--conn_variable_name**: (obrigatório) Variável de ambiente com os parâmetros de conexão. Essa variável deve
seguir o formato especificado [aqui](https://www.geeksforgeeks.org/connecting-postgresql-with-sqlalchemy-in-python/).  
Ex:  
```
postgresql+psycopg2://<usuário>:<senha>@<ip do servidor>:<porta>/<banco de dados>
```

- **--sep_id**: Id do Setor Estratégico destinatário dos dados no banco.
- **--input_folder**: Caminho para a pasta de entrada. 
- **--verbose**: Exibe mensagens detalhadas sobre o andamento do processo de importação.

### Arquivo de parâmetros JSON
Um arquivo JSON deve existir no diretório indicado em **input_folder** informando outros outros parâmetros 
mais específicos de um determinado Setor Estratégico.  
Exemplo:  
```json
{
	"min_value": 5000,
	"after_all_insert": "INSERT INTO _schema_.image (id, imageurl, indicator_id) \nselect _min_value_,'https://s3.sa-east-1.amazonaws.com/cache-sistema.adaptabrasil.mcti.gov.br/imagens/201.svg', _min_value_ ON CONFLICT DO NOTHING; \nINSERT INTO _schema_.indicator_indicator (indicator_id_master, indicator_id_detail)\nSELECT 0, _min_value_ FROM _schema_.indicator_indicator ON CONFLICT DO NOTHING;\nupdate _schema_.year set default_value = 0,orderby = id - (select min(id) from _schema_.year where sep_id = _sep_id_)+1  where sep_id = _sep_id_;\nupdate _schema_.year set default_value = 1 where id = (select min(id) from _schema_.year where sep_id = _sep_id_) and sep_id = _sep_id_;\nupdate _schema_.scenario set default_value = 0,orderby = scenario_id - (select min(scenario_id) from _schema_.scenario where sep_id = _sep_id_)+1  where sep_id = _sep_id_;\nupdate _schema_.scenario set default_value = 1 where scenario_id = (select min(scenario_id) from _schema_.scenario where sep_id = _sep_id_) and sep_id = _sep_id_;\nupdate _schema_.\"indicator\" set legend_id = pessimist + 1 where sep_id = _sep_id_;\nSELECT public.\"__refreshallmaterializedviews\"('_schema_');\n",
	"after_all_delete": "",
	"geoobjecttype_id": 1,
	"indicator": {
		"id": {"min_value":  "_min_value_"}},
	"indicator_indicator": {
		"indicator_id_master": {"min_value":  "_min_value_"},
		"indicator_id_detail": {"min_value":  "_min_value_"}},
	"insert_values": "INSERT INTO _schema_.value (indicator_id, scenario_id, geoobject_id, year, value)\nSELECT NULLIF(indicator_id, '')::int+_min_value_-1,scenario_id,g.id,\"year\"::int,value\nFROM _schema_._valores v\nINNER JOIN _schema_.geoobject g\nON v.geocod::int  = g.geocod::int AND geoobjecttype_id = '_geoobjecttype_id_'\nLEFT JOIN _schema_.scenario s\nON COALESCE(s.symbol,'') = COALESCE(v.scenario,'') AND sep_id = _sep_id_\nORDER BY 1\n",
	"insert_contributions": "INSERT INTO _schema_.contribution (geoobject_id, master_indicator_id, detail_indicator_id, master_scenario_id, detail_scenario_id, master_year, detail_year, value) \nSELECT g.id,master_indicator_id::int+_min_value_-1,detail_indicator_id::int+_min_value_-1,s1.scenario_id,s2.scenario_id,master_year::int,detail_year::int ,value\nFROM _schema_._proporcionalidades p\nINNER JOIN _schema_.geoobject g\nON p.geocod::int  = g.geocod::int AND geoobjecttype_id = _geoobjecttype_id_\nLEFT JOIN _schema_.scenario s1\nON COALESCE(s1.symbol,'') = COALESCE(p.master_scenario_id,'') AND s1.sep_id = _sep_id_\nLEFT JOIN _schema_.scenario s2\nON COALESCE(s2.symbol,'') = COALESCE(p.detail_scenario_id,'') AND s2.sep_id = _sep_id_\nORDER BY 1\n"
}
```

- **min_value**: determina o menor valor de um indicador no banco de dados. Esse valor será somado aos ids
de indicadores informados nos arquivos de entrada, que começam sempre do valor 1.
- **after_all_insert**: script SQL a ser executado depois que todos os dados forem inseridos no banco.  
Tem por objetivo atualizar dados no banco de dados que não são informados nas planilhas.   
Ex:
```postgresql
-- Insere o link para a imagem no Setor Estratégico na tabela image
INSERT INTO _schema_.image (id, imageurl, indicator_id)   
SELECT _min_value_,'https://s3.sa-east-1.amazonaws.com/cache-sistema.adaptabrasil.mcti.gov.br/imagens/201.svg', _min_value_ ON CONFLICT DO NOTHING;
-- Insere na tabela indicator_indicator o registro responsável por conectar o indicador de nível 1 com o
-- registro existente no banco que conecta todos os Setores.
INSERT INTO _schema_.indicator_indicator (indicator_id_master, indicator_id_detail)  
SELECT 0, _min_value_ FROM _schema_.indicator_indicator ON CONFLICT DO NOTHING;
-- Seta os campos default_value e o orderby na tabela year do banco de dados 
UPDATE _schema_.year SET default_value = 0,
                         orderby = id - (select min(id) FROM _schema_.year 
                                                        WHERE sep_id = _sep_id_)+1  
WHERE sep_id = _sep_id_;
UPDATE _schema_.year set default_value = 1 
where id = (SELECT min(id) FROM _schema_.year WHERE sep_id = _sep_id_) AND sep_id = _sep_id_;  
-- Seta os campos default_value e o orderby na tabela scenario do banco de dados 
UPDATE _schema_.scenario SET default_value = 0,orderby = scenario_id - (SELECT min(scenario_id) 
FROM _schema_.scenario WHERE sep_id = _sep_id_)+1 
WHERE sep_id = _sep_id_;  
UPDATE _schema_.scenario SET default_value = 1 
WHERE scenario_id = (SELECT min(scenario_id) FROM _schema_.scenario WHERE sep_id = _sep_id_) AND sep_id = _sep_id_; 
-- Seta o id da legenda de cada indicador. Essa sentença segue a regra usada até hoje no schema adaptabrasil. 
-- Outros schemas poderão ter regiras distintas.
UPDATE _schema_.indicator set legend_id = pessimist + 1 WHERE sep_id = _sep_id_;
-- Executa a procedure que atualiza as views materializadas do schema. 
-- Necessário executar toda ver que algun dado no banco for atualizado.
SELECT public.__refreshallmaterializedviews('_schema_');
```
Nesse script e em outros desse JSON existem três macros que serão atualizadas antes do script srer executado.  
São elas:  
_\_schema__: schema destino dos dados, informado no parâmetro *--schema* da linha de comando.  
_\_min_value__: valor do parâmetro *--min_value* informado no mesmo JSON.  
_\_sep_id__: id do Setor Estratégico indicado no parâmetro *--sep_id*.

- **after_all_delete**: a primeira fase da importação implica na remoção de todos os dados referentes ao 
Setor Estratégico. O script contido nessa variável é executado após a remoção de todos os dados de todas as tabelas.

- **geoobjecttype_id**: id do tipo de geoobject que deve ser associado aos indicadores que estão sendo importados.  
Exemplos:

| id  | Nome               |
|-----|--------------------|
| 1   | Município          |
| 6   | Porto              |
| 7   | Trecho de Estrada  |
| 8   | Trecho de Ferrovia |

Se os valores e proporcionalidades forem informados para cada município, o valor é **1**, se for por portos é **6** etc.

- **indicator**, **indicator_indicador**: JSON contendo conteúdos específicos de campos nessas tabelas. 
No exemplo, os valores dos campos vindos da tabela a ser importada serão somados ao valor de *min_value*.

- **insert_values**: os dados vindos da planilha **valores.xlsx** serão primeiramente importados para uma tabela
temporária no banco, **_valores**. Essa sentença irá migrar esses dados para a tabela definitiva, **value**, 
fazendo os ajustes necessários.

- **insert_contributions**: os dados vindos da planilha **proporcionalidades.xlsx** serão primeiramente importados para uma tabela
temporária no banco, **_proporcionalidades**. Essa sentença irá migrar esses dados para a tabela definitiva, **contribution**, 
fazendo os ajustes necessários.



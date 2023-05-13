import oracledb
import getpass
import sqlalchemy
import sys
import pandas
import os
import time

import logging

from decouple import config

from dash import Dash, html, dash_table
from flask_caching import Cache
import dash_bootstrap_components as dbc

SATDB_USERNAME = config('SATDB_USERNAME', default='')
SATDB_PASSWORD = config('SATDB_PASSWORD', default='' )
SATDB_SID = config('SATDB_SID', default='PITUBA:2118/sat')


#os.environ.get('PASSWORD','')

#logging.warning('Watch out!')  # will print a message to the console

#oracledb.init_oracle_client(lib_dir=r"C:\ORACLE\instantclient_21_9")
oracledb.init_oracle_client() 


oracledb.is_thin_mode()

sys.modules["cx_Oracle"] = oracledb

oracledb.version = "8.3.0"


conn = sqlalchemy.create_engine("oracle://{SATDB_USERNAME}:{SATDB_PASSWORD}@{SATDB_SID}".format(SATDB_USERNAME=SATDB_USERNAME, SATDB_PASSWORD=SATDB_PASSWORD, SATDB_SID=SATDB_SID), coerce_to_decimal=False)

print( conn )


app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

cache = Cache(app.server, config={
    'CACHE_TYPE': 'FileSystemCache',
    'CACHE_DIR':'/tmp/dash-cache',
    'CACHE_DEFAULT_TIMEOUT': 1*60,
    'CACHE_THRESHOLD':20 
})

@cache.memoize(timeout=30)
def dfTotal():
    print(sys._getframe().f_code.co_name)
    return pandas.read_sql('''
	WITH TENTATIVASCERTIDAO AS 
		(SELECT 
			'CFPM',SQCERTIDAOCFPM, SQTIPOCERTIDAO, 
			TPCADASTRO, NUIDENTIFICADOR, TPPESSOACERTIDAO,NUCPFCNPJCERTIDAO, DTEMISSAOCERTIDAO,
			FLPENDSEFAZ, FLPENDPGMS, FLPENDCADASTRO, FLNAOINSCRITO, IDLOCAL
		FROM DBASAT.CERTIDAOCFPM c 
		WHERE DTEMISSAOCERTIDAO >= '19/04/2023'
		union
		SELECT 
			'NAOINSCRITO',SQCERTCFPMNAOINSCRITO, 8 SQTIPOCERTIDAO,
			TPCADASTRO, NUIDENTIFICADOR, TPPESSOACERTIDAO, NUCPFCNPJCERTIDAO, DTEMISSAOCERTIDAO,
			'N' FLPENDSEFAZ, 'N' FLPENDPGMS, 'N' FLPENDCADASTRO, 'S' FLNAOINSCRITO, 999
		FROM DBASAT.CERTIDAOCFPMNAOINSCRITO c)
	SELECT NVL(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'),'TOTAL') DATA,
		count(DISTINCT DECODE (SQTIPOCERTIDAO,2, NUIDENTIFICADOR, NULL)) "Negativas", count(DISTINCT DECODE (SQTIPOCERTIDAO,3, NUIDENTIFICADOR, NULL))  "P. E. N.",
		count(DISTINCT DECODE (SQTIPOCERTIDAO,1, NUIDENTIFICADOR, NULL)) "Positivas" , 
		count(DISTINCT DECODE (SQTIPOCERTIDAO,8, NUIDENTIFICADOR, NULL)) "Não Inscrito",
		count(DISTINCT DECODE (SQTIPOCERTIDAO,1, NUIDENTIFICADOR, NULL)) + 
		count(DISTINCT DECODE (SQTIPOCERTIDAO,2, NUIDENTIFICADOR, NULL)) + /*NEG*/
		count(DISTINCT DECODE (SQTIPOCERTIDAO,8, NUIDENTIFICADOR, NULL)) +
		count(DISTINCT DECODE (SQTIPOCERTIDAO,3, NUIDENTIFICADOR, NULL)) /*PEN*/  "Total"
	FROM TENTATIVASCERTIDAO
	WHERE DTEMISSAOCERTIDAO >= '19/04/2023' /*and IDLOCAL = 999*/
	GROUP BY ROLLUP(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'))
	ORDER BY TO_DATE(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'),'DD/MM/YYYY') ASC NULLS LAST
	''', conn)

@cache.memoize(timeout=30)
def dfPresencial():
    print(sys._getframe().f_code.co_name)
    return pandas.read_sql('''
	WITH TENTATIVASCERTIDAO AS 
		(SELECT 
			'CFPM',SQCERTIDAOCFPM, SQTIPOCERTIDAO, 
			TPCADASTRO, NUIDENTIFICADOR, TPPESSOACERTIDAO,NUCPFCNPJCERTIDAO, DTEMISSAOCERTIDAO,
			FLPENDSEFAZ, FLPENDPGMS, FLPENDCADASTRO, FLNAOINSCRITO, IDLOCAL
		FROM DBASAT.CERTIDAOCFPM c 
		WHERE DTEMISSAOCERTIDAO >= '19/04/2023'
		union
		SELECT 
			'NAOINSCRITO',SQCERTCFPMNAOINSCRITO, 8 SQTIPOCERTIDAO,
			TPCADASTRO, NUIDENTIFICADOR, TPPESSOACERTIDAO, NUCPFCNPJCERTIDAO, DTEMISSAOCERTIDAO,
			'N' FLPENDSEFAZ, 'N' FLPENDPGMS, 'N' FLPENDCADASTRO, 'S' FLNAOINSCRITO, 999
		FROM DBASAT.CERTIDAOCFPMNAOINSCRITO c
		)
	SELECT NVL(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'),'TOTAL') DATA,
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,2, NUIDENTIFICADOR, NULL)) "Negativas", 
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,3, NUIDENTIFICADOR, NULL))  "P. E. N.",
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,1, NUIDENTIFICADOR, NULL)) "Positivas" , 
		--count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,8, NUIDENTIFICADOR, NULL)) "Não Inscrito",
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,1, NUIDENTIFICADOR, NULL)) + 
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,2, NUIDENTIFICADOR, NULL)) + /*NEG*/
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,8, NUIDENTIFICADOR, NULL)) +
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL <> 999 THEN SQTIPOCERTIDAO ELSE NULL END,3, NUIDENTIFICADOR, NULL)) /*PEN*/  "Total"
	FROM TENTATIVASCERTIDAO
	WHERE DTEMISSAOCERTIDAO >= '19/04/2023' --and IDLOCAL <> 999
	GROUP BY ROLLUP(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'))
	ORDER BY TO_DATE(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'),'DD/MM/YYYY') ASC NULLS LAST
	''', conn)

@cache.memoize(timeout=30)
def dfSite():
    print(sys._getframe().f_code.co_name)
    return pandas.read_sql('''
	WITH TENTATIVASCERTIDAO AS 
		(SELECT 
			'CFPM',SQCERTIDAOCFPM, SQTIPOCERTIDAO, 
			TPCADASTRO, NUIDENTIFICADOR, TPPESSOACERTIDAO,NUCPFCNPJCERTIDAO, DTEMISSAOCERTIDAO,
			FLPENDSEFAZ, FLPENDPGMS, FLPENDCADASTRO, FLNAOINSCRITO, IDLOCAL
		FROM DBASAT.CERTIDAOCFPM c 
		WHERE DTEMISSAOCERTIDAO >= '19/04/2023'
		union
		SELECT 
			'NAOINSCRITO',SQCERTCFPMNAOINSCRITO, 8 SQTIPOCERTIDAO,
			TPCADASTRO, NUIDENTIFICADOR, TPPESSOACERTIDAO, NUCPFCNPJCERTIDAO, DTEMISSAOCERTIDAO,
			'N' FLPENDSEFAZ, 'N' FLPENDPGMS, 'N' FLPENDCADASTRO, 'S' FLNAOINSCRITO, 999
		FROM DBASAT.CERTIDAOCFPMNAOINSCRITO c)
	SELECT NVL(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'),'TOTAL') DATA,
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,2, NUIDENTIFICADOR, NULL)) "Negativas", 
        count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,3, NUIDENTIFICADOR, NULL))  "P. E. N.",
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,1, NUIDENTIFICADOR, NULL)) "Positivas" , 
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,8, NUIDENTIFICADOR, NULL)) "Não Inscrito",
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,1, NUIDENTIFICADOR, NULL)) + 
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,2, NUIDENTIFICADOR, NULL)) + /*NEG*/
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,8, NUIDENTIFICADOR, NULL)) +
		count(DISTINCT DECODE (CASE WHEN  IDLOCAL = 999 THEN SQTIPOCERTIDAO ELSE NULL END,3, NUIDENTIFICADOR, NULL)) /*PEN*/  "Total"
	FROM TENTATIVASCERTIDAO
	WHERE DTEMISSAOCERTIDAO >= '19/04/2023' /*and IDLOCAL = 999*/
	GROUP BY ROLLUP(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'))
	ORDER BY TO_DATE(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'),'DD/MM/YYYY') ASC NULLS LAST
	''', conn)

@cache.memoize(timeout=30)
def dfPendenciasPositivas():
    print(sys._getframe().f_code.co_name)
    return pandas.read_sql('''
	SELECT TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY') "Data",
	--	count(DISTINCT DECODE (FLPENDSEFAZ||FLPENDPGMS||FLPENDCADASTRO,'SSN', NUIDENTIFICADOR,'SNN',NUIDENTIFICADOR, 'NSN',NUIDENTIFICADOR,NULL)) APENAS_DEBITOS,
		count(DISTINCT DECODE (FLPENDSEFAZ||FLPENDPGMS||FLPENDCADASTRO,'NNS', NUIDENTIFICADOR, NULL))  "Apenas Cadastral",
		count(DISTINCT DECODE (FLPENDCADASTRO,'S', NUIDENTIFICADOR,NULL)) "Cadastral",
		count(DISTINCT NUIDENTIFICADOR) "Certidões Positivas"
	FROM dbasat.CERTIDAOCFPM c
	WHERE DTEMISSAOCERTIDAO >= '19/04/2023' AND SQTIPOCERTIDAO = 1
	GROUP BY ROLLUP(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'))
	ORDER BY TO_DATE(TO_CHAR(DTEMISSAOCERTIDAO, 'DD/MM/YYYY'),'DD/MM/YYYY') ASC NULLS LAST
	''', conn)

@cache.memoize(timeout=30)
def dfAlvara():
    print(sys._getframe().f_code.co_name)
    return pandas.read_sql('''
  SELECT 
  	NVL(TO_CHAR(DTHISTORICO, 'DD/MM/YYYY'),'TOTAL') DATA, 
  	count(DISTINCT NUCGABASE || NUCGAFILIAL) TOTAL,
  	count(DISTINCT CASE WHEN FLDOCUMENTOEMITIDO ='N' THEN NUCGABASE || NUCGAFILIAL ELSE NULL END) EMITIDAS,
  	count(DISTINCT CASE WHEN FLDOCUMENTOEMITIDO ='S' THEN NUCGABASE || NUCGAFILIAL ELSE NULL END) "NÃO EMITIDOS",
  	count(DISTINCT CASE WHEN FLPENDCADIN='S' THEN NUCGABASE || NUCGAFILIAL ELSE NULL END) CADIN,
  	count(DISTINCT CASE WHEN FLPENDSEFAZ ='S' THEN NUCGABASE || NUCGAFILIAL ELSE NULL END) SEFAZ,
  	count(DISTINCT CASE WHEN FLPENDCADASTRO ='S' THEN NUCGABASE || NUCGAFILIAL ELSE NULL END) CADASTRO
  	FROM DBASAT.HISTORICOALVARA
  --WHERE DTHISTORICO  >= '19/04/2023'
  GROUP BY ROLLUP(TO_CHAR(DTHISTORICO, 'DD/MM/YYYY'))
  ORDER BY TO_DATE(TO_CHAR(DTHISTORICO, 'DD/MM/YYYY'),'DD/MM/YYYY') ASC NULLS LAST
	''', conn)

@cache.memoize(timeout=30)
def dfAlvaraPorSituacaoCadastral():
    print(sys._getframe().f_code.co_name)
    return pandas.read_sql('''
WITH TENTATIVASCERTIDAO AS
  (SELECT DSMENSAGEMCADASTRO, count(DISTINCT NUCGABASE || NUCGAFILIAL) Quantidade FROM DBASAT.HISTORICOALVARA WHERE DSMENSAGEMCADASTRO IS NOT NULL 
  GROUP BY DSMENSAGEMCADASTRO ORDER BY COUNT (*) DESC)
SELECT REGEXP_SUBSTR (DSMENSAGEMCADASTRO,'[^\,]+\,[^\.]+\.') "Erro Cadastral", Quantidade FROM TENTATIVASCERTIDAO
''', conn)


def default_table(df):
    table = dash_table.DataTable(
            df.to_dict('records'),
            columns=[
                {"name": i, "id": i, "deletable": False, "selectable": True} for i in df.columns
            ],
            sort_action='native', 
            filter_action='native'
    )
    
    

    return dbc.Card(
		dbc.CardBody(
			[
				table
			]
		),
		className="mt-3",
	)

def main_layout():
    print ("main_layout({PID}): {POOL}".format(PID=os.getpid(), POOL=conn.pool.status()))
    
    tabCertidoes = dbc.Tabs(
		[
			dbc.Tab(default_table(dfTotal()), label="Total"),
			dbc.Tab(default_table(dfPresencial()), label="Presencial"),
            dbc.Tab(default_table(dfSite()), label="Site"),
            dbc.Tab(default_table(dfPendenciasPositivas()), label="Pendencias (Cert. Positivas)")
		]
	)
    
    tabAlvaras = dbc.Tabs(
		[
            dbc.Tab(default_table(dfAlvara()), label="Emissões (Site)"),
            dbc.Tab(default_table(dfAlvaraPorSituacaoCadastral()), label="Pendencias por situação Cadastral (Site)"),
		]
	)
    
    tabs = dbc.Tabs(children=
		[
            dbc.Tab(label='Certidões', children=tabCertidoes), 
            dbc.Tab(label='Alvarás', children=tabAlvaras), 
		]
	)

    body = dbc.Container([dbc.Row([html.H1("Certidões / Alvarás"), tabs], justify="center", align="center", className="h-50")])
    
	

    return html.Div([
        html.Div(children=''),
        body,
        #dbc.Alert("Hello, Bootstrap!", className="m-5",),
        #dbc.Table.from_dataframe(historico, striped=True, bordered=True, hover=True),
        
    ])


app.layout = main_layout

server = app.server







if __name__ == '__main__':
    app.run_server(debug=True)

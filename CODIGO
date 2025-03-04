# Databricks notebook source 
# MAGIC %md <h5> Script: Script da criação da tabela TbDistribuicaoMailingSeguros </h5> 
# MAGIC 
# MAGIC 
# MAGIC - **Descrição:** Tabela que seleciona os clientes elegíveis ao contato das EPSs de Seguros. 
# MAGIC - **Área responsavel Regra:** Seguros (Natália/Melissa/Gabriela) 
# MAGIC 
# MAGIC - **Periodicidade:** Diária 
# MAGIC - **Carrega Synapse:** Sim 
# MAGIC 
# MAGIC - **Histórico:** 
# MAGIC - 20/10/2022 - Criação do notebook - Distribuição dos leads conforme as EPSs especificadas na tabela de parametro 
# MAGIC - 29/11/2022 - [DCIDCB-530](https://jira.bvnet.bv/browse/DCIDCB-530) Criação do crm_salva_config_synapse (tenbu.ffonseca) 
# MAGIC - 08/12/2022 - Atualização da coleta da Data de Inclusão 
# MAGIC - 12/01/2022 - Após a atualização da TbParametrosMailing, o código foi alterado para fazer a ponderação de porcentagem. 
# MAGIC - 23/03/2023 - Atualização dos imports para adequação ao cluster & alteração para H6 
# MAGIC - 26/09/2024 - Incluído uma tratativa caso a strParticaoNegocio seja vazia. 

# COMMAND ---------- 

# DBTITLE 1,Importando Notebooks de apoio 
# MAGIC %run "/Shared/Common-Notebooks/Utils" 

# COMMAND ---------- 

# MAGIC %run /Shared/CRM/Common-Notebooks/Utils 

# COMMAND ---------- 

# DBTITLE 1,Criando Parametros com Widgets 
dbutils.widgets.text("wgParamOri","") 
dbutils.widgets.text("wg_TsInicio","") 
dbutils.widgets.text("wg_DiasDelta","") 
dbutils.widgets.text("wg_DtCampoParticao","") 
dbutils.widgets.text("wg_Carga_Tabela_Full",'False') 
dbutils.widgets.text("wg_Carga_Csv_Full", 'False') 
#dbutils.widgets.text("wg_DsCampoChave", '') 

carga_tabela_full = bool(eval(dbutils.widgets.get("wg_Carga_Tabela_Full"))) 
carga_csv_full = bool(eval(dbutils.widgets.get("wg_Carga_Csv_Full"))) 
dias_delta = int(dbutils.widgets.get("wg_DiasDelta")) 
#colunas_pk = dbutils.widgets.get("wg_DsCampoChave") 

# COMMAND ---------- 

# DBTITLE 1,Configuracoes de Cluster 
spark.conf.set("spark.databricks.io.cache.enabled", "true") 
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC dbutils.library.installPyPI('holidays', version='0.12') 

# COMMAND ---------- 

# DBTITLE 1,Importando bibliotecas 
import pytz, json 
import pyspark 
from pyspark.sql.functions import lit 
from pyspark.sql.types import IntegerType 
from pyspark.sql.types import DecimalType 
from datetime import datetime as dt 
from pyspark.sql import SparkSession, functions as F 
from pyspark.sql.functions import col 
from pyspark.sql.window import Window 
from pyspark.sql.functions import monotonically_increasing_id 
from datetime import datetime, timedelta, date 
from dateutil.relativedelta import relativedelta 
import pyspark.sql.functions as f 
from pyspark.sql.functions import split 
import pytz 
from pyspark.sql.types import StructType,StructField, StringType, DoubleType, IntegerType, DateType, LongType 
from os.path import isfile, join 
from pyspark.sql.functions import from_json 
#import holidays 

# no minimo você tera as variaveis de entrada aqui 
# trata os parametros de entrada 
dictParam = json.loads(dbutils.widgets.get("wgParamOri")) 
owner = dictParam['NmBase'] #obrigatório 
tabela = dictParam['NmTabela'] #obrigatório 
colunas_pk = dictParam['DsCampoChave'] #obrigatório 
nm_owner=owner 
nm_tabela=tabela 

# trata se a data que será processada é a que esta na tabela de controle ou a digitada manualmente 
if ((dbutils.widgets.get("wg_TsInicio") == "") or (dbutils.widgets.get("wg_TsInicio") == None)) and carga_tabela_full == False: 
tsInicio = (datetime.strptime(dictParam['DtUltimoRegistroProcessado'], '%Y-%m-%d %H:%M:%S') + relativedelta(days=-dias_delta)).strftime("%Y-%m-%d 00:00:00") 
elif ((dbutils.widgets.get("wg_TsInicio") == "") or (dbutils.widgets.get("wg_TsInicio") == None)) and carga_tabela_full == True: 
tsInicio = '1900-01-01 00:00:00' 
else: 
tsInicio = dbutils.widgets.get("wg_TsInicio") 

dtCampoParticao = dbutils.widgets.get("wg_DtCampoParticao") 


#o path de gravação da tabela será montado a partir das variaveis de entrada. O ADF já vai passar como minusculos 
pathGravacao = captura_storage_base() + "/mnt/dvry/{}/{}".format(owner, tabela) 
pathGravacaoSyn = pathGravacao + '_synapse' 

# TsInclusaoAlteracao será pratica agora nas tabelas - Ele identifica quando o registro foi inserido ou alterado. Obrigatório utilização e criação do campo. 
tsIncAlt = datetime.now(pytz.timezone('Brazil/East')).strftime("%Y-%m-%d %H:%M:%S") # timestamp atual para inserir a data de processamento correta 

# SUA PARTICAO SEMPRE DEVE SER UM INTEIRO. SE FOR USAR DIA, FORMATO => AAAAMMDD 
dtAtual = datetime.now(pytz.timezone('Brazil/East')).strftime("%Y-%m-%d %H:%M:%S") 
dtCampoParticao = 'DtContratoFinanceiro' 
dtRef = datetime.now(pytz.timezone('Brazil/East')).strftime("%Y%m%d") 
nm_particao='ParticaoNegocio' 

#funcaoPartNegocio = "cast(date_format(to_date({}, 'dd/MM/yyyy'), 'yyyyMM') as int)".format(dtCampoParticao) 
funcaoPartNegocio = "cast({} as int)".format(dtCampoParticao) 

filename_com_extensao = 'SquadCRM_SF_TbDistribuicaoMailingSeguros_YYYYMMDDHHMM.csv' 

remove_colunas_upper = colunas_pk 
csv_envio = False 
tp_tabela_carga = "delta_incremental" 

if carga_tabela_full == True: 
filter_dias_delta = '--' 
else: 
#AJUSTAR CONFORME A QUERY DE CONSTRUCAO DO DATAFRAME 
filter_dias_delta = '' 

dtReferencia = spark.sql("select date_format(current_timestamp - INTERVAL 3 hour, 'yyyy-MM-dd HH:mm:ss') as now").collect()[0].now 
dtParticao = spark.sql("select date_format(current_timestamp, 'yyyyMMddhhmmss') as now").collect()[0].now 
GetDate = spark.sql("select date_format(current_timestamp, 'yyyy-MM-dd HH:mm:ss') as now").collect()[0].now 
#qtde_files = int(dbutils.widgets.get("wg_QtdeFiles")) 
qtde_files=1 


try: 
pathGravacao_tmp = "/legacy/{}/tmp/{}/".format(owner, tabela) 
dbutils.fs.rm(pathGravacao_tmp, recurse=True) 
dbutils.fs.mkdirs(pathGravacao_tmp+'modelo') 
except: 
pathGravacao_tmp = "/legacy/{}/tmp/{}/".format(owner, tabela) 
dbutils.fs.mkdirs(pathGravacao_tmp+'modelo') 




print("Path Gravacao: " + pathGravacao) 
print("Path Gravacao Synapse: " + pathGravacaoSyn) 
print("Path Gravacao Tmp: " + pathGravacao_tmp) 
print(dtAtual) 
print(funcaoPartNegocio) 
print(dtRef) 
print(tsInicio) 
print(carga_tabela_full) 
print(remove_colunas_upper) 
print(f'filter_dias_delta: {filter_dias_delta}') 






# COMMAND ---------- 

# DBTITLE 1,Definição do dia da semana 
day = dt.now(pytz.timezone('Brazil/East')).strftime("%Y-%m-%d") 
dfday = spark.createDataFrame([(day,)], ['dt']).select(F.dayofweek('dt').alias('weekday_number')) 
dfweekday=dfday.collect()[0].weekday_number 

print(day) 
print(dfweekday) 

# COMMAND ---------- 

# DBTITLE 1,Coleta a última data de inclusão na TbDistribuicaoMailing nas últimas 24h 
dfTsIncRank = spark.sql("""SELECT Distinct(TsInclusao) from dvry_crm.TbDistribuicaoMailingSeguros ORDER BY TsInclusao DESC limit 3 """) 
dfTsIncRank.createOrReplaceTempView("dfTsIncRank") 

dfTsIncRank.display() 

# COMMAND ---------- 

dftsinc=spark.sql("SELECT TsInclusao from dfTsIncRank ORDER BY TsInclusao ASC limit 1").collect()[0].TsInclusao 
#dfDM1.createOrReplaceTempView("dfDM1") 

print(dftsinc) 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC type(dftsinc) 

# COMMAND ---------- 

import datetime as dt 
if dfweekday == 2: 
dt_prazo = (dftsinc - dt.timedelta(days=4)) 
print(dt_prazo) 
dfClientes_potenciais_d1 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-4) AND a.IdPessoa NOT IN (SELECT IdPessoa from dvry_crm.TbDistribuicaoMailingSeguros WHERE TsInclusao >= '{0}') AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
print('4 days') 
dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 
elif dfweekday == 3: 
dt_prazo = (dftsinc - dt.timedelta(days=3)) 
print(dt_prazo) 
dfClientes_potenciais_d1 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-3) AND a.IdPessoa NOT IN (SELECT IdPessoa from dvry_crm.TbDistribuicaoMailingSeguros WHERE TsInclusao >= '{0}') AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
print('3 days') 
dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 
else: 
dt_prazo = (dftsinc - dt.timedelta(days=2)) 
print(dt_prazo) 
dfClientes_potenciais_d1 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-2) AND a.IdPessoa NOT IN (SELECT IdPessoa from dvry_crm.TbDistribuicaoMailingSeguros WHERE TsInclusao >= '{0}') AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
print('2 day') 
dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC import datetime as dt 
# MAGIC if dfweekday == 2: 
# MAGIC dt_prazo = (dftsinc - dt.timedelta(days=4)) 
# MAGIC print(dt_prazo) 
# MAGIC dfClientes_potenciais_d1 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-4) AND a.IdPessoa NOT IN (SELECT IdPessoa from dvry_crm.TbDistribuicaoMailingSeguros WHERE TsInclusao >= '{0}') AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
# MAGIC print('4 days') 
# MAGIC dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 
# MAGIC elif dfweekday == 3: 
# MAGIC dt_prazo = (dftsinc - dt.timedelta(days=3)) 
# MAGIC print(dt_prazo) 
# MAGIC dfClientes_potenciais_d1 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-3) AND a.IdPessoa NOT IN (SELECT IdPessoa from dvry_crm.TbDistribuicaoMailingSeguros WHERE TsInclusao >= '{0}') AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
# MAGIC print('3 days') 
# MAGIC dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 
# MAGIC else: 
# MAGIC dt_prazo = (dftsinc - dt.timedelta(days=2)) 
# MAGIC print(dt_prazo) 
# MAGIC dfClientes_potenciais_d1 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-4) AND a.IdPessoa NOT IN (SELECT IdPessoa from dvry_crm.TbDistribuicaoMailingSeguros WHERE TsInclusao >= '{0}') AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
# MAGIC dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 
# MAGIC dfClientes_potenciais_d2 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-4) AND a.IdPessoa IN (SELECT IdPessoa from dvry_crm.TbDistribuicaoMailingSeguros WHERE TsInclusao = '2024-02-12 05:43:47') AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
# MAGIC dfClientes_potenciais_d2.createOrReplaceTempView("dfClientes_potenciais_d2") 
# MAGIC dfClientes_potenciais_d1 = spark.sql("""SELECT * from dfClientes_potenciais_d1 union all SELECT * from dfClientes_potenciais_d2 """) 
# MAGIC print('2 day') 
# MAGIC dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 

# COMMAND ---------- 

# DBTITLE 1,APENAS PARA DESENVOLVIMENTO 
# MAGIC %md 
# MAGIC import datetime as dt 
# MAGIC dt_prazo = (dftsinc - dt.timedelta(days=3)) 
# MAGIC print(dt_prazo) 
# MAGIC dfClientes_potenciais_d1 = spark.sql("""SELECT a.* FROM dvry_crm.tbprospectseguroauto AS a WHERE to_date(a.DtContratoFinanceiro) >= date_add(current_date(),-3) AND a.TpPessoa = 'PF' AND DsFiltro = '0 - ELEGIVEL' AND a.NuContrato is not null AND a.NuRenavam is not null """.format(dt_prazo)) 
# MAGIC dfClientes_potenciais_d1.createOrReplaceTempView("dfClientes_potenciais_d1") 

# COMMAND ---------- 

w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientes_potenciais_d1.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

# COMMAND ---------- 

dfClienteD1 = spark.sql("""SELECT COUNT(DISTINCT(IdPessoa)) AS TOTAL FROM dfClientesPotenciais """).collect()[0].TOTAL 

print(dfClienteD1) 

# COMMAND ---------- 

dfparametrosmailing = spark.sql("SELECT * FROM trusted_crm.tbparametrosmailingseguros") 

dfparametrosmailing = dfparametrosmailing.withColumn("NU_AMOSTRAGEM", dfparametrosmailing["NU_AMOSTRAGEM"].cast(IntegerType()))\ 
.withColumn("DS_INTERVALO", dfparametrosmailing["DS_INTERVALO"].cast(IntegerType()))\ 
.withColumn("DS_LEADS", dfparametrosmailing["DS_LEADS"].cast(IntegerType()))\ 
.withColumn("Aa_IDADEVEICULO", dfparametrosmailing["Aa_IDADEVEICULO"].cast(IntegerType()))\ 
.withColumn("VRFINANCIAMENTO", dfparametrosmailing["VRFINANCIAMENTO"].cast(DecimalType(38,18)))\ 
.withColumn("CDFILIAL", dfparametrosmailing["CDFILIAL"].cast(IntegerType()))\ 
.withColumn("CDPRODUTOSEGURO", dfparametrosmailing["CDPRODUTOSEGURO"].cast(IntegerType()))\ 


dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 
dfparametrosmailing = dfparametrosmailing.withColumn("col1", split(col("DS_PORCENTAGENS"), ",").getItem(0)).withColumn("col2", split(col("DS_PORCENTAGENS"), ",").getItem(1)).withColumn("col3", split(col("DS_PORCENTAGENS"), ",").getItem(2)).drop(f.col('DS_PORCENTAGENS')) 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 

dfparametrosmailing = spark.sql(""" SELECT *, CAST('{totalAmostra}' AS INT) AS dfClienteD1 FROM dfparametrosmailing """.format(totalAmostra=dfClienteD1)) 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 

dfparametrosmailing.display() 

# COMMAND ---------- 

for row in dfparametrosmailing: 

dfDsEPS = tuple(dfparametrosmailing.select('DS_EPS').rdd.flatMap(lambda x: x).collect()) 
dfDsPerc = tuple(dfparametrosmailing.select('DS_PERC').rdd.flatMap(lambda x: x).collect()) 
dfNuAmostra = tuple(dfparametrosmailing.select('NU_AMOSTRAGEM').rdd.flatMap(lambda x: x).collect()) 
dfNuIdade = tuple(dfparametrosmailing.select('Aa_IDADEVEICULO').rdd.flatMap(lambda x: x).collect()) 
dfVrFinanciamento = tuple(dfparametrosmailing.select('VRFINANCIAMENTO').rdd.flatMap(lambda x: x).collect()) 
dfCdFilial = tuple(dfparametrosmailing.select('CDFILIAL').rdd.flatMap(lambda x: x).collect()) 
dfCdProdutoSeguro = tuple(dfparametrosmailing.select('CDPRODUTOSEGURO').rdd.flatMap(lambda x: x).collect()) 

# COMMAND ---------- 

for row in dfDsPerc: 
if row == 'True': 
App_Percent = 'True' 
else: 
App_Percent = 'False' 

# COMMAND ---------- 

# DBTITLE 1,Rate do Corte de Mailing 
#rate = 0.6 
rate = 1 

# COMMAND ---------- 

if App_Percent == 'False': 
dfparametrosmailing = dfparametrosmailing.select(col("DS_EPS"),col("NU_AMOSTRAGEM"),col("Aa_IDADEVEICULO")) 
dfparametrosmailing = dfparametrosmailing.withColumnRenamed("NU_AMOSTRAGEM","LEADS").withColumnRenamed("Aa_IDADEVEICULO","NU_LIMITWHATS").withColumn("NU_Rate",lit(rate)) 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 
elif dfClienteD1 >= 0 and dfClienteD1 <= spark.sql("""SELECT CAST(DS_LEADS AS INT) FROM dfparametrosmailing WHERE DS_INTERVALO=1""").collect()[0].DS_LEADS: 
dfparametrosmailing = dfparametrosmailing.select(col("DS_EPS"),col("DS_INTERVALO"),col("DS_LEADS"),col("col1"),col("dfClienteD1"),col("Aa_IDADEVEICULO")).orderBy(col("DS_INTERVALO")) 
dfparametrosmailing = dfparametrosmailing.withColumnRenamed("Aa_IDADEVEICULO","NU_LIMITWHATS").withColumn("DS_PERCENT", dfparametrosmailing["col1"]).drop(f.col('col1')).withColumn("NU_Rate",lit(rate)) 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 
elif dfClienteD1 > spark.sql("""SELECT CAST(DS_LEADS AS INT) FROM dfparametrosmailing WHERE DS_INTERVALO=1""").collect()[0].DS_LEADS and dfClienteD1 <= spark.sql("""SELECT CAST(DS_LEADS AS INT) FROM dfparametrosmailing WHERE DS_INTERVALO=2""").collect()[0].DS_LEADS: 
dfparametrosmailing = dfparametrosmailing.select(col("DS_EPS"),col("DS_INTERVALO"),col("DS_LEADS"),col("col2"),col("dfClienteD1"),col("Aa_IDADEVEICULO")).orderBy(col("DS_INTERVALO")) 
dfparametrosmailing = dfparametrosmailing.withColumnRenamed("Aa_IDADEVEICULO","NU_LIMITWHATS").withColumn("DS_PERCENT", dfparametrosmailing["col2"]).drop(f.col('col2')).withColumn("NU_Rate",lit(rate)) 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 
else: 
dfparametrosmailing = dfparametrosmailing.select(col("DS_EPS"),col("DS_INTERVALO"),col("DS_LEADS"),col("col3"),col("dfClienteD1"),col("Aa_IDADEVEICULO")).orderBy(col("DS_INTERVALO")) 
dfparametrosmailing = dfparametrosmailing.withColumnRenamed("Aa_IDADEVEICULO","NU_LIMITWHATS").withColumn("DS_PERCENT", dfparametrosmailing["col3"]).drop(f.col('col3')).withColumn("NU_Rate",lit(rate)) 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 

# COMMAND ---------- 

if App_Percent == 'False': 
dfparametrosmailing = dfparametrosmailing 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 
dfparametrosmailing.display() 
else: 
dfparametrosmailing = spark.sql("""SELECT *, 
ROUND(DS_PERCENT * dfClienteD1 * NU_Rate, 0) AS LEADS 
FROM dfparametrosmailing""") 
dfparametrosmailing.createOrReplaceTempView("dfparametrosmailing") 
dfparametrosmailing.display() 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC <h5>*Aplicação regra PROPENSAO AUTO AVULSO*<h5> 

# COMMAND ---------- 

dfClientesPotenciais = spark.sql(""" 
SELECT A.*, 
/*------------------------------------------------------------------SCORE PROPENSÃO DIGITAL--------------------------------------------------------------------------*/ 
CASE WHEN DsPropensaoDigital='ALTA' THEN 100 ELSE 0 END AS SCORE_PROPENSAODIG, 

/*------------------------------------------------------------------SCORE ANO VEÍCULO--------------------------------------------------------------------------------*/ 
CASE 
WHEN AaModelo=2018 THEN 56 
WHEN AaModelo=2019 THEN 56 
WHEN AaModelo=2020 THEN 63 
WHEN AaModelo=2021 THEN 63 
WHEN AaModelo=2022 THEN 70 
WHEN AaModelo=2023 THEN 70 
WHEN AaModelo=2024 THEN 70 
WHEN AaModelo>2024 THEN 70 ELSE 0 
END AS SCORE_ANOVEICULO, 

/*------------------------------------------------------------------- SCORE IDADE CLIENTE-----------------------------------------------------------------------------*/ 
CASE WHEN datediff(YEAR, to_date(DtNascimento),current_date()) > 50 THEN 0 
WHEN datediff(YEAR, to_date(DtNascimento),current_date()) >= 46 THEN 56 
WHEN datediff(YEAR, to_date(DtNascimento),current_date()) >= 41 THEN 64 
WHEN datediff(YEAR, to_date(DtNascimento),current_date()) >= 36 THEN 56 
WHEN datediff(YEAR, to_date(DtNascimento),current_date()) >= 31 THEN 64 ELSE 0 
END AS SCORE_IDADECLIENTE, 


/*----------------------------------------------------------------- SCORE SEGMENTO RENDA MERCADO ----------------------------------------------------------------------*/ 
CASE 
WHEN B.RendaBV >7000 AND B.RendaBV <=10000 then 90 /*Média Renda 2*/ 
WHEN B.RendaBV >10000 AND B.RendaBV <=15000 then 90 /*Alta Renda 1*/ 
WHEN B.RendaBV >15000 then 90 /*Alta Renda 2*/ 
ELSE 0 
END AS SCORE_SGMT_RENDA, 
--(SELECT Distinct(TsInclusao) FROM dvry_credito.tbrendabv_v2 ORDER BY TsInclusao DESC Limit 1 ) AS TsInclusao 

/*----------------------------------------------------------------- SCORE SEGMENTO RENDA MERCADO ----------------------------------------------------------------------*/ 
CASE 
WHEN datediff(current_date(),C.DtInicioRelacionamento)/365 between 1 and 3 then 54 
WHEN datediff(current_date(),C.DtInicioRelacionamento)/365 between 3 and 5 then 42 
WHEN datediff(current_date(),C.DtInicioRelacionamento)/365 between 5 and 7 then 48 
ELSE 0 
END AS SCORE_TEMPO_RELAC, 

/*--------------------------------------------------------- SCORE PROPENSÃO DIGITAL E SEGMENTO DE RENDA --------------------------------------------------------------------------*/ 
CASE WHEN DsPropensaoDigital='ALTA' THEN 
CASE 
WHEN B.RendaBV >4000 AND B.RendaBV <=7000 then 70 /*Média Renda 1*/ 
WHEN B.RendaBV >7000 AND B.RendaBV <=10000 then 80 /*Média Renda 2*/ 
WHEN B.RendaBV >10000 AND B.RendaBV <=15000 then 90 /*Alta Renda 1*/ 
WHEN B.RendaBV >15000 then 100 /*Alta Renda 2*/ 
ELSE 0 
END 
ELSE 0 END AS SCORE_PROPENSAODIG_SEGMT_RENDA 


FROM dfClientesPotenciais A 
LEFT JOIN dvry_credito.tbrendabv_v2 B 
ON (cast(A.IdPessoa as bigint)= cast(B.cpf as bigint)) 
AND B.TsInclusao = (SELECT Distinct(TsInclusao) FROM dvry_credito.tbrendabv_v2 ORDER BY TsInclusao DESC Limit 1 ) 
LEFT JOIN dvry_corp_cliente.tbvisao360 C 
ON (cast(A.IdPessoa as bigint)= cast(C.IdPessoa as bigint)) AND C.FlVigenciaAtiva = 1; 


""") 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

# COMMAND ---------- 

dfClientesPotenciais = spark.sql(""" 
SELECT 
*, 
SCORE_PROPENSAODIG + SCORE_ANOVEICULO + SCORE_IDADECLIENTE + SCORE_SGMT_RENDA + SCORE_TEMPO_RELAC + SCORE_PROPENSAODIG_SEGMT_RENDA AS SCORE 
FROM dfClientesPotenciais 

""") 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

# COMMAND ---------- 

dfClientesPotenciais = spark.sql(""" SELECT *, 
CASE WHEN cast(CdProdutoSeguro as int) IN {0} THEN 'S' ELSE 'N' END AS FlGRConsultor 
FROM dfClientesPotenciais 
""".format(dfCdProdutoSeguro)) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 


w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientesPotenciais.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

# COMMAND ---------- 

# DBTITLE 1,Montar o Dataframe vazio 
schema = StructType([ 
StructField('IdBv', StringType(), True), 
StructField('IdPessoa', StringType(),True), 
StructField('DsAtivo', StringType(),True), 
StructField('TpPessoa', StringType(),True), 
StructField('DtNascimento', DateType(),True), 
StructField('NmPessoa', StringType(),True), 
StructField('DsSexo', StringType(),True), 
StructField('DtObito', DateType(),True), 
StructField('DsEstadoCivil', StringType(),True), 
StructField('VrRenda', DoubleType(),True), 
StructField('DsEmail', StringType(),True), 
StructField('DsEmail2', StringType(),True), 
StructField('DsEmail3', StringType(),True), 
StructField('DsEmail4', StringType(),True), 
StructField('DsTipoTelefone', StringType(),True), 
StructField('NuDDD', IntegerType(),True), 
StructField('NuTelefone', StringType(),True), 
StructField('NuDDD2', IntegerType(),True), 
StructField('NuTelefone2', StringType(),True), 
StructField('NuDDD3', IntegerType(),True), 
StructField('NuTelefone3', StringType(),True), 
StructField('NuDDD4', IntegerType(),True), 
StructField('NuTelefone4', StringType(),True), 
StructField('NuDDD5', IntegerType(),True), 
StructField('NuTelefone5', StringType(),True), 
StructField('NuDDD6', IntegerType(),True), 
StructField('NuTelefone6', StringType(),True), 
StructField('NuDDD7', IntegerType(),True), 
StructField('NuTelefone7', StringType(),True), 
StructField('NuDDDCelular', IntegerType(),True), 
StructField('NuTelefoneCelular', StringType(),True), 
StructField('DsEndereco', StringType(),True), 
StructField('NuEndereco', StringType(),True), 
StructField('DsComplemento', StringType(),True), 
StructField('DsBairro', StringType(),True), 
StructField('DsCidade', StringType(),True), 
StructField('CdUf', StringType(),True), 
StructField('CdCep', StringType(),True), 
StructField('FlCepRisco', StringType(),True), 
StructField('FlCepsEmAtendimento', StringType(),True), 
StructField('CdMunicipio', StringType(),True), 
StructField('DsMunicipio', StringType(),True), 
StructField('DsPropensaoDigital', StringType(),True), 
StructField('FlPrivateBank', StringType(),True), 
StructField('FlFamilyOffice', StringType(),True), 
StructField('FlBlackList', StringType(),True), 
StructField('FlEscudoCRM', StringType(),True), 
StructField('FlObito', StringType(),True), 
StructField('DtCadastro', DateType(),True), 
StructField('DtModificacao', DateType(),True), 
StructField('NuContratoLegado', StringType(),True), 
StructField('NuContrato', StringType(),True), 
StructField('DtContratoFinanceiro', DateType(),True), 
StructField('CdProduto', IntegerType(),True), 
StructField('CdModalidadeProduto', IntegerType(),True), 
StructField('NmModalidadeProduto', StringType(),True), 
StructField('DtInicioVigencia', DateType(),True), 
StructField('DtFimVigencia', DateType(),True), 
StructField('VrContrato', DoubleType(),True), 
StructField('VrFinanciamento', DoubleType(),True), 
StructField('VrEntrada', DoubleType(),True), 
StructField('QtParcelas', IntegerType(),True), 
StructField('VrPrestacao', DoubleType(),True), 
StructField('DtPrimeiroVencimento', DateType(),True), 
StructField('DtUltimoVencimento', DateType(),True), 
StructField('StProposta', StringType(),True), 
StructField('CdProdutoSeguro', IntegerType(),True), 
StructField('NuScoreFim', StringType(),True), 
StructField('AaModelo', StringType(),True), 
StructField('CdModelo', StringType(),True), 
StructField('CdMarca', StringType(),True), 
StructField('NuChassi', StringType(),True), 
StructField('DsCor', StringType(),True), 
StructField('DsPlaca', StringType(),True), 
StructField('NuRenavam', StringType(),True), 
StructField('DsFiltro', StringType(),True), 
StructField('TsInclusao', DateType(),True), 
StructField('ParticaoNegocio', IntegerType(),True), 

#/*----------------------------------------------------------------- SCORE SEGMENTO RENDA MERCADO ----------------------------------------------------------------------*/ 
StructField('SCORE_PROPENSAODIG', IntegerType(),True), 
StructField('SCORE_ANOVEICULO', IntegerType(),True), 
StructField('SCORE_IDADECLIENTE', IntegerType(),True), 
StructField('SCORE_SGMT_RENDA', IntegerType(),True), 
StructField('SCORE_TEMPO_RELAC', IntegerType(),True), 
StructField('SCORE_PROPENSAODIG_SEGMT_RENDA', IntegerType(),True), 
StructField('SCORE', IntegerType(),True), 

StructField('FlGRConsultor', StringType(),True), 
StructField('Distribuicao', StringType(),True), 

StructField('Controle', StringType(),True) 


]) 

dfMailingSeguros = spark.createDataFrame([], schema) 

dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

# COMMAND ---------- 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfMailingSeguros AS a """).collect()[0].TOTAL 

print(f'Quant dfMailingSeguros inicial') 
print(dfvalidacao) 

# COMMAND ---------- 

# DBTITLE 1,Apagar 
# MAGIC %md 
# MAGIC dfDsEPS = ('N/A','XPEER') 

# COMMAND ---------- 

print(dfDsEPS) 

# COMMAND ---------- 

# DBTITLE 1,Separação do grupo controle 
GC = 0.2 

dfLimitControl = int(GC*rate*dfClienteD1) 

print(dfLimitControl) 

# COMMAND ---------- 

spark.conf.set("spark.databricks.io.cache.enabled", "true") 
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) 

# COMMAND ---------- 

for row in dfDsEPS: 
if row =='N/A': 
continue 
elif '_WPP' in row: 
continue 
else : 
dfLimitControl=dfLimitControl 

#print(f'Limite:') 
#print(f'Row:'+ row) 
#print(dfLimitControl) 
dfEPSsample = spark.sql("""SELECT * FROM dfClientesPotenciais limit {0}""".format(dfLimitControl)) 
dfEPSsample.createOrReplaceTempView("dfEPSsample") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfEPSsample AS a """).collect()[0].TOTAL 


#print(f'Quant dfEPSsample fase '+ row) 
#print(dfvalidacao) 

#print(f"Tuple_EPSs etapa " + row ) 
variavel_tuple = (row) 
variavel_tuple = tuple([variavel_tuple]) 
#print (variavel_tuple) 
tuple_EPSs = variavel_tuple 
#print (tuple_EPSs) 


#removendo os cnpjs já classificado 
dfClientesPotenciais = dfClientesPotenciais.union(dfEPSsample) 
w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientesPotenciais.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

#dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfClientesPotenciais AS a """).collect()[0].TOTAL 

#print(f'Quant dfClientesPotenciais fase ' + row) 
#print(dfvalidacao) 


dfEPS = spark.sql("""SELECT *, '{EPS}' AS Distribuicao, 'x' AS Controle FROM dfEPSsample""".format(EPS = row)) 

dfMailingSeguros = dfMailingSeguros.union(dfEPS) 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

#dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfMailingSeguros AS a """).collect()[0].TOTAL 

#print(f'Quant dfMailingSeguros fase '+ row) 
#print(dfvalidacao) 



# COMMAND ---------- 

print(dfDsEPS) 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC SELECT max(SCORE) from dfClientesPotenciais 

# COMMAND ---------- 

spark.conf.set("spark.databricks.io.cache.enabled", "true") 
spark.conf.set("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true") 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) 

# COMMAND ---------- 

# DBTITLE 1,Ordenação do corte de leads 
for row in dfDsEPS: 
if row =='N/A': 
continue 
elif '_WPP' in row: 
continue 
#print(row) 
dfTotalAmostra = spark.sql(""" SELECT cast(LEADS as int) AS LEADS FROM dfparametrosmailing WHERE DS_EPS ='{EPS}' """.format(EPS = row)).collect()[0].LEADS 
dfTotalMailing = spark.sql(""" SELECT COUNT(IdPessoa) AS COUNT FROM dfMailingSeguros """).collect()[0].COUNT 
dfSaldo = dfTotalAmostra - dfTotalMailing 
if dfSaldo <=0: 
dfSaldo = 0 
else: 
dfSaldo = dfSaldo 

#print(f'Row:'+ row) 
dfLimit = spark.sql("""SELECT CAST(LEADS AS INT) AS NU_AMOSTRAGEM FROM dfparametrosmailing WHERE DS_EPS ='{EPS}'""".format(EPS = row)).collect()[0].NU_AMOSTRAGEM 
#print(f'Limite:'+ row) 
#print(dfLimit) 
dfEPSsample = spark.sql("""SELECT * FROM dfClientesPotenciais ORDER BY SCORE DESC limit {0} """.format(dfSaldo)) 
dfEPSsample.createOrReplaceTempView("dfEPSsample") 

#dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfEPSsample AS a """).collect()[0].TOTAL 

#print(f'Quant dfEPSsample fase '+ row) 
#print(dfvalidacao) 

#print(f"Tuple_EPSs etapa " + row ) 
variavel_tuple = (row) 
variavel_tuple = tuple([variavel_tuple]) 
#print (variavel_tuple) 
tuple_EPSs += (variavel_tuple) 
#print (tuple_EPSs) 

#removendo os cnpjs já classificado 
dfClientesPotenciais = dfClientesPotenciais.union(dfEPSsample) 
w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientesPotenciais.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

#dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfClientesPotenciais AS a """).collect()[0].TOTAL 

#print(f'Quant dfClientesPotenciais fase '+ row) 
#print(dfvalidacao) 

dfEPS = spark.sql("""SELECT *, '{EPS}' AS Distribuicao, 'NA' AS Controle FROM dfEPSsample""".format(EPS = row)) 

dfMailingSeguros = dfMailingSeguros.union(dfEPS) 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

#dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfMailingSeguros AS a """).collect()[0].TOTAL 
#print(f'Quant dfMailingSeguros fase '+ row) 
#print(dfvalidacao) 


# COMMAND ---------- 

dfDsEPS = [row for row in dfDsEPS if row not in tuple_EPSs] 
print(dfDsEPS) 

# COMMAND ---------- 

for row in dfDsEPS: 
if '_WPP' in row: 
print(f'Row:'+ row) 
dfLimit = spark.sql("""SELECT CAST(LEADS AS INT) AS NU_AMOSTRAGEM FROM dfparametrosmailing WHERE DS_EPS ='{EPS}'""".format(EPS = row)).collect()[0].NU_AMOSTRAGEM 
if dfLimit >= spark.sql("""SELECT CAST(NU_LIMITWHATS AS INT) AS NU_LimitWhats FROM dfparametrosmailing WHERE DS_EPS ='{EPS}'""".format(EPS = row)).collect()[0].NU_LimitWhats: 
dfLimit = spark.sql("""SELECT CAST(NU_LIMITWHATS AS INT) AS NU_LimitWhats FROM dfparametrosmailing WHERE DS_EPS ='{EPS}'""".format(EPS = row)).collect()[0].NU_LimitWhats 
print(dfLimit) 
else: 
dfLimit=dfLimit 
print(dfLimit) 
print(f'Limite:') 
print(dfLimit) 
dfEPSpropensao = spark.sql("""SELECT *, 
CASE WHEN a.DsPropensaoDigital = 'ALTA' THEN 1 
WHEN a.DsPropensaoDigital = 'MEDIA' THEN 2 
ELSE 3 END as DsPropensao 
FROM dfClientesPotenciais as a 
ORDER BY DsPropensao ASC""" ) 
dfEPSpropensao.createOrReplaceTempView("dfEPSpropensao") 

dfEPSpropensao = spark.sql("""SELECT * FROM dfEPSpropensao WHERE DsPropensao IN (1,2,3) ORDER BY DsPropensao ASC limit {0}""".format(dfLimit)) 
dfEPSsample = dfEPSpropensao.drop(f.col('DsPropensao')) 
dfEPSsample.createOrReplaceTempView("dfEPSsample") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfEPSsample AS a """).collect()[0].TOTAL 

print(f'Quant dfEPSsample '+ row) 
print(dfvalidacao) 

print(f"Tuple_EPSs etapa " + row ) 
variavel_tuple = (row) 
variavel_tuple = tuple([variavel_tuple]) 
print (variavel_tuple) 
tuple_EPSs += (variavel_tuple) 
#tuple_EPSs = variavel_tuple 
print(tuple_EPSs) 


#removendo os cnpjs já classificado 
dfClientesPotenciais = dfClientesPotenciais.union(dfEPSsample) 
w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientesPotenciais.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfClientesPotenciais AS a """).collect()[0].TOTAL 

print(f'Quant dfClientesPotenciais fase ' + row) 
print(dfvalidacao) 


dfEPS = spark.sql("""SELECT *, '{EPS}' AS Distribuicao, 'NA' AS Controle FROM dfEPSsample""".format(EPS = row)) 

dfMailingSeguros = dfMailingSeguros.union(dfEPS) 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

#dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfMailingSeguros AS a """).collect()[0].TOTAL 

#print(f'Quant dfMailingSeguros fase '+ row) 
#print(dfvalidacao) 

#dfEPS.printSchema() 

# COMMAND ---------- 

for row in dfDsEPS: 
if row == 'GR': 
print(f'Row:'+ row) 
dfLimit = spark.sql("""SELECT CAST(LEADS AS INT) AS NU_AMOSTRAGEM FROM dfparametrosmailing WHERE DS_EPS ='{EPS}'""".format(EPS = row)).collect()[0].NU_AMOSTRAGEM 
print(f'Limite:') 
print(dfLimit) 
dfEPSsample = spark.sql("""SELECT * FROM dfClientesPotenciais WHERE FlGRConsultor = 'S' limit {0}""".format(dfLimit)) 
dfEPSsample.createOrReplaceTempView("dfEPSsample") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfEPSsample AS a """).collect()[0].TOTAL 

print(f'Quant dfEPSsample GR') 
print(dfvalidacao) 

#removendo os cnpjs já classificado 
dfClientesPotenciais = dfClientesPotenciais.union(dfEPSsample) 
w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientesPotenciais.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfClientesPotenciais AS a """).collect()[0].TOTAL 

print(f'Quant dfClientesPotenciais fase GR') 
print(dfvalidacao) 

dfEPS = spark.sql("""SELECT *, '{EPS}' AS Distribuicao, 'NA' AS Controle FROM dfEPSsample""".format(EPS = row)) 

dfMailingSeguros = dfMailingSeguros.union(dfEPS) 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfMailingSeguros AS a """).collect()[0].TOTAL 

print(f'Quant dfMailingSeguros fase GR') 
print(dfvalidacao) 

print(f"Tuple_EPSs etapa " + row ) 
variavel_tuple = (row) 
variavel_tuple = tuple([variavel_tuple]) 
print (variavel_tuple) 
tuple_EPSs += (variavel_tuple) 
print (tuple_EPSs) 

else: 
continue 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC dfMailingSeguros.display() 

# COMMAND ---------- 

dfDsEPS = [row for row in dfDsEPS if row not in tuple_EPSs] 
print(dfDsEPS) 

# COMMAND ---------- 

for row in dfDsEPS: 
for word in tuple_EPSs: 
if row in word: 
dfTotalAmostra = spark.sql(""" SELECT cast(LEADS as int) AS LEADS FROM dfparametrosmailing WHERE DS_EPS ='{EPS}' """.format(EPS = row)).collect()[0].LEADS 
dfTotalMailing = spark.sql(""" SELECT COUNT(IdPessoa) AS COUNT FROM dfMailingSeguros """).collect()[0].COUNT 
dfSaldo = dfTotalAmostra - dfTotalMailing 
if dfSaldo <=0: 
dfSaldo = 0 
else: 
dfSaldo = dfSaldo 

print(f'Row:'+ row) 
dfLimit = spark.sql("""SELECT CAST(LEADS AS INT) AS NU_AMOSTRAGEM FROM dfparametrosmailing WHERE DS_EPS ='{EPS}'""".format(EPS = row)).collect()[0].NU_AMOSTRAGEM 
print(f'Limite:'+ row) 
print(dfLimit) 
dfEPSsample = spark.sql("""SELECT * FROM dfClientesPotenciais limit {0}""".format(dfSaldo)) 
dfEPSsample.createOrReplaceTempView("dfEPSsample") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfEPSsample AS a """).collect()[0].TOTAL 

print(f'Quant dfEPSsample fase '+ row) 
print(dfvalidacao) 

print(f"Tuple_EPSs etapa " + row ) 
variavel_tuple = (row) 
variavel_tuple = tuple([variavel_tuple]) 
print (variavel_tuple) 
tuple_EPSs += (variavel_tuple) 
print (tuple_EPSs) 

#removendo os cnpjs já classificado 
dfClientesPotenciais = dfClientesPotenciais.union(dfEPSsample) 
w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientesPotenciais.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfClientesPotenciais AS a """).collect()[0].TOTAL 

print(f'Quant dfClientesPotenciais fase '+ row) 
print(dfvalidacao) 

dfEPS = spark.sql("""SELECT *, '{EPS}' AS Distribuicao, 'NA' AS Controle FROM dfEPSsample""".format(EPS = row)) 

dfMailingSeguros = dfMailingSeguros.union(dfEPS) 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfMailingSeguros AS a """).collect()[0].TOTAL 
print(f'Quant dfMailingSeguros fase '+ row) 
print(dfvalidacao) 
else: 
continue 

# COMMAND ---------- 

dfDsEPS = [row for row in dfDsEPS if row not in tuple_EPSs] 
print(dfDsEPS) 

# COMMAND ---------- 

for row in dfDsEPS: 
if row =='N/A': 
continue 
else : 
print(f'Row:'+ row) 
dfLimit = spark.sql("""SELECT CAST(LEADS AS INT) AS NU_AMOSTRAGEM FROM dfparametrosmailing WHERE DS_EPS ='{EPS}'""".format(EPS = row)).collect()[0].NU_AMOSTRAGEM 
print(f'Limite:'+ row) 
print(dfLimit) 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfClientesPotenciais AS a """).collect()[0].TOTAL 

print(f'Quant dfClientesPotenciais fase '+ row) 
print(dfvalidacao) 

dfEPSsample = spark.sql("""SELECT * FROM dfClientesPotenciais limit {0}""".format(dfLimit)) 
dfEPSsample.createOrReplaceTempView("dfEPSsample") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfEPSsample AS a """).collect()[0].TOTAL 

print(f'Quant dfEPSsample fase '+ row) 
print(dfvalidacao) 

print(f"Tuple_EPSs etapa " + row ) 
variavel_tuple = (row) 
variavel_tuple = tuple([variavel_tuple]) 
print (variavel_tuple) 
tuple_EPSs += (variavel_tuple) 
print (tuple_EPSs) 

#removendo os cnpjs já classificado 
dfClientesPotenciais = dfClientesPotenciais.union(dfEPSsample) 
w = Window.partitionBy("IdPessoa").orderBy(f.desc("DtModificacao")) 
dfClientesPotenciais = dfClientesPotenciais.withColumn('n', f.count('IdPessoa').over(w)).sort('IdPessoa') 
dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('n')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfClientesPotenciais AS a """).collect()[0].TOTAL 

print(f'Quant dfClientesPotenciais fase '+ row) 
print(dfvalidacao) 

dfEPS = spark.sql("""SELECT *, '{EPS}' AS Distribuicao, 'NA' AS Controle FROM dfEPSsample""".format(EPS = row)) 

dfMailingSeguros = dfMailingSeguros.union(dfEPS) 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

dfvalidacao = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfMailingSeguros AS a """).collect()[0].TOTAL 
print(f'Quant dfMailingSeguros fase '+ row) 
print(dfvalidacao) 



# COMMAND ---------- 

dfMailingSeguros = spark.sql("""SELECT * FROM dfMailingSeguros ORDER BY SCORE DESC""") 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 

# COMMAND ---------- 

# DBTITLE 1,Geração do arquivo de estudo 
# MAGIC %md 
# MAGIC dbutils.fs.mkdirs('/mnt/raw/file/CRM/TMP/DistribuicaoMailingSeguros') 

# COMMAND ---------- 

dtParticaoHora = datetime.now(pytz.timezone('Brazil/East')).strftime("%Y%m%d%H%M") 
print(dtParticaoHora) 

# COMMAND ---------- 

nmarquivofinal = (spark.sql(f"""SELECT concat('DistribuicaoMailingSeguros_','{dtParticaoHora}')""").collect()[0][0]) 
dfMailingSeguros.write.parquet(f'{captura_storage_base()}/mnt/raw/file/CRM/TMP/DistribuicaoMailingSeguro/{nmarquivofinal}') 
print(f'Segmento do arquivo foi colocado no dir /mnt/raw/file/CRM/TMP/DistribuicaoMailingSeguros com o nome {nmarquivofinal}') 

# COMMAND ---------- 

dfClientesPotenciais = dfClientesPotenciais.where(f.col('n') == 1).drop(f.col('SCORE_PROPENSAODIG')).drop(f.col('SCORE_ANOVEICULO')).drop(f.col('SCORE_IDADECLIENTE')).drop(f.col('SCORE_SGMT_RENDA')).drop(f.col('SCORE_TEMPO_RELAC')).drop(f.col('SCORE_PROPENSAODIG_SEGMT_RENDA')).drop(f.col('SCORE')).drop(f.col('Controle')) 
dfClientesPotenciais.createOrReplaceTempView("dfClientesPotenciais") 

# COMMAND ---------- 

dfMailingSeguros = dfMailingSeguros.drop('TsInclusao') 
dfMailingSeguros.createOrReplaceTempView("dfMailingSeguros") 


dfListEPS = spark.sql(""" 
SELECT 
IdBv 
,IdPessoa 
,DsAtivo 
,TpPessoa 
,CAST(DtNascimento AS TIMESTAMP) 
,NmPessoa 
,DsSexo 
,CAST(DtObito AS TIMESTAMP) 
,DsEstadoCivil 
,CAST(VrRenda AS decimal(17,2)) 
,DsEmail 
,DsEmail2 
,DsEmail3 
,DsEmail4 
,DsTipoTelefone 
,CAST(NuDDD AS INT) 
,NuTelefone 
,CAST(NuDDD2 AS INT) 
,NuTelefone2 
,CAST(NuDDD3 AS INT) 
,NuTelefone3 
,CAST(NuDDD4 AS INT) 
,NuTelefone4 
,CAST(NuDDD5 AS INT) 
,NuTelefone5 
,CAST(NuDDD6 AS INT) 
,NuTelefone6 
,CAST(NuDDD7 AS INT) 
,NuTelefone7 
,CAST(NuDDDCelular AS INT) 
,NuTelefoneCelular 
,DsEndereco 
,NuEndereco 
,DsComplemento 
,DsBairro 
,DsCidade 
,CdUf 
,CdCep 
,FlCepRisco 
,FlCepsEmAtendimento 
,CdMunicipio 
,DsMunicipio 
,DsPropensaoDigital 
,FlPrivateBank 
,FlFamilyOffice 
,FlBlackList 
,FlEscudoCRM 
,FlObito 
,CAST(DtCadastro AS TIMESTAMP) 
,CAST(DtModificacao AS TIMESTAMP) 
,CAST (NuContratoLegado AS STRING) 
,CAST(NuContrato AS STRING) 
,CAST(DtContratoFinanceiro AS TIMESTAMP) 
,CAST(CdProduto AS INT) 
,CAST(CdModalidadeProduto AS INT) 
,NmModalidadeProduto 
,CAST(DtInicioVigencia AS TIMESTAMP) 
,CAST(DtFimVigencia AS TIMESTAMP) 
,CAST(VrContrato AS decimal(38,18)) 
,CAST(VrFinanciamento AS decimal(38,18)) 
,CAST(VrEntrada AS decimal(38,18)) 
,CAST(QtParcelas AS INT) 
,CAST(VrPrestacao AS decimal(38,18)) 
,CAST(DtPrimeiroVencimento AS TIMESTAMP) 
,CAST(DtUltimoVencimento AS TIMESTAMP) 
,StProposta 
,CAST(CdProdutoSeguro AS INT) 
,NuScoreFim 
,AaModelo 
,CdModelo 
,CdMarca 
,NuChassi 
,DsCor 
,DsPlaca 
,NuRenavam 
,DsFiltro 
,CAST(ParticaoNegocio AS INT) 
,FlGRConsultor 
,Distribuicao 
,CAST('{p_TsInclusao}' AS TIMESTAMP) As TsInclusao 
,CAST('{p_DtParticao}' AS TIMESTAMP) AS DtReferencia 
,row_number() over (partition by IdPessoa,NuContrato,NuRenavam order by DtContratoFinanceiro desc) as rnk 
FROM dfMailingSeguros 
""".format (p_TsInclusao=dtAtual, p_DtParticao=tsInicio)).filter('rnk=1').drop('rnk') 

dfListEPS.createOrReplaceTempView("dfListEPS") 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC dfvalidacao5 = spark.sql("""SELECT COUNT(DISTINCT(a.IdPessoa)) AS TOTAL FROM dfListEPS AS a """).collect()[0].TOTAL 
# MAGIC print(dfvalidacao5) 

# COMMAND ---------- 

# DBTITLE 1,Gera o parquet para o Synapse 
dfListEPS.write.mode('overwrite').parquet(pathGravacaoSyn) 
dfCarga=spark.read.parquet(pathGravacaoSyn) 

# COMMAND ---------- 

# DBTITLE 1,Padroniza os dados no dataframe, aplica transformacoes nos campos strings como upper, replace por , e remocao de acentos e EOL e realiza o tratamento de PKs nulas 
dfCarga = crm_padronizar_df_v3(dfCarga, [i.strip() for i in remove_colunas_upper.split(',')]) 
dfCarga = crm_tratamento_pks(dfCarga, colunas_pk) 

# COMMAND ---------- 

# DBTITLE 1,Verifica se a tabela existe - Synapse + Particionamento no Lake 
if spark.catalog.tableExists("{0}.{1}".format(owner, tabela)) and carga_tabela_full==False: 
print("Tabela existe. Continuando para a carga incremental - MERGE.") 
print(owner) 
print(tabela) 
tabela_existe=True 
elif spark.catalog.tableExists("{0}.{1}".format(owner, tabela)) and carga_tabela_full==True: 
print("Tabela existe, porém será feito uma carga full. Dropando a tabela e recriando-a.") 
crm_prepara_carga_full_tabela_v3(owner,tabela) 
dfCarga.write.mode("overwrite").parquet(pathGravacaoSyn) 
spark.read.parquet(pathGravacaoSyn).write.mode("overwrite").format("delta").partitionBy("ParticaoNegocio").save(pathGravacao) 
spark.sql("create table {0}.{1} using delta location '{2}'".format(owner, tabela, pathGravacao)) 
spark.sql("alter table {0}.{1} set tblproperties(delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(owner, tabela)) 
spark.sql("optimize {0}.{1}".format(owner, tabela)) 
tabela_existe=False 
else: 
print("Tabela não existe. Gravando primeira carga + arquivo para synapse") 
dfCarga.write.mode("overwrite").parquet(pathGravacaoSyn) 
spark.read.parquet(pathGravacaoSyn).write.mode("overwrite").format("delta").partitionBy("ParticaoNegocio").save(pathGravacao) 
spark.sql("create table {0}.{1} using delta location '{2}'".format(owner, tabela, pathGravacao)) 
spark.sql("alter table {0}.{1} set tblproperties(delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true)".format(owner, tabela)) 
spark.sql("optimize {0}.{1}".format(owner, tabela)) 
tabela_existe=False 


# COMMAND ---------- 

# DBTITLE 1,Caso a tabela já exista, cria-se o parquet para o synapse 
# if (tabela_existe==True and carga_tabela_full==False): 
# if spark.catalog.tableExists("{0}.{1}".format(owner, tabela)) and carga_tabela_full==False: 
# # dbutils.fs.rm(pathGravacaoSyn,True) 
# dfCarga.write.mode("overwrite").parquet(pathGravacaoSyn) 

#dfCount=spark.read.parquet(pathGravacaoSyn).count() 
#print(dfCount) 
#qtdLinhas = dfCarga.count() 
#if dfCount == 0: 
#dbutils.notebook.exit(json.dumps({ 
#"rowsRead": dfCount, 
#"rowsWrite": dfCount, 
#"msg":"N/A", 
#"status": "Sucesso" 
#})) 

# COMMAND ---------- 

# DBTITLE 1,Monta arquivo para gravação - Com Synapse 
# df_Merge = spark.read.parquet(pathGravacaoSyn) 
# df_Merge.createOrReplaceTempView("tbMerge") 
dfCarga.createOrReplaceTempView("tbMerge") 

# COMMAND ---------- 

# DBTITLE 1,Montagem do merge e particoes 
# lstParticaoNegocio = df_Merge.selectExpr("ParticaoNegocio").dropDuplicates().collect() 
lstParticaoNegocio = dfCarga.selectExpr("ParticaoNegocio").dropDuplicates().collect() 
# lstColunas = df_Merge.columns 
lstColunas = dfCarga.columns 
# lstDetalhe = df_Merge.columns 
lstDetalhe = dfCarga.columns 
lstChave = colunas_pk.replace(' ', '').split(',') 

#tira a chave da lista de detalhes 
for chave in lstChave: 
for idx, det in enumerate(lstDetalhe): 
if det.upper() == chave.upper(): 
lstDetalhe.pop(idx) 

#monta filtro particao 
strParticaoNegocio = ",".join(str(item.ParticaoNegocio) for item in lstParticaoNegocio) 

#monta ON do merge 
strOnMerge = " AND ".join("dvry." + item.upper() + " = mrg." + item.upper() for item in lstChave) 

#monta clasula Update 
strUpdMerge = ", ".join("dvry." + item.upper() + " = mrg." + item.upper() for item in lstDetalhe) 

#monta clausula insert 
strInsMerge = ",".join(item.upper() for item in lstColunas) 
strInsValMerge = ",".join("mrg." + item.upper() for item in lstColunas) 

#checa se há alguma particao negocio valida 
if strParticaoNegocio == '': 
strParticaoNegocio = "''" 

print("Particao negocio:" + strParticaoNegocio) 
print("On Merge:" + strOnMerge) 
print("Update Merge:" + strUpdMerge) 
print("Insert:" + strInsMerge) 
print(strInsValMerge) 


# COMMAND ---------- 

# DBTITLE 1,Execucao do merge 
if (tabela_existe==True and carga_tabela_full==False): 
spark.sql("""MERGE INTO {owner}.{tab} dvry 
USING tbMerge mrg 
ON dvry.ParticaoNegocio IN ({partNeg}) AND {onMrg} 
WHEN MATCHED THEN 
UPDATE SET {updtMrg} 
WHEN NOT MATCHED THEN 
INSERT({insMrg}) VALUES({insValMrg})""".format(owner = owner, 
tab = tabela, 
partNeg = strParticaoNegocio, 
onMrg = strOnMerge, 
updtMrg = strUpdMerge, 
insMrg = strInsMerge, 
insValMrg = strInsValMerge)) 
print("Sucesso no Merge!") 

# COMMAND ---------- 

df_desenvolvimento=spark.read.parquet(pathGravacaoSyn) 

# COMMAND ---------- 

# DBTITLE 1,Cria schema da tabela 
columns = { 
'IdBv': 'Numero do CPF/CNPJ do cliente criptografado' 
,'IdPessoa': 'Numero do CPF/CNPJ do cliente' 
,'DsAtivo': 'Indicativo dos clientes que estao ativos, inativos ou prospect' 
,'TpPessoa': 'Tipo de pessoa Fisica ou Juridica' 
,'DtNascimento': 'Data de nascimento' 
,'NmPessoa': 'Nome e sobrenome' 
,'DsSexo': 'Descrição do sexo: Feminino ou Masculino' 
,'DtObito': 'Data de obito' 
,'DsEstadoCivil': 'Descrição do estado civil' 
,'VrRenda': 'Valor Renda' 
,'DsEmail': 'Melhor email por IdPessoa' 
,'DsEmail2': 'Segundo melhor email por Idpessoa' 
,'DsEmail3': 'Terceiro melhor email por Idpessoa' 
,'DsEmail4': 'Quarto melhor email por Idpessoa' 
,'DsTipoTelefone': 'Melhor Tipo do Telefone por IdPessoa' 
,'NuDDD': 'Melhor DDD por IdPessoa' 
,'NuTelefone': 'Melhor Telefone por IdPessoa' 
,'NuDDD2': 'Melhor segundo DDD por IdPessoa' 
,'NuTelefone2': 'Melhor segundo Telefone por IdPessoa' 
,'NuDDD3': 'Melhor terceiro DDD por IdPessoa' 
,'NuTelefone3': 'Melhor terceiro Telefone por IdPessoa' 
,'NuDDD4': 'Melhor quarto DDD por IdPessoa' 
,'NuTelefone4': 'Melhor quarto Telefone por IdPessoa' 
,'NuDDD5': 'Melhor quinto DDD por IdPessoa' 
,'NuTelefone5': 'Melhor quinto Telefone por IdPessoa' 
,'NuDDD6': 'Melhor sexto DDD por IdPessoa' 
,'NuTelefone6': 'Melhor sexto Telefone por IdPessoa' 
,'NuDDD7': 'Melhor sétimo DDD por IdPessoa' 
,'NuTelefone7': 'Melhor sétimo Telefone por IdPessoa' 
,'NuDDDCelular': 'Melhor DDD Celular por IdPessoa' 
,'NuTelefoneCelular': 'Melhor Telefone Celular por IdPessoa' 
,'DsEndereco': 'Melhor logradouro por IdPessoa' 
,'NuEndereco': 'Numero do melhor logradouro por IdPessoa' 
,'DsComplemento': 'Complemento do melhor logradouro por IdPessoa' 
,'DsBairro': 'Bairro do melhor logradouro por IdPessoa' 
,'DsCidade': 'Cidade do melhor logradouro por IdPessoa' 
,'CdUf': 'Estado do melhor logradouro por IdPessoa' 
,'CdCep': 'CEP do melhor logradouro por IdPessoa' 
,'FlCepRisco': 'Flap para identificar se o CEP apresenta algum risco' 
,'FlCepsEmAtendimento': 'Flap para identificar se o CEP está em atendimento' 
,'CdMunicipio': 'Código do Município de acordo com o IBGE baseadono código do CEP Principal' 
,'DsMunicipio': 'Descrição do Município de acordo com o IBGE baseadono código do CEP Principal' 
,'DsPropensaoDigital': 'Descricao da Propensao Digital' 
,'FlPrivateBank': 'Indicativo se CPF/CNPJ é private bank (S=SIM ou N=NÃO)' 
,'FlFamilyOffice': 'Indicativo se CPF/CNPJ é family office (S=SIM ou N=NÃO)' 
,'FlBlackList': 'Indicativo se CPF/CNPJ está contido na blacklist (S=SIM ouN=NÃO)' 
,'FlEscudoCRM': 'Indicativo se CPF/CNPJ faz parte da Base Escudo CRM(S=SIM ou N=NÃO)' 
,'FlObito': 'Indicativo se CPF/CNPJ consta como Óbito (S=SIM ou N=NÃO)' 
,'DtCadastro': 'Data do primeiro Cadastro' 
,'DtModificacao': 'Data da ultima modificacao no cadastro' 
,'NuContratoLegado': 'Número do Contrato Legado' 
,'NuContrato': 'Numero do Contrato' 
,'DtContratoFinanceiro': 'Data de abertura do Contrato' 
,'CdProduto': 'Codigo produto' 
,'CdModalidadeProduto': 'Código da Modalidade do Produto' 
,'NmModalidadeProduto': 'Nome da modalidade do produto' 
,'DtInicioVigencia': 'Data inicio vigencia' 
,'DtFimVigencia': 'Data fim vigencia' 
,'VrContrato': 'Valor contrato' 
,'VrFinanciamento': 'Valor financiamento' 
,'VrEntrada': 'Valor entrada' 
,'QtParcelas': 'Quantidade de parcelas' 
,'VrPrestacao': 'Valor prestacao' 
,'DtPrimeiroVencimento': 'Data primeiro vencimento' 
,'DtUltimoVencimento': 'Data ultimo vencimento' 
,'StProposta': 'Descrição do status da proposta' 
,'CdProdutoSeguro': 'Código do Produto de Seguro' 
,'NuScoreFim': 'Número de Score do Veículo' 
,'AaModelo': 'Ano do modelo' 
,'CdModelo': 'Código do Modelo Auto' 
,'CdMarca': 'Código da Marca Auto' 
,'NuChassi': 'Número do chassi' 
,'DsCor': 'Descritivo da cor' 
,'DsPlaca': 'Descritivo da placa' 
,'NuRenavam': 'Número do renavam' 
,'DsFiltro': 'Descrição da Classificação de Elegibilidade' 
,'FlGRConsultor': 'Flag indicativo de GR Consultor' 
,'Distribuicao': 'Distribuição para a EPS' 
,'TsInclusao': 'Refere-se a data e hora de inclusao do registro na tabela. Formato YYYYMMDDHHMMSS' 
,'DtReferencia': 'Particao da tabela' 
,'ParticaoNegocio': 'Particao da tabela' 

} 

colunas = "".join([ f"{c[0]} {c[1]} COMMENT '{columns[c[0]]}', " for c in df_desenvolvimento.dtypes[:-1]])[:-2] 

# COMMAND ---------- 

# Nem todos os processos irão aplicar um filtro. 

# Atenção: A variável filtro_csv deve conter a cláusula where 
# O dataframe df_desenvolvimento recebe o parametro filtro_csv: df_desenvolvimento.where(filtro_csv) 

# Exemplo de definição de filtro que resgata dados dos últimos 3 anos: 
df_date_sub = spark.sql("select date_sub(date_format(current_timestamp, 'yyyy-MM-dd'), 2*365) as date_sub_anos").collect()[0].date_sub_anos 


# Para processos nos quais df_date_sub é definido: 
#filtro_csv = f"DtVencimento >= '{df_date_sub}'" 

# Para processos sem filtro: 
filtro_csv = '' 

# COMMAND ---------- 

# DBTITLE 1,Grava os dados 
# Como boa prática, os parâmetros deve ser chamados de forma nominal 

crm_prepara_dados_geracao_csv(carga_tabela_full = carga_tabela_full, 
carga_csv_full = carga_csv_full, 
csv_envio = csv_envio, 
tp_tabela_carga = tp_tabela_carga, 
nm_owner = nm_owner, 
nm_tabela = nm_tabela, 
colunas_create_table = colunas, 
colunas_pk = colunas_pk, 
remove_colunas_upper = remove_colunas_upper, 
filtro_temporal_csv = filtro_csv, 
df_dev = df_desenvolvimento) 

# COMMAND ---------- 

# DBTITLE 1,Config da tabela gerenciada do Synapse 
params_synapse = { 
"hash": "IdPessoa", 
"index_clustered_01": "IdPessoa,NuContrato,NuRenavam", 
"index_nonclustered_02": "DtContratoFinanceiro, DtNascimento, DtInicioVigencia", 
"index_nonclustered_03": "" 
} 
crm_salva_config_synapse(f'{nm_owner}', f'{nm_tabela}', **params_synapse) 

# COMMAND ---------- 

# DBTITLE 1,Gerando estatísticas de linhas lidas/escritas para Retorno e Log - Opcional 
# qtdLinhasEscritas = df_Merge.count() 
qtdLinhasEscritas = dfCarga.count() 
print("Linhas escritas: " + str(qtdLinhasEscritas)) 

qtdLinhasLidas = qtdLinhasEscritas 
print("Linhas lidas: " + str(qtdLinhasLidas)) 

# COMMAND ---------- 

# DBTITLE 1,Adiciona o comment das colunas quando for a primeira carga 
columnsList = [] 
if(tsInicio == '1900-01-01 00:00:00'): 
for key in columns: 
columnsList.append('ALTER TABLE '+ nm_owner+ '.' + nm_tabela +' CHANGE COLUMN '+ key+ " COMMENT '"+ columns[key] + "'; ") 
#Executa o alter table individualmente em cada coluna 
print("Realizando ALTER TABLE para adicionar os comentários na tabela") 
for i in columnsList: 
print(i) 
spark.sql(i) 
else: 
print('A tabela já rodou antes, não há necessidade de adicionar os comentários') 

# COMMAND ---------- 

# DBTITLE 1,Seleciona as EPSs para distribuição 
df_Lists=dfListEPS.select('Distribuicao').distinct().toPandas() 
df_Lists=df_Lists['Distribuicao'].to_list() 
#print(df_Lists) 

# COMMAND ---------- 

# MAGIC %md 
# MAGIC Essas duas próximas células fazem um processo de criação de .csv e .zip para serem enviados para o CrmHub. Esse processo precisará ser revisto no GCP. 
# MAGIC Como o NB está falhando por esse processo que precisará ser revisto, vou comentá-los no momento (int.gmaccarini - Gabriel Lajús Maccarini) 

# COMMAND ---------- 

# DBTITLE 1,Preparo do arquivo .csv e .zip 
# path_py='/Volumes'+pathGravacao_tmp 
# print("pathy_py:" + path_py) 

# #spark.sql("set spark.sql.caseSensitive=true") 
# df_split_={} 
# x=0 

# for i in df_Lists: 
# print("i:"+ i) 
# df=dfListEPS.select('*').where("Distribuicao='"+i+"'") 
# df.repartition(1).write.mode('overwrite').csv(pathGravacao_tmp+i, encoding='UTF-8',header=True,sep=',') 
# print("Path Gravacao_temp 1:" + pathGravacao_tmp+i) 
# all_files = os.listdir(path_py+i) 
# csv_files = list(filter(lambda f: f.endswith('.csv'), all_files)) 
# for f in csv_files: 
# print(f) 
# shutil.move(path_py+i+'/'+f , path_py+i+'.csv') 
# filename_com_extensao = f'SquadCRM_SEGUROS_{i}_YYYYMMDDHHMM.csv' 
# crm_create_zipfile(owner, tabela, filename_com_extensao, qtde_files) 
# file=[f for f in os.listdir(path_py) if isfile(join(path_py, f))] 
# for i in file: 
# shutil.move(path_py+i , path_py + 'modelo') 

# COMMAND ---------- 

# DBTITLE 1,Transferência dos arquivos finais 
# file=[f for f in os.listdir(path_py+'modelo/') if isfile(join(path_py+'modelo/', f))] 
# for i in file: 
# shutil.move(path_py+'modelo/'+i , path_py) 

# COMMAND ---------- 

# DBTITLE 1,Finaliza o processamento e retorna os dados estatísticos 
import json 

dbutils.notebook.exit(json.dumps({ 
"rowsRead": qtdLinhasLidas, 
"rowsWrite": qtdLinhasLidas, 
"msg":"N/A", 
"status": "Sucesso" 
}))

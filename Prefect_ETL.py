import pyodbc 
import pyspark
import pandas as pd
import pandasql as ps
import sqlalchemy
import logging
from sqlalchemy.engine import URL,create_engine
import prefect
from prefect import task, Flow
from configparser import ConfigParser
from prefect.agent.local import LocalAgent




# #recuperation de la configuration de la connexion stg_babilou pour la donnee date
# config = ConfigParser()
# config.read('config.ini')
# database_date = config['date']['database']
# serverURL = config['date']['host']
# username = config['date']['username']
# password = config['date']['password']
# port = config['date']['port']
# driver=config['date']['driver']

def connection_string_creation(driver,serverURL,database,username,password):
    connection_string='DRIVER='+driver+';SERVER='+serverURL+';DATABASE='+database+';UID='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryIntegrated'
    return connection_string

@task
def connection_db(connection_string):
    cnxn:pyodbc.Connection=pyodbc.connect(connection_string)
    return cnxn

@task
def creation_dataframe(requete,connexion):
    dataframe = pd.read_sql(requete,connexion)
    return dataframe   
    
@task
def pandas_sql_requete_execution(requete_sql):
    df_result_sql = ps.sqldf(requete_sql)
    return df_result_sql

@task
def read_csv_file(path):
    df_csv = pd.read_csv(path,sep=';')
    return df_csv

@task
def sql_db_execution(requete_sql_base):
    cnxn = pyodbc.connect(connection_string)
    cnxn_cursor = cnxn.cursor()
    cnxn_cursor.execute(requete_sql_base)
    cnx.commit()
    return 

@task
def sqlalchemy_create_engine(connection_string):
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string_date})
    engine = create_engine(connection_url,fast_executemany=True)
    return engine

@task
def sqalchemy_insert_into_table(table_name,engine):
    df_insert_procareETP.to_sql(table_name,engine,if_exists='append',index=False,chunksize=1000)
    return

database_procare = 'STG_BABILOU'
database_date = 'DW_BABILOU'
serverURL = 'bab-dev-sql-dwh.database.windows.net,1433'
username = 'svc_bi_local'
password = 'BabilouBI123&'
driver= "'{SQL Server Native Client 11.0}'"
table_name = 'PROCARE_ETP'    
    
    



#recuperation de la configuration de la connexion stg_babilou pour la donnee date


with Flow("connection_pyodbc") as flow:
    #configuration variables procare
    # config = ConfigParser()
    # config.read('config.ini')
    # database_procare = config['procare']['database']
    # serverURL = config['procare']['host']
    # username = config['procare']['username']
    # password = config['procare']['password']
    # port = config['procare']['port']
    # driver=config['procare']['driver']

    connexion_string_procare = connection_string_creation(driver,serverURL,database_procare,username,password)
    
    #etablissement de la connexion procare
    connexion_procare = connection_db(connexion_string_procare)
    
    select_procare_query = '''
    WITH SCHOOL AS
    (SELECT 
           SCO.[SchoolID]
          ,SCO.[Code]
          ,SCO.[SchoolName]
          ,SCO.[Database]
          ,CHI.[ChildSchoolID]
          ,CHI.PersonID
      FROM [dbo].[STG_PROCARE_G_SCHOOLS] SCO
      INNER JOIN STG_PROCARE_AR_CHILDSCHOOL CHI
        ON (SCO.SchoolID = CHI.SchoolID))
    
    SELECT 
    SCH.ScheduleKeyID
    ,SCO.PersonID
    ,SCO.SchoolID
    ,SCH.[Database]
    ,(SCD.OutMinute-SCD.InMinute)/60.00 AS HoursWorked
    ,SCH.StartAppliesTo
    ,SCH.EndAppliesTo
    ,SCD.DayNumber
    ,ENR.StartDate
    ,ENR.EndDate
    ,SCH.ScheduleID
    ,GETDATE() AS ExtractDate 
    
    FROM STG_PROCARE_AR_SCHEDULE SCH
    INNER JOIN STG_PROCARE_AR_SCHEDULEDETAIL SCD
        ON (SCH.ScheduleKeyID=SCD.ScheduleKeyID
            and SCH.[Database]=SCD.[Database])
    INNER JOIN STG_PROCARE_G_TYPESTABLE TYP
        ON (SCH.ChildSchoolID=TYP.TypeID
            and SCH.[Database]=TYP.[Database])
    INNER JOIN STG_PROCARE_AR_ENROLLMENT ENR
        ON (SCH.ChildSchoolID=ENR.ChildSchoolID
            and SCH.[Database]=ENR.[Database])
    /*INNER JOIN STG_PROCARE_G_SCHOOLS SCO
        ON (SCH.ChildSchoolID=SCO.SchoolID
            and SCH.[Database]=SCO.[Database]) */
    INNER JOIN SCHOOL SCO
        ON (SCH.ChildSchoolID=SCO.ChildSchoolID)
    '''
    #creation du dataframe procare via requete sql sur sqlserver
    dataframe_procare = pandas_sql_requete_execution(select_procare_query)
    
    #configuration variables date
    # config = ConfigParser()
    # config.read('config.ini')
    # database_date = config['date']['database']
    # serverURL = config['date']['host']
    # username = config['date']['username']
    # password = config['date']['password']
    # port = config['date']['port']
    # driver=config['date']['driver']
    # table_name=config['date']['table_name']
    

    
    connection_string_date = connection_string_creation(driver,serverURL,database_date,username,password)
    #etablissement de la connexion date
    connexion_date = connection_db(connection_string_date)
    
    select_date_query = '''
    SELECT 
          [DT_DATE]
          ,[DAY_OF_WEEK]
          ,[LB_DAY_NAME]
      FROM [dbo].[D_TIME]
      '''
    #creation du dataframe date via requete sql sur sqlserver
    dataframe_date = pandas_sql_requete_execution(select_date_query)
    
    psql_join_query ='''
    SELECT * FROM data_procare pro
    INNER JOIN data_date DAT
    ON DAT.DT_DATE BETWEEN PRO.StartAppliesTo AND PRO.EndAppliesTo
    WHERE (PRO.DayNumber = 1 AND DAT.LB_DAY_NAME ='LUNDI')
    OR (PRO.DayNumber = 2 AND DAT.LB_DAY_NAME ='MARDI')
    OR (PRO.DayNumber = 3 AND DAT.LB_DAY_NAME ='MERCREDI')
    OR (PRO.DayNumber = 4 AND DAT.LB_DAY_NAME ='JEUDI')
    OR (PRO.DayNumber = 5 AND DAT.LB_DAY_NAME ='VENDREDI')
    OR (PRO.DayNumber = 6 AND DAT.LB_DAY_NAME ='SAMEDI')
    OR (PRO.DayNumber = 7 AND DAT.LB_DAY_NAME ='DIMANCHE')
    ;
    '''
    #creation d'un dataframe issu de la jointure de procare et date
    df_joined_procare_date = pandas_sql_requete_execution(psql_join_query)
    
    #creation du dataframe contenant les valeurs d'ETP 
    df_etp = read_csv_file('etp.csv')
    
    sql_etp_query = '''
    SELECT 
    PRO.*,
    ETP.[Correspondance ETP] 
    FROM df_psql PRO
    INNER JOIN df_etp ETP
    ON PRO.HoursWorked BETWEEN ETP.MinDailyHour AND ETP.MaxDailyHour
    '''
    
    #joiture entre les deux dataframe
    df_joined_procare_date_etp = pandas_sql_requete_execution(sql_etp_query)
    
    #supression des donnees du jour deja presentes dans la table PROCARE_ETP
    sql_db_execution('''DELETE FROM PROCARE_ETP WHERE ExtractDate = CONVERT (date, GETDATE())''')
    
    # On cree un nouveau DataFrame à l'image de la table finale
    df_insert_procareETP = pandas_sql_requete_execution('''SELECT 
    date(DT_DATE) AS jour,
    DayNumber,
    [Database],
    PersonID,
    SchoolID,
    [Correspondance ETP] AS ETP,
    date(ExtractDate) as ExtractDate
    FROM df_joined_procare_date_etp''')
    
    #creation de l'engine sql alchemy pour inserer dans la table
    engine = sqlalchemy_create_engine(connection_string_date)
    
    #realisation de l'insertion
    sqalchemy_insert_into_table(table_name,engine)
    
flow.register(project_name="connexion_pyodbc")
    
LocalAgent().start()
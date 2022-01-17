#!/usr/bin/env python
# coding: utf-8

# # L'objectif de ce notebook sera de réaliser une ébauche de solution ETL en python

# In[1]:


#Import des packages necessaires à la réalisation du projet

import pyodbc 
import pyspark
import pandas as pd
import pandasql as ps
import sqlalchemy
import logging
from sqlalchemy.engine import URL,create_engine

# In[2]:
logging.basicConfig(filename='LOG_PYETL.log',level=logging.INFO,format='%(asctime)s|%(levelname)s|%(message)s',
                            datefmt='%H:%M:%S')

logging.info("creation des variables depuis le fichier de parametrage")
#creation des variables depuis le fichier de configuration qui doit être placé dans le même dossier que le notebook
from configparser import ConfigParser
#recuperation de la configuration de la connexion stg_babilou pour la donnee procare
config = ConfigParser()
config.read('config.ini')
database_procare = config['procare']['database']
sqlsUrl = config['procare']['host']
username = config['procare']['username']
password = config['procare']['password']
port = config['procare']['port']

#recuperation de la configuration de la connexion stg_babilou pour la donnee date
config = ConfigParser()
config.read('config.ini')
database_date = config['date']['database']
sqlsUrl = config['date']['host']
username = config['date']['username']
password = config['date']['password']
port = config['date']['port']


# ## Utilisation de pyodbc pour la connexion à la base de donnée SQL SERVER

# In[3]:



#String de connexion procare
connection_string_procare ='DRIVER={SQL Server Native Client 11.0};SERVER='+sqlsUrl+';DATABASE='+database_procare+';UID='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryIntegrated'
#String de connexion date
connection_string_date ='DRIVER={SQL Server Native Client 11.0};SERVER='+sqlsUrl+';DATABASE='+database_date+';UID='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;Authentication=ActiveDirectoryIntegrated'


# In[4]:

logging.info("Etablissement de la connexion a la base STG_BABILOU")

try:
    #connexion a la base STG_BABILOU pour recuperer les donnees procare
    cnxn_procare:pyodbc.Connection= pyodbc.connect(connection_string_procare)
except Exception as exception_connexion_procare:
    logging.warning(exception_connexion_procare)
else:
    pass

logging.info("Etablissement de la connexion a la base DW_BABILOU")
try:
    #connexion a la base DWH_BABILOU pour recuperer les donnees de dates
    cnxn_date:pyodbc.Connection= pyodbc.connect(connection_string_date)
except Exception as exception_connexion_date:
    logging.warning(exception_connexion_date)
else:
    pass


# In[6]:


#Requetes pour interroger les bases DW_BABILOU et STG_BABILOU

#select_query= 'Select * from STG_PROCARE_AR_SCHEDULE'

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

select_date_query = '''
SELECT 
      [DT_DATE]
      ,[DAY_OF_WEEK]
      ,[LB_DAY_NAME]
  FROM [dbo].[D_TIME]
  '''


# In[7]:

logging.info("creation du dataframe procare")
# Creation du dataframe procare
try:
    data_procare = pd.read_sql(select_procare_query,cnxn_procare)
except Exception as ErreurCreationDataFrameProcare:
    logging.warning(ErreurCreationDataFrameProcare)
else:
    pass

logging.info("creation du dataframe date")
# Creation du dataframe date
try:
    data_date = pd.read_sql(select_date_query,cnxn_date)
except Exception as ErreurCreationDataFrameDate:
    logging.warning(ErreurCreationDataFrameProcare)
else:
    pass

#fermeture des connexions
cnxn_procare.close()
cnxn_date.close()


# ## Utilisation des packages pandas et pandasql pour respecter les règles de gestion

# * On cree la query qui sert à faire la jointure entre les deux dataframes precedemment crees
# * On fait une jointure glissante sur les dates de début et de fin de contrats
# * Le filtre nous permet de récupérer les données cohérentes (le premier jour de la semaine est forcément lundi)

# In[8]:


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


# * Execution de la query via pandasql pui affichage des 5 premières lignes

# In[10]:

logging.info("creation de la jointure entre les deux dataframe")
try: 
    df_psql = ps.sqldf(psql_join_query)
except Exception as ErreurJointure:
    logging.warning(ErreurJointure)
else:
    pass
    
#Pour voir les 5 premières lignes df_psql.head(5)


# #### On recupere les donnees etp dans un dataframe pour les rajouter dans le dataframe final

# In[11]:

logging.info("creation du dataframe ETP depuis le csv")

try:
    df_etp = pd.read_csv('etp.csv',sep=';')
except Exception as ErreurDataframeETP:
    logging.warning(ErreurDataframeETP)
else:
    pass

# * Conversion des donnees en decimal
df_etp_int = df_etp.astype('float64')


# In[13]:

#query pour joindre les etp correspondants
sql_etp_query = '''
SELECT 
PRO.*,
ETP.[Correspondance ETP] 
FROM df_psql PRO
INNER JOIN df_etp ETP
ON PRO.HoursWorked BETWEEN ETP.MinDailyHour AND ETP.MaxDailyHour
'''


# In[16]:

logging.info("lancement de la jointure avec les ETP")

try:
    df_join_etp = ps.sqldf(sql_etp_query)
except Exception as ErreurJointureETP:
    logging.warning(ErreurJointureETP)
else:
    pass

#df_join_etp.head(2)


# ## Creation de la table dans SQL Server via Pyodbc

# In[25]:


#on établit une nouvelle connexion avec la base DW_BABILOU
connexion_dwh = pyodbc.connect(connection_string_date)
dwh_crusor = connexion_dwh.cursor()


# In[ ]:
#Table déjà installée -> scrip commenté mais fonctionnel
# dwh_crusor.execute('''
# CREATE TABLE [dbo].PROCARE_ETP(
#     Jour DATE,
#     DayNumber INT,
#     [Database] VARCHAR(40),
#     PersonID INT,
#     SchoolID INT,
#     ETP DECIMAL (3,2),
#     ExtractDate DATE
# );
# ''')
#connexion_dwh.commit() #sert à confirmer les changements dans la base


# #### on efface les données de la table dont l'extractdate est à la date du jour 

# In[31]:

logging.info("Effacement des donnees deja presentes a la date du jour")
try:
    dwh_crusor.execute('''DELETE FROM PROCARE_ETP WHERE ExtractDate = CONVERT (date, GETDATE())''')
except Exception as ErreurDeleteDateJour:
    logging.warning(ErreurDeleteDateJour)
else:
    pass
    
connexion_dwh.commit()


# ## Insertion des donnees dans la table PROCATE_ETP nouvellement creee

# In[19]:

logging.info("creation d'un dataframe a l'image de la table finale")
# On créé un nouveau DataFrame à l'image de la table finale
try:
    df_insert_procareETP = ps.sqldf('''SELECT 
    date(DT_DATE) AS jour,
    DayNumber,
    [Database],
    PersonID,
    SchoolID,
    [Correspondance ETP] AS ETP,
    date(ExtractDate) as ExtractDate
    FROM df_join_etp''')

except Exception as ErreurDataFrameFinal:
    logging.warning(ErreurDataFrameFinal)

else:
    pass


# In[20]:


#df_insert_procareETP.head(2)

# ## Script d'insertion dans la table, pandas.to_sql et sqlachemy

# * On cree les informations de connexion à la table en utilisant le connexion string utilise precedemment dans pyodbc

# In[37]:




connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string_date})

engine = create_engine(connection_url,fast_executemany=True)


# In[38]:


#https://docs.sqlalchemy.org/en/14/dialects/mssql.html#module-sqlalchemy.dialects.mssql.pyodbc
#https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
#df_insert_procareETP_reduced = df_insert_procareETP.head(500) -> a utiliser comme variable dans le cas ou le traitement es trop long
logging.info("lancement de l'insertion")
try:
    df_insert_procareETP.to_sql('PROCARE_ETP',engine,if_exists='append',index=False,chunksize=1000)
except Exception as ErreurInsertion:
    logging.warning(ErreurInsertion)
else:
    pass
logging.info("fin de l'insertion")
# In[23]:


#vérification de l'insertion des lignes 
#pd.read_sql('PROCARE_ETP',engine)


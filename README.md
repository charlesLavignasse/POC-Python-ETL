# POC-Python-ETL
### Developpement d'un outil ETL sur Python

Projet réalisé pour un client qui d'ordinaire traite ses flux ETL via Talend. Dans une démarche de réduction des coûts liés à la licence, une solution ETL avec Python a été envisagée.
### Le projet pour objectif :
* Se connecter à une base de données SQL SERVER 
* Interroger des tables sur deux bases différentes et effectuer une requete complexe afin de regrouper les informations.
* Effectuer des transformations à l'aide de python.
* Avoir un suivi temporel des temps déxécutions
* vVariabiliser et industrialiser la procédure

## Environnement technologique : 

Dans un premier temps, j'ai choisi la suite Anaconda pour réaliser le projet, mais suite à des problèmes d'interdépendances entre packages et/ou des soucis d'installation, j'ai préféré désinstaller Anaconda et travailler avec un environnement virtuel python qui comprenait uniquement les packages dont nous aurions besoin. Ceci présente un double avantage, en effet, le projet sera déployable unitairement sans problème de dépendances.

* PYODBC pour la connexion en base
* Pandas pour la gestion des données
* Pandasql pour effectuer des transformations entre Dataframes
* SqlAlchemy pour l'insertion en base
* Prefect pour l'orchestration

Une tentative d'orchestration via prefect a également été faite, mais suite à des problèmes de connexion, le script est resté à l'état théorique.

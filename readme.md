-- Je me suis inspirer en partie de  lassilias
https://github.com/lassilias/IMDB_db_Hadoop_Hive/blob/main/creation_partition_queries_hive_hadoop.sh
En tentant de faire plus simple 


-------------------------------------------------------------------Hadoop-Hive --------------------------------------------------------------------
## -------------------------BASH
mkdir data && cd $_
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/docker-compose.yml \
 -O /home/ubuntu/docker-compose.yml
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/hadoop-hive.env \
 -O /home/ubuntu/hadoop-hive.env

docker-compose up -d
-------------------------------------------------------------------TABLE name.basics.tsv.gz --------------------------------------------------------------------


wget https://datasets.imdbws.com/name.basics.tsv.gz \
wget https://datasets.imdbws.com/title.ratings.tsv.gz \
wget https://datasets.imdbws.com/title.principals.tsv.gz \
wget https://datasets.imdbws.com/title.akas.tsv.gz \
wget https://datasets.imdbws.com/title.basics.tsv.gz \
wget https://datasets.imdbws.com/title.crew.tsv.gz ;

docker exec -it hive-server bash

## --------------------------HIVE
hive

CREATE DATABASE imdb;

USE imdb;

### --- j'ai choisi de créer une table externe afin de persister le fichier des données. 
CREATE EXTERNAL TABLE names( nconst VARCHAR(20), 
                    primaryname VARCHAR(64), 
                    birthYear SMALLINT, 
                    deathYear SMALLINT, 
                    primaryProfession ARRAY<VARCHAR(64)>, 
                    knownForTitles ARRAY<VARCHAR(64)> )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

### --- En chargeant avec "LOCAL" je n'ai plus besoin de l'étape de chargement dans HDFS.  
LOAD DATA LOCAL INPATH '/data/name.basics.tsv.gz' INTO TABLE names;

### - --Comme les VM sont limitées, nous allons aussi limité la taille des tables et par la même occasion retirer la première ligne qui constituait le en tête du fichier.  
INSERT OVERWRITE TABLE names  SELECT * FROM names WHERE nconst<>'nconst' LIMIT 1000;



### Pour le reste des tables nous procédons de la même manière
-------------------------------------------------------------------TABLE title.ratings.tsv.gz -------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE ratings( tconst VARCHAR(20), 
                      averageRating FLOAT, 
                      numVotes INT )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/data/title.ratings.tsv.gz' INTO TABLE ratings;
INSERT OVERWRITE TABLE ratings  SELECT * FROM ratings WHERE tconst<>'tconst' LIMIT 1000;

------------------------------------------------------------------TABLE title.principals.tsv.gz ----------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE principals( tconst VARCHAR(20),  
                          ordering SMALLINT, 
                          nconst  VARCHAR(64), 
                          category  VARCHAR(64), 
                          job VARCHAR(64),
                          characters ARRAY<VARCHAR(64)> )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/data/title.principals.tsv.gz' INTO TABLE principals;
INSERT OVERWRITE TABLE principals  SELECT * FROM principals WHERE tconst<>'tconst' LIMIT 1000;

------------------------------------------------------------------TABLE title.akas.tsv.gz ----------------------------------------------------------------------------------------------------------------

CREATE EXTERNAL TABLE akas( titleId VARCHAR(64),  
                    ordering SMALLINT, 
                    title VARCHAR(64), 
                    region VARCHAR(64), 
                    lang VARCHAR(64),
                    types  ARRAY<VARCHAR(64)>, 
                    attributes ARRAY<VARCHAR(64)>, 
                    isOriginalTitle  BOOLEAN)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/data/title.akas.tsv.gz' INTO TABLE akas;
INSERT OVERWRITE TABLE akas  SELECT * FROM akas WHERE titleId<>'titleId' LIMIT 1000;
--------------------------------------------------------------------------TABLE title.basics.tsv

CREATE EXTERNAL TABLE basics( tconst VARCHAR(64),  
                    titleType VARCHAR(64), 
                    primaryTitle VARCHAR(64), 
                    originalTitle VARCHAR(64), 
                    isAdult SMALLINT,
                    startYear SMALLINT, 
                    endYear  SMALLINT, 
                    runtimeMinutes INT, 
                    genres ARRAY<VARCHAR(64)>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/data/title.basics.tsv.gz' INTO TABLE basics;
INSERT OVERWRITE TABLE basics  SELECT * FROM basics WHERE tconst<>'tconst' LIMIT 1000;

--------------------------------------------------------------------TABLE title.crew.tsv

CREATE EXTERNAL TABLE crew( tconst  VARCHAR(64),  
                  directors  ARRAY<VARCHAR(64)>, 
                  writers  ARRAY<VARCHAR(64)>)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH '/data/title.crew.tsv.gz' INTO TABLE crew;
INSERT OVERWRITE TABLE crew  SELECT * FROM crew WHERE tconst<>'tconst' LIMIT 1000;
# Création des partitions

## Afin de savoir quelles données partitionner, il faut d'abords savoir quelles serons les données réquêtées et donc créer les requêtes. 

## Pour cela, j'ai repris les requêtes d'un exo précedant.  
### les notes moyennes des 10 titres les mieux notés dans la base de données par genre en france, 
### Pour cette requête il faudrait partitionner; rating de la table rating et title des tables rating et title. 

### Nous savons qu'il y a plusieurs régions, il serait bien de les prendre la colonne région pour partionner la table aka


SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


CREATE EXTERNAL TABLE akas_partitions( titleId VARCHAR(64),  
                    ordering SMALLINT, 
                    title VARCHAR(64),  
                    lang VARCHAR(64),
                    types  ARRAY<VARCHAR(64)>, 
                    attributes ARRAY<VARCHAR(64)>, 
                    isOriginalTitle  BOOLEAN)
PARTITIONED BY (region VARCHAR(64))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


INSERT INTO TABLE akas_partitions PARTITION (region) SELECT titleId, ordering, title, lang, types, attributes, isOriginalTitle, region FROM akas;
### ce qui donne 35 partitions


### La table principals quant à elle peut très bien se partitionner par catégories


CREATE EXTERNAL TABLE principals_partitions( tconst VARCHAR(20),  
                          ordering SMALLINT, 
                          nconst  VARCHAR(64), 
                          job VARCHAR(64),
                          characters ARRAY<VARCHAR(64)> )
PARTITIONED BY (category  VARCHAR(64))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE principals_partitions PARTITION (category) SELECT tconst, ordering, nconst,job, characters, category FROM principals;
### -- ce qui donne 10 partitions

CREATE TABLE names_partitions( nconst STRING,  primaryname STRING, birthYear SMALLINT, deathYear SMALLINT, knownForTitles ARRAY<varchar(64)>,secondProfession  STRING , thirdProfession STRING)
PARTITIONED BY (firstProfession STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;


INSERT INTO TABLE names_partitions
PARTITION (firstProfession)
SELECT nconst, primaryname, birthYear, deathYear, knownForTitles,primaryProfession[1] as s, primaryProfession[2] as t, primaryProfession[0] as firstProfession
FROM names;
### -- ce qui donne 15 partitions


### --- Pour rating, il me semble que les "Buckets" sont plus appropriés car il n'y pas vraiment de possibilité de grouper les données. 

CREATE TABLE rating_partitions(tconst VARCHAR(20), 
                      averageRating FLOAT,
                      numVotes INT ) 
CLUSTERED BY (averageRating) INTO 10 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE rating_partitions SELECT tconst,averageRating, numVotes FROM ratings;

### J'ai choisi de partitioner par titleType car c'est ce qui me semble le plus logique  
CREATE TABLE basics_partition( tconst VARCHAR(64), 
                                primaryTitle VARCHAR(64), 
                                originalTitle VARCHAR(64), 
                                isAdult BOOLEAN,
                                startYear INT, 
                                endYear  INT, 
                                runtimeMinutes INT, 
                                genres ARRAY<VARCHAR(64)> )
PARTITIONED BY (titleType VARCHAR(64))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE basics_partition
PARTITION (titleType)
SELECT tconst,primaryTitle ,originalTitle,isAdult, startYear,endYear,runtimeMinutes,genres, titleType
FROM basics;
### cela n'a créé qsue 2 partitions 


CREATE TABLE crew_partitions(tconst VARCHAR(64), 
                              writers ARRAY<VARCHAR(64)>)
PARTITIONED BY (directors VARCHAR(64))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY "\t"
COLLECTION ITEMS terminated by ','
LINES TERMINATED BY "\n"
STORED AS TEXTFILE;

INSERT INTO TABLE crew_partitions
PARTITION (directors)
SELECT tconst, writers,directors
FROM crew;

#  ----------------------Requests----------------------- 

git config --global user.email "idri.malek@gmail.com"
git config --global user.name "IDRIMalek"
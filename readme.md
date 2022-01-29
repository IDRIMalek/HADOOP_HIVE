## Créer l'envirronnement d'haddop hive
mkdir /home/ubuntu/data
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/docker-compose.yml \
 -O /home/ubuntu/docker-compose.yml
wget https://dst-de.s3.eu-west-3.amazonaws.com/hadoop_hive_fr/hadoop-hive.env \
 -O /home/ubuntu/hadoop-hive.env

## Dans le container hive server
apt update
apt install wget

## Téléchargement des gz de IMDB 

wget https://datasets.imdbws.com/title.akas.tsv.gz  \
wget https://datasets.imdbws.com/title.basics.tsv.gz   \
wget https://datasets.imdbws.com/title.crew.tsv.gz  \
wget https://datasets.imdbws.com/title.episode.tsv.gz   \
wget https://datasets.imdbws.com/title.principals.tsv.gz  \
wget https://datasets.imdbws.com/title.ratings.tsv.gz  \
wget https://datasets.imdbws.com/name.basics.tsv.gz 

## loading files into HDFS
hdfs dfs -put title.akas.tsv.gz /title.akas.tsv.gz 
hdfs dfs -put title.basics.tsv.gz /title.basics.tsv.gz 
hdfs dfs -put title.crew.tsv.gz  /title.crew.tsv.gz 
hdfs dfs -put title.episode.tsv.gz /title.episode.tsv.gz 
hdfs dfs -put title.principals.tsv.gz /title.principals.tsv.gz 
hdfs dfs -put title.ratings.tsv.gz  /title.ratings.tsv.gz 
hdfs dfs -put name.basics.tsv.gz /name.basics.tsv.gz 

## checking that files are indeed in HDFS
hdfs dfs -ls /

## -console HIVE  

### Création bd
-- creating a database
CREATE DATABASE my_hive_db;

-- selecting the database
USE my_hive_db;


### Création tables

# -------------------------------BASICS
-- DROP TABLE my_hive_db.title_basics;

CREATE TABLE `my_hive_db.title_basics_raw`(
  `tconst` varchar(10), 
  `titleType` varchar(64), 
  `primaryTitle` varchar(64), 
  `originalTitle` varchar(64), 
  `isAd` varchar(4),  
  `startYear` varchar(4), 
  `endYear` varchar(4), 
  `runtimeMinutes` int, 
  `genres` array<varchar(64)>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

LOAD DATA INPATH '/title.basics.tsv.gz' INTO TABLE my_hive_db.title_basics_raw; 


CREATE TABLE `my_hive_db.title_basics`(
  `tconst` varchar(10), 
  `titleType` varchar(64), 
  `primaryTitle` varchar(64), 
  `originalTitle` varchar(64), 
  `isAd` varchar(4),  
  `startYear` varchar(4), 
  `endYear` varchar(4), 
  `runtimeMinutes` int, 
  `genres` array<varchar(64)>)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

INSERT INTO my_hive_db.title_basics SELECT * From my_hive_db.title_basics_raw LIMIT 1000;
DROP TABLE my_hive_db.title_basics_raw;

# -------------------------------AKAS

// ---------------------------------
CREATE TABLE `my_hive_db.akas_raw`(
  `titleId` varchar(64), 
  `ordering` int, 
  `title` varchar(64), 
  `region` varchar(64), 
  `language` varchar(64),  
  `types` varchar(64), 
  `attributes` varchar(64), 
  `isOriginalTitle` varchar(1))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

LOAD DATA INPATH '/opt/data/title.akas.tsv.gz' INTO TABLE my_hive_db.akas_raw; 

// ---------------------------------
CREATE TABLE `my_hive_db.akas`(
  `titleId` varchar(64), 
  `ordering` int, 
  `title` varchar(64), 
  `region` varchar(64), 
  `language` varchar(64),  
  `types` varchar(64), 
  `attributes` varchar(64), 
  `isOriginalTitle` varchar(1))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

INSERT INTO my_hive_db.akas SELECT * From my_hive_db.akas_raw LIMIT 1000;
DROP TABLE my_hive_db.akas_raw;


# ----------------------CREW

// ---------------------------------
CREATE TABLE `my_hive_db.crew_raw`(
  `tconst` varchar(10), 
  `directors` varchar(64), 
  `writers` varchar(64))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

LOAD DATA INPATH '/opt/data/title.crew.tsv.gz' INTO TABLE my_hive_db.crew_raw; 

// ---------------------------------
CREATE TABLE `my_hive_db.crew`(
  `tconst` varchar(10), 
  `directors` varchar(64), 
  `writers` varchar(64))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

INSERT INTO my_hive_db.crew SELECT * From my_hive_db.crew_raw LIMIT 1000;
DROP TABLE my_hive_db.crew_raw;

# ----------------------principals

// ---------------------------------
CREATE TABLE `my_hive_db.principals_raw`(
  `tconst` varchar(10), 
  `ordering` int, 
  `nconst` varchar(64), 
  `category` varchar(64), 
  `job` varchar(64), 
  `characters` varchar(64))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

LOAD DATA INPATH '/opt/data/title.principals.tsv.gz' INTO TABLE my_hive_db.principals_raw; 

// ---------------------------------
CREATE TABLE `my_hive_db.principals`(
  `tconst` varchar(10), 
  `ordering` int, 
  `nconst` varchar(64), 
  `category` varchar(64), 
  `job` varchar(64), 
  `characters` varchar(64))
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

INSERT INTO my_hive_db.principals SELECT * From my_hive_db.principals_raw LIMIT 1000;
DROP TABLE my_hive_db.principals_raw;

# ----------------------ratings

// ---------------------------------
CREATE TABLE `my_hive_db.ratings_raw`(
  `tconst` varchar(10), 
  `averageRating` float, 
  `numVotes` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

LOAD DATA INPATH '/opt/data/title.ratings.tsv.gz' INTO TABLE my_hive_db.ratings_raw; 

// ---------------------------------
CREATE TABLE `my_hive_db.ratings`(
  `tconst` varchar(10), 
  `averageRating` float, 
  `numVotes` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

INSERT INTO my_hive_db.ratings SELECT * From my_hive_db.ratings_raw LIMIT 1000;
DROP TABLE my_hive_db.ratings_raw;

# ----------------------name_basics

// ---------------------------------
CREATE TABLE `my_hive_db.name_basics_raw`(
  `tconst` varchar(10), 
  `averageRating` float, 
  `numVotes` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

LOAD DATA INPATH '/opt/data/name.basics.tsv.gz' INTO TABLE my_hive_db.name_basics_raw; 

// ---------------------------------
CREATE TABLE `my_hive_db.name_basics`(
  `tconst` varchar(10), 
  `averageRating` float, 
  `numVotes` int)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES ( 
  'field.delim'='\t', 
  'line.delim'='\n', 
  'serialization.format'=',') 
STORED AS TEXTFILE;

INSERT INTO my_hive_db.name_basics SELECT * From my_hive_db.name_basics_raw LIMIT 1000;
DROP TABLE my_hive_db.name_basics_raw;
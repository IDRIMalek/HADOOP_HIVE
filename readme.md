# Téléchargement des gz de IMDB 

wget https://datasets.imdbws.com/title.akas.tsv.gz  \
wget https://datasets.imdbws.com/title.basics.tsv.gz   \
wget https://datasets.imdbws.com/title.crew.tsv.gz  \
wget https://datasets.imdbws.com/title.episode.tsv.gz   \
wget https://datasets.imdbws.com/title.principals.tsv.gz  \
wget https://datasets.imdbws.com/title.ratings.tsv.gz  \
wget https://datasets.imdbws.com/name.basics.tsv.gz \

# loading files into HDFS
hdfs dfs -put ~/title.akas.tsv.gz /title.akas.tsv.gz \
hdfs dfs -put ~/title.basics.tsv.gz /title.basics.tsv.gz \
hdfs dfs -put ~/title.crew.tsv.gz  /title.crew.tsv.gz \
hdfs dfs -put ~/title.episode.tsv.gz /title.episode.tsv.gz \
hdfs dfs -put ~/title.principals.tsv.gz /title.principals.tsv.gz \
hdfs dfs -put ~/title.ratings.tsv.gz  /title.ratings.tsv.gz \
hdfs dfs -put ~/name.basics.tsv.gz /name.basics.tsv.gz 

# checking that files are indeed in HDFS
hdfs dfs -ls /

git config --global user.email "idri.malek@gmail.com"
git config --global user.name "IDRIMalek"
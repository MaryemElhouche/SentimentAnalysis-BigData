-- Charger le dataset depuis HDFS
sentiments = LOAD '/user/cloudera/SentimentAnalysis/data/sentimentsData_clean.csv' 
    USING PigStorage(',') 
    AS (target:chararray, id:long, date:chararray, flag:chararray, user:chararray, text:chararray);

-- Filtrer l'en-tête si elle existe
sentiments_filtered = FILTER sentiments BY target != 'target';

-- Compter les sentiments positifs et négatifs
grouped = GROUP sentiments_filtered BY target;
counts = FOREACH grouped GENERATE group AS sentiment, COUNT(sentiments_filtered) AS count;

-- Stocker le résultat dans HDFS
STORE counts INTO '/user/cloudera/SentimentAnalysis/pig_output' USING PigStorage(',');


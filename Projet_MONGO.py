
### contro + z pour revenir en arrière
### installer pip 
#### Extraction du fichier csv 
! curl -O https://blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/127383/transactions.zip
## deziper le fichier de donnees
sudo apt install unzip
! unzip transactions.zip
###sudo apt install python3-pip
### python3 -m pip install --upgrade pip
###python3 -m pip install pymongo

###python3 -m pip install  pyspark



sudo apt install python3-pymongo
python3
import pymongo
from pymongo import MongoClient
import csv
from pprint import pprint  # Permet d'afficher des dictionnaires Python plus lisibles
from bson import ObjectId

## changer le mot de passe chaque fois

client = MongoClient('3.252.82.203:27017', 27017, username='admin', password='e32wJ01SdPiwqLBV', authSource='admin',serverSelectionTimeoutMS=60000)


### ETAPE 1 : Intégration des données dans MongoDB
#### Connection au sandbox
#### Creation de notre base de données
db = client['banque']
#### creation de la collection
transactions = db['transaction']
#### lecture du fichier csv et insertion des données dans MongoDB (trop lent, tester la methode de Maxime )
with open('transactions.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for row in csv_reader:
        transactions.insert_one(row)
## afficher les colonnes
    for doc in transactions.find_one():
        print(doc)

## Afficher les objectID
for doc in db.transaction.find():
    print(doc['_id'])


### Inserer 100 documents pour le test agregation
with open('transactions.csv', 'r') as file:
    csv_reader = csv.DictReader(file)
    for index, row in enumerate(csv_reader):
        if index < 100:  # Modifier cette condition pour insérer le nombre de documents souhaité
            transactions.insert_one(row)
        
#### Voyons comment sont nos données   
pprint(db["transaction"].find_one({"_id": ObjectId("667eacf9f65c3445882b0104")}))

### ETAPE 2: Réalisation des calculs d'agrégation avec Spark
### Connection au workspace jupyterLab-Apache Spark


!curl https://blent-mirror.s3.amazonaws.com/mongodb/mongodb-drivers.zip -o drivers.zip
!unzip drivers.zip -d Jar
import findspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
##from mongodb_spark import MongoSpark
## modifier le mot de passe à chaque connection
from pyspark.sql import SparkSession

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config('spark.driver.extraClassPath','Jar/*') \
    .config("spark.mongodb.read.connection.uri", "mongodb://admin:qID52eKqpZ1E9hVm@52.17.170.252:27017/admin") \
    .config("spark.mongodb.write.connection.uri", "mongodb://admin:qID52eKqpZ1E9hVm@52.17.170.252:27017/admin") \
    .getOrCreate()
my_spark

dataFrame = my_spark.read \
    .format("mongodb") \
    .option("database", "banque") \
    .option("collection", "transaction") \
    .load()



 dataFrame.show()   


# Convertir la date au format approprié pour les calculs
df = dataFrame.withColumn("TRANS_DT", to_date(col("TRANS_DT"), "MM/dd/yyyy"))
df.show()

# Calculer le montant total de transactions réalisées par semaine pour chaque division

df_agg = df.groupBy(weekofyear("TRANS_DT").alias("semaine"), "DIV_NAME").agg(sum("MERCHANDISE_AMT").alias("total_transactions"))

# Afficher les résultats
df_agg.show()


# Calculer le montant total dépensé pour chaque semaine

df_agg = df.groupBy(F.weekofyear(F.col("TRANS_DT")).alias("semaine"), "DIV_NAME").agg(F.sum("MERCHANDISE_AMT").alias("total_transactions"))

df_agg = df_agg.withColumn("part_total", F.col("total_transactions") / F.sum("total_transactions").over(Window.partitionBy("semaine")))

df_agg.show()


## Pour calculer la part que représente chaque division dans un département (DEPT_NAME) dans le montant total dépensé chaque semaine
df_agg = df.groupBy(F.when(F.col("TRANS_DT").isNotNull(), F.weekofyear(F.col("TRANS_DT"))).alias("semaine"), "DIV_NAME", "DEPT_NAME").agg(F.sum("MERCHANDISE_AMT").alias("total_transactions"))

df_agg = df_agg.withColumn("part_total", F.col("total_transactions") / F.sum("total_transactions").over(Window.partitionBy("semaine", "DEPT_NAME")))

df_agg.show()


## Pour calculer la part que représente chaque division dans un département (DEPT_NAME) dans le montant total dépensé chaque semaine
df_agg = df.groupBy(F.when(F.col("TRANS_DT").isNotNull(), F.weekofyear(F.col("TRANS_DT"))).alias("semaine"), "DIV_NAME", "DEPT_NAME").agg(F.sum("MERCHANDISE_AMT").alias("total_transactions"))

df_agg = df_agg.withColumn("part_total", F.col("total_transactions") / F.sum("total_transactions").over(Window.partitionBy("semaine", "DEPT_NAME")))

df_agg.show()


## L'évolution (en pourcentage) du montant total des transactions entre deux semaines successives pour un même département
df_agg = df.groupBy(F.when(F.col("TRANS_DT").isNotNull(), F.weekofyear(F.col("TRANS_DT"))).alias("semaine"), "DEPT_NAME").agg(F.sum("MERCHANDISE_AMT").alias("total_transactions"))

df_agg = df_agg.withColumn("part_total", (F.col("total_transactions") - F.lag("total_transactions").over(Window.partitionBy("DEPT_NAME").orderBy("semaine"))) / F.lag("total_transactions").over(Window.partitionBy("DEPT_NAME").orderBy("semaine")))

df_agg = df_agg.withColumn("part_total", F.when(F.col("part_total").isNotNull(), F.col("part_total") * 100).otherwise(None))

df_agg = df_agg.withColumn("part_total", F.when(F.col("part_total") < 0, 0).otherwise(F.col("part_total")))

df_agg.show()



client.close()

### ETAPE 3 : Automatisation du pipeline de données

## AIRFLOW ou OOZIE
<workflow-app name="data_pipeline" xmlns="uri:oozie:workflow:0.5">
  <start to="spark-node"/>
  <action name="spark-node">
    <spark xmlns="uri:oozie:spark-action:0.2">
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
      <configuration>
        <property>
          <name>mapred.job.queue.name</name>
          <value>${queueName}</value>
        </property>
        <property>
          <name>oozie.libpath</name>
          <value>${nameNode}/user/${user.name}/share/lib</value>
        </property>
      </configuration>
      <main-class>my.spark.MainClass</main-class>
    </spark>
    <ok to="sqoop-node"/>
    <error to="fail"/>
  </action>
  <action name="sqoop-node">
    <sqoop xmlns="uri:oozie:sqoop-action:0.4">
      <job-tracker>${jobTracker}</job-tracker>
      <name-node>${nameNode}</name-node>
      <configuration>
        <property>
          <name>mapred.job.queue.name</name>
          <value>${queueName}</value>
        </property>
      </configuration>
      <command>UPDATE weekly_transactions SET total_transactions = (SELECT SUM(MERCHANDISE_AMT) FROM transactions WHERE WEEKOFYEAR = {kwargs['week']} AND DEPT_NAME = {kwargs['dept']}) WHERE WEEKOFYEAR = {kwargs['week']} AND DEPT_NAME = {kwargs['dept']} IF NOT EXISTS (SELECT * FROM weekly_transactions WHERE WEEKOFYEAR = {kwargs['week']} AND DEPT_NAME = {kwargs['dept'}) INSERT INTO weekly_transactions (WEEKOFYEAR, DEPT_NAME, total_transactions) VALUES ({kwargs['week']}, {kwargs['dept']}, (SELECT SUM(MERCHANDISE_AMT) FROM transactions WHERE WEEKOFYEAR = {kwargs['week']} AND DEPT_NAME = {kwargs['dept'}))</command>
    </sqoop>
    <ok to="end"/>
    <error to="fail"/>
  </action>
  <kill name="fail">
    <message>Workflow failed, error message[${wf:errorMessage( mesage)}]</message>
  </kill>
  <end name="end"/>
</workflow-app>





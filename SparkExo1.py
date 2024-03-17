#!/usr/bin/env python
# coding: utf-8

# # Apache-Spark

# **Apache Spark** is an open source **distributed computing framework** (or parallel computing framework) provides an ability to perform **in memory computations** with minimum latency.  
# 
# It is an analytical engine which can load data from memory disk or other data storages like Hadoop Distributed File System (HDFS), Amazon S3 or NoSQL DataBases such as MongoDB, Cassandra...   
# 
# Spark is an advanced version of Hadoop also using clusters.  
# Apache-Spark supports Python with a Spark API **PySpark** (combination of **Python + Spark** which provides and interface for programming in a distributed computing environment.  
# 
# PySpark is famous for its ease of use as well as speed.
# There are others Spark API such as Scala or Java ...etc available as well.  
# 
# Algorithms in Spark MLlib are basically divided into supervised and unsupervised.
# 

# In[ ]:


# PART 1: Apache Spark Installation on Anaconda (PySpark)
# =================================================
# https://www.youtube.com/watch?v=AL6zTrlyAhc


# In[2]:


# Parameters

data_rep = '/Users/davidtbo/Documents/Data_Science/99_Data/'


# In[3]:


# Check the list of environments
get_ipython().system('conda env list')


# In[4]:


# Create an environment named pyspark-env
# !conda create -n pyspark-env
# !conda activate pyspark-env
# !conda deactivate


# In[ ]:


# Install pyspark
# !conda install pyspark

# To Check the pyspark installation

# import pyspark
# from pyspark.sql import SparkSession


# In[ ]:


# to know the kernel available in jupyter
# !jupyter kernelspec list

# to install jupyter kernel with conda environment we need to install ipykernel
# !conda install ipykernel

# install my new python environment named "pyspark-env" in jupyter (ipykernel)
# !python -n ipykernel install --user --name=pyspark-env

# install this to identify Spark on Jupyter notebook
# !conda install -c conda-forge findspark

# jupyter notebook

# choose pyspark-env


# In[5]:


# to find where is Spark
import findspark
findspark.init()
findspark.find()


# In[ ]:


# PART II : Create a SparkContext from Apache-Spark
# =================================================


# In[6]:


# https://www.youtube.com/watch?v=EB8lfdxpirM
# Import de la classe SparkContext du module pyspark.context. 
# SparkContext est l'objet principal qui représente la connexion 
# à un cluster Spark et permet d'exécuter des tâches sur ce cluster.

from pyspark.context import SparkContext


# In[7]:


# Since Spark 2.0, unified entry point for interaction with Spark
# combines the functionalities of SparkContext + SQLContext,HiveContext, StreamingContext
# Supports multiple programming languages (Scala, Java, Python, R)
# Importe de la classe SparkSession du module pyspark.sql.session. 
# SparkSession est une interface unifiée pour les fonctionnalités de traitement de données structurées de Spark, 
# y compris SQL et DataFrames.

from pyspark.sql.session import SparkSession


# In[ ]:


# Functionality Differences between SparkContext vs SparkSession:
# SparkContext
# Core functionality for low-level programming and cluster interaction
# Creates RDDs (Resilient Distributed Dataset)
# Performs transformations and defines actions
# SparkSession
# Extends SaprkContext functionality
# High-level abstractions like DataFrames and Datasets
# Supports structured querying using SQL or DataFrame API
# Provides data source APIs, machine learning algorithms, and streaming capabilities


# In[8]:


# Créer ou récupèrer une instance de SparkContext. 
# Si une instance existe déjà, elle est réutilisée. 
# Sinon, une nouvelle instance est créée en fonction de la configuration du cluster Spark.

sc = SparkContext.getOrCreate()


# In[9]:


# Créer une instance de SparkSession à partir de l'instance SparkContext précédemment créée. 
# SparkSession utilise SparkContext pour effectuer des opérations sur les données.

spark = SparkSession(sc)


# In[10]:


# To go to the Spark UI and see my activity:
sc


# In[11]:


# Shut down the current active SparkContext
sc.stop()


# In[ ]:


# Another way to create a SparkContext:
# Since version2.x the prefered entry point is a SparkSession
# Create SparkContext in Apache Spark version 2.x and later
from pyspark.sql import SparkSession

# Create a SparkSession
# builder: to create a SparkSession object
# .appName to provide an application name
spark = SparkSession.builder \
    .appName("MySparkApplication") \
    .getOrCreate()

# Get the SparkContext from the SparkSession
sc = spark.sparkContext


# In[ ]:


# To go to the Spark UI and see my activity:
sc


# In[ ]:


# to Shut down the current active SparkContext
sc.stop() #or spark.stop()


# In[12]:


# PART III: Spark RDD and RDD Operations
# ======================================
# Core concepts Deep Dive & Demo

# Resilient Distributed Datasets
# immutable
# distributed into multiple machine in a cluster
# resilient to failure (lineage tracks transformation for fault tolerance)
# lazily evaluated: exectution plan optimized, transformations evaluated when necessary
# fault-tolerance operations: map,filter,reduce, collect, count, save, etc...

# Transformations:
# create new RDDs by applying computation/manipulation
# lazy evaluation, lineage graph
# examples: map, filter, flatMap, reduceByKey, sortBy, join
# VS
# Actions:
# returns results or perform actions on RDD, triggering execution
# eager evaluation, data movement/computation immediately
# examples: collect, count, first, take, save, foreach


# In[13]:


# How to create RDDs ?

# create a SparkSession:
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD-Demo").getOrCreate()

# we create an RDD by parallelizing a list of number:
numbers = [1,2,3,4,5]
rdd = spark.sparkContext.parallelize(numbers)

# Collect action; Retrieve all elements of the RDD
rdd.collect()


# In[14]:


spark


# In[15]:


# Create an RDD from a list of tuples
data = [('Alice',25), ('Bob', 30), ('Charlie',35), ('Alice',40)]
rdd = spark.sparkContext.parallelize(data)
# Collect action; Retrieve all elements of the RDD
rdd.collect()


# In[16]:


# RDD operations
# ==============


# In[17]:


# count() the number of elements
count = rdd.count()
print(count)


# In[18]:


# take() retrieve the n elements of the RDD:
taken_elements = rdd.take(2)
print(taken_elements)


# In[19]:


# foreach() print each element of the RDD
rdd.foreach(lambda x : print(x))


# In[20]:


mapped_rdd = rdd.map(lambda x : (x[0].upper(), x[1]))


# In[21]:


mapped_rdd.collect()


# In[22]:


# filter transformation: filter where age > 30
filtered_rdd = rdd.filter(lambda x : x[1] > 30)
filtered_rdd.collect()


# In[23]:


rdd.collect()


# In[24]:


# ReduceByKey transformation: calculate the total age for each name
reduced_rdd = rdd.reduceByKey(lambda x,y : x+y)
reduced_rdd.collect()


# In[25]:


# SortBy transformation: sort the RDD by age in descending order
sorted_rdd = rdd.sortBy(lambda x : x[1], ascending=False)
sorted_rdd.collect()


# In[26]:


#rdd.saveAsTextFile("output.txt")


# In[27]:


import os
os.getcwd()


# In[28]:


get_ipython().system('ls -la')


# In[29]:


# Create RDD from the text file
rdd_text = spark.sparkContext.textFile("output.txt")
rdd_text.collect()


# In[30]:


# Shut down
spark.stop()


# In[161]:


# PART IV: Spark DataFrame Introduction

# Intro
# DataFrames in Apache Spark are powerful abstraction for distributed and structured data.
# DataFrame structure: similar to a table in a relational database (rows and columns)
# DF offers schema organization

# Advantages of DataFrames over RDDs
# Optimized Executiton:
# - Schema information enables query optimization and predicate pushdowns
# - Faster and more efficient data processing
# Ease of Use:
# - High-leve, SQL-like interface for data interaction
# - Simplified compared to complex RDD transformations
# Integration with Ecosystem:
# - Semaless integration with Spark's ecosystem (Spark SQL, MLlib, GraphX)
# - Leverage various libraries and functionalities.
# Built-in Optimization:
# - Leveraging Spark's Catalyst optimizer for advanced optimizations.
# - Predicate pushdown, constant folding, loop unrolling.
# Interoperability:
# - Easily convert to/from other data foramts (e.g. Pandas DataFrames)
# - Seamless integration with other data processing tools.


# In[31]:


# RDDs vs DataFrame

# Set the PySpark environment variables
import os
os.environ['SPARK_HOME'] = '/Users/davidtbo/anaconda3/envs/pyspark-env/lib/python3.12/site-packages/pyspark'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'


# In[32]:


# import the necessary modules:
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc


# In[33]:


# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-Demo").getOrCreate()


# In[34]:


get_ipython().system('ls -la')


# In[35]:


the_file = os.path.join(data_rep, 'Intro_Apache_Spark.txt')


# In[36]:


# Using RDDs
# ----------
# Read the text file
rdd = spark.sparkContext.textFile(the_file)
result_rdd = rdd.flatMap(lambda line: line.split(" ")) \
    .map(lambda word : (word, 1)) \
    .reduceByKey(lambda a,b : a + b) \
    .sortBy(lambda x : x[1], ascending=False)

result_rdd.take(10)


# In[37]:


# Using DataFrames
# ----------------
df = spark.read.text(the_file)
result_df = df.selectExpr("explode(split(value, ' ')) as word") \
    .groupby("word").count().orderBy(desc("count"))

result_df.take(10)


# In[38]:


spark


# In[39]:


spark.stop()


# ### California Housing Prices

# In[40]:


# Set the PySpark environment variables
import os
os.environ['SPARK_HOME'] = '/Users/davidtbo/anaconda3/envs/pyspark-env/lib/python3.12/site-packages/pyspark'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

# import the necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Create a SparkSession
spark = SparkSession.builder.appName("California-Housing-Prices").getOrCreate()


# ### Load California Housing Prices
# [https://www.kaggle.com/datasets/camnugent/california-housing-prices]

# In[41]:


# present working directory
#!pwd


# In[42]:


print(data_rep)


# In[43]:


get_ipython().run_cell_magic('bash', '', 'head -10 /Users/davidtbo/Documents/Data_Science/99_Data/housing.csv\n')


# In[44]:


# Read a CSV file in a DataFrame:
the_file = os.path.join(data_rep, 'housing.csv')


# In[45]:


df = spark.read.csv(the_file, header=True)


# In[46]:


# Display schema of DataFrame
df.printSchema()


# In[47]:


# Display the content of the DataFrame
df.show(5)


# In[48]:


# The datatypes are not correct we need to modify them
# For that, we need to import the necessary types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# In[49]:


# Definition of the schema
schema = StructType([
    StructField(name="longitude", dataType=DoubleType(),nullable = True),
    StructField(name="latitude", dataType=DoubleType(),nullable = True),
    StructField(name="housing_median_age", dataType=DoubleType(),nullable = True),
    StructField(name="total_rooms", dataType=DoubleType(),nullable = True),
    StructField(name="total_bedrooms", dataType=DoubleType(),nullable = True),
    StructField(name="population", dataType=DoubleType(),nullable = True),
    StructField(name="households", dataType=DoubleType(),nullable = True),
    StructField(name="median_income", dataType=DoubleType(),nullable = True),
    StructField(name="median_house_value", dataType=DoubleType(),nullable = True),
    StructField(name="ocean_proximity", dataType=StringType(),nullable = True)
    ])


# In[50]:


# Read a CSV file in a DataFrame:
the_file = os.path.join(data_rep, 'housing.csv')
df = spark.read.csv(the_file, header=True, schema=schema)


# In[51]:


# Display schema of DataFrame
df.printSchema()


# In[52]:


# Now the Schema is correct :)


# In[53]:


# Read CSV with inferSchema
# It allows Spark to automatically guess the data types of columns
the_file = os.path.join(data_rep, 'housing.csv')
df = spark.read.csv(the_file, header=True, inferSchema=True)


# In[54]:


# Display schema of DataFrame
df.printSchema()


# In[55]:


# Reading JSON Files into DataFrame
# Java Script Object Notation
# ==================================


# In[59]:


os.chdir('/Users/davidtbo/Documents/Data_Science/99_Data')


# In[60]:


# Single line JSON:


# In[61]:


get_ipython().run_cell_magic('bash', '', 'head -10 iris.json\n')


# In[62]:


# Read JSON
the_file = os.path.join(data_rep, 'iris.json')
df = spark.read.json(the_file)


# In[63]:


# Display schema of DataFrame
df.printSchema()


# In[64]:


# Display the content of the DataFrame
df.show(5)


# In[65]:


# Multi-lines JSON


# In[66]:


get_ipython().run_cell_magic('bash', '', 'head -50 starwarsintents.json\n')


# In[67]:


# Read multi-line JSON
# JSON is an array of record, records are separated by a coma
# each record is defined in a multiple line
# The file is Star Wars Chat Bot from kaggle
the_file = os.path.join(data_rep, 'starwarsintents.json')


# In[68]:


# For multiple-lines we have to set multiLine to True
df = spark.read.json(the_file, multiLine=True)


# In[69]:


# Display schema of DataFrame
df.printSchema()


# In[70]:


# Display the content of the DataFrame
df.show(50)


# In[71]:


# Reading Parquet Files into DataFrame
# ====================================


# In[73]:


# We read the housing California into a spark dataframe
# and export to a Parquet file:


# In[74]:


# 1. Read a CSV file in a DataFrame:
the_file = os.path.join(data_rep, 'housing.csv')
df = spark.read.csv(the_file, header=True, schema=schema)


# In[78]:


# 2. Write the dataframe into parquet file
parquet_file = os.path.join(data_rep, 'housing.parquet')
# df.write.parquet(parquet_file)


# In[79]:


# Read the parquet file back into a dataframe
df = spark.read.parquet(parquet_file)


# In[80]:


# Display schema of DataFrame
df.printSchema()

# Display the content of the DataFrame
df.show(10)


# In[81]:


# Stop the Spark Session (always a good practice)
spark.stop()


# In[82]:


# Spark DataFrame Operations
# ==========================

# Select, Filter, Group, Aggregate, Join, Sort, Drop, and more

# To facilitate the data analysis


# In[83]:


# Set the PySpark environment variables
import os
os.environ['SPARK_HOME'] = '/Users/davidtbo/anaconda3/envs/pyspark-env/lib/python3.12/site-packages/pyspark'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

# import the necessary modules
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame-Operations").getOrCreate()


# In[84]:


# Supermarket sales (kaggle)
# Historical records of sales data in 3 different supermarkets
# https://www.kaggle.com/datasets/aungpyaeap/supermarket-sales


# In[86]:


get_ipython().run_cell_magic('bash', '', "head -10 'supermarket_sales - Sheet1.csv'\n")


# In[87]:


# Amazon Products Dataset 2023 (kaggle datasets)  
# https://www.kaggle.com/datasets/asaniczka/amazon-products-dataset-2023-1-4m-products?select=amazon_products.csv


# In[88]:


# Read a CSV file in a DataFrame:
the_file = os.path.join(data_rep, 'supermarket_sales - Sheet1.csv')
df = spark.read.csv(the_file, header=True, inferSchema=True)


# In[89]:


# Display schema of DataFrame
df.printSchema()

# Display the content of the DataFrame
df.show(10)


# In[90]:


# Select columns
# ==============

selected_columns = df.select("Invoice ID", "Unit price")
selected_columns.show(10)


# In[91]:


df.show(10)


# In[92]:


# Filter: apply conditions to filter rows
# =======================================

filtered_data = df.filter(df.Total > 400)
filtered_data.show(10)


# In[93]:


# Grouping and Aggregation
# ========================

# GroupBy

grouped_data = df.groupBy("Branch").agg({"Total": "sum", "Invoice ID": "count"})
grouped_data.show()


# In[94]:


# Join: combine multiple DataFrames based on a specific column

# create a 10 rows df1 based on df
df1 = df.select("Invoice ID", "Branch").limit(10)
df1.show()


# In[95]:


# create a 10 rows df1 based on df
df2 = df.select("Invoice ID", "City").limit(10)
df2.show()


# In[96]:


# Join the dataframes

joined_data = df1.join(df2, "Invoice ID", "inner")
joined_data.show()


# In[97]:


# Sort: arrange rows based on one or more columns
# ===============================================

# Sort by a column

# 10 smallest invoices by Total sales
sorted_data = df.orderBy("Total")
sorted_data.show(10)


# In[98]:


# Sort by a column desc
from pyspark.sql.functions import col, desc

# 10 biggest invoices
sorted_data = df.orderBy(col("Total").desc(), col("Invoice ID").desc())
sorted_data.show(10)


# In[99]:


# Distinct: get unique rows
# =========================

# Get distinct Product line

distinct_row = df.select("Product line").distinct()
distinct_row.show()



# In[100]:


# Drop: remove specified columns
# ==============================

dropped_columns = df.drop("Branch", "City", "Customer type", "Product line", "Gender", "Time", "gross margin percentage")
dropped_columns.show(10)


# In[101]:


# WithColumn: Add new calculated columns
# ======================================

df_with_new_column = dropped_columns.withColumn("ROE", 100*dropped_columns['gross income'] / dropped_columns['cogs'])
df_with_new_column.show(10)


# In[102]:


# Alias: rename columns for better readability
# ============================================

# Rename columns using alias

df_with_alias = df.withColumnRenamed("Invoice ID", "invoice_id")
df_with_alias.show(10)


# In[103]:


spark.stop()


# In[104]:


# Spark SQL and SQL Operations
# ============================

# Querying Structured and Semi-Structured Data

# What is Spark SQL ?

# - Spark SQL is a module in Apache Spark.
# - It enables querying of structured and semi-structured data using SQL commands
# - It extends Spark's capabilities to handle structured data effictively.


# Key feautres of Spark SQL

# - Unified Data Processing: Spark SQL provides a unified API for both batch and real-time data processing,
#                               simplifying end-to-end data pipeline development.
# - Schema inference: Automatically infers structured data sources' schema, 
#                       reducing the need for explicit schema definitions
# - Data Source Abstraction: Supports a wide range of data sources (Hive, Parquet, Avro, ORE, JSON, JDBC),
#                               enhancing versatility for working with various data formats.
# - Integration with Hive: Seamlessly integrates with Apache Hive, enabling Hive query execution and
#                           access to Hive UDFs within Spark SQL.


# In[105]:


# Perform SQL-like queries on Spark DataFrames
# ============================================

# First, initialise a Spark Session

# Set the PySpark environment variables
import os
os.environ['SPARK_HOME'] = '/Users/davidtbo/anaconda3/envs/pyspark-env/lib/python3.12/site-packages/pyspark'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'jupyter'
os.environ['PYSPARK_DRIVER_PYTHON_OPTS'] = 'notebook'
os.environ['PYSPARK_PYTHON'] = 'python'

# import the necessary modules
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrameSQL").getOrCreate()


# In[106]:


data_rep


# In[107]:


get_ipython().run_cell_magic('bash', '', "head -10 '/Users/davidtbo/Documents/Data_Science/99_Data/supermarket_sales - Sheet1.csv'\n")


# In[112]:


# Load a CSV file in a DataFrame:
the_file = os.path.join(data_rep, 'supermarket_sales - Sheet1.csv')
df = spark.read.csv(the_file, header=True, inferSchema=True)

# Simplify the dataframe dropping colummns
df = df.drop("Branch", "City", "Customer type", 
             "Product line", "Tax 5%", "Gender", 
             "Time", "payment",
             "gross margin percentage","rating")

# Renaming columns

from pyspark.sql import functions as F

# Replace the " " in the columns names by "_"
df = df.select([F.col(col).alias(col.replace(' ', '_')) for col in df.columns])

# Lowercase all column names
df = df.toDF(*[col.lower() for col in df.columns])


# In[113]:


# Display schema of DataFrame
df.printSchema()

# Display the content of the DataFrame
df.show(10)


# In[175]:


# Register the DataFrame as a Temporary Table
# ===========================================


# In[114]:


# To run SQL queries in  a DataFrame you need to register the df as a temporary table

df.createOrReplaceTempView("my_table")


# In[116]:


# Perform SQL-like Queries
# ========================

# select all rows where quantity is greater than 10

result = spark.sql("SELECT * FROM my_table WHERE quantity > 9")
result.show()


# In[117]:


# Creating and managing temporary views
# =====================================

# Create a temporary view
df.createOrReplaceTempView("my_table")


# In[119]:


# Check if view exists
view_result = spark.catalog.tableExists('my_table')
view_result


# In[120]:


# Drop a temporary view
spark.catalog.dropTempView('my_table')


# In[121]:


# Check if view exists
view_result = spark.catalog.tableExists('my_table')
view_result


# In[ ]:


# Using advanced SQL operations for data analysis
# ===============================================

# Subqueries
# ==========


# In[123]:


# Create DataFrames

employee_data = [
    (1, "John"), (2, "Alice"), (3, "Bob"), (4, "Emily"),
    (5, "David"), (6, "Sarah"), (7, "Michael"), (8, "Lisa"),
    (9, "William")
]
employees = spark.createDataFrame(employee_data,["id", "name"])


# In[139]:


salary_data = [
    ("HR", 1, 60000), ("HR", 2, 55000), ("HR", 3, 58000),
    ("IT",4,70000),("IT",5,72000),("IT",6,68000),
    ("Sales",7,75000),("Sales",8,78000),("Sales",9,77000)
]
salaries = spark.createDataFrame(salary_data, ["dpt","id","salary"])


# In[140]:


salaries.printSchema()


# In[141]:


employees.show()

employees.printSchema()


# In[142]:


salaries.show()


# In[143]:


# Register them as temporary views
employees.createOrReplaceTempView("employees")
salaries.createOrReplaceTempView("salaries")


# In[144]:


# Subquery to find employees with salaries above average
result = spark.sql("""
   SELECT name FROM employees 
   WHERE id IN (SELECT id FROM salaries 
                   WHERE salary > (SELECT AVG(salary) FROM salaries)
               )
                  """)


# In[145]:


result.show()


# In[156]:


# Window functions
# ================

from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Window function allows to make calculations across a window of rows
# defined by an ordered partition.
# Common window functions include:
# ROW_NUMBER(), RANK(), DENSSE_RANK(), SUM(), AVG()

# In this example, we will calculat the rank of employee within each dpt
# based on salary.

# First we join the table 'employee' and 'salaries'
# to have a dataframe called 'employee_salary'ArithmeticError

employee_salary = employees.join(salaries, "id", "inner")
employee_salary.show()


# In[157]:


# Using the SQL way:
employee_salary = spark.sql("""SELECT salaries.*, employees.name  
                            FROM salaries 
                            LEFT JOIN employees 
                            ON salaries.id = employees.id""")
employee_salary.show()


# In[158]:


# NB: salaries.* to select all the columns of salaries table
#       which we add the column employees.name from employees.


# In[159]:


# Create a window specification
window_spec = Window.partitionBy("dpt").orderBy(F.desc("salary"))


# In[160]:


# Calculate the rank of employees within each department based on salary
employee_salary.withColumn("rank", F.rank().over(window_spec))


# In[ ]:


# Stop the session

spark.stop()


# In[ ]:






# Python #

## Itroduction python ##

Python is a high-level, general-purpose programming language that is used for a wide variety of applications.

## OOps Concept ##

 Python is also an object-oriented language since its beginning. It allows us to develop applications using an Object-Oriented approach. In Python, we can easily create and use classes and objects.
 
 Major principles of object-oriented programming system are 

- Class
- Object
- Method
- Inheritance
- Polymorphism
- Data Abstraction
- Encapsulation

## Oparetor Precedence ##

Operator precedence is the order in which operators are evaluated in an expression. In Python, operator precedence is determined by a set of rules that specify which operators are evaluated first when an expression is evaluated. 

## Expressions VS Statements ##

An expression is any valid unit of code that can be evaluated to a value. This can include variables, constants, function calls, and arithmetic or logical operations. When an expression is evaluated, it produces a result. For example, 2 + 3 is an expression that evaluates to 5.

A statement, on the other hand, is a complete line of code that performs an action or has an effect. Statements can include assignments, loops, conditionals, function definitions, and more. A statement can contain one or more expressions, but it does not necessarily need to evaluate to a value. For example, an assignment statement x = 3 assigns the value 3 to the variable x.

## Type Coversion ##

Type conversion, also known as type casting, is the process of converting a value from one data type to another data type. In Python, there are several built-in functions that can be used to perform type conversion, including:

int,float,str,bool

## Big Data ##

Big data refers to extremely large and complex data sets that are beyond the ability of traditional data processing software and hardware to manage and analyze effectively.

## HDFS ##

HDFS (Hadoop Distributed File System) is a distributed file system that is designed to store and manage large data sets across multiple servers in a Hadoop cluster. It is a key component of the Apache Hadoop software framework for distributed storage and processing of big data.

## Yarn ##

YARN (Yet Another Resource Negotiator) is a distributed resource management system that is a key component of the Apache Hadoop ecosystem.

## Map Reduce ##

MapReduce is a programming model and processing framework for large-scale distributed data processing, which was popularized by Apache Hadoop. It is designed to process and analyze large amounts of data in a parallel and distributed manner across a large cluster of computers.

## Hive ##

Hive is an open-source data warehousing and SQL-like query language that is built on top of Hadoop Distributed File System (HDFS) and Apache Hadoop. 

# Spark

## Spark Introduction ##

Spark is an open-source distributed computing system designed to process large datasets across a cluster of computers. It was developed at the University of California, Berkeley's AMPLab and is now maintained by the Apache Software Foundation.

## Spark Features ##

- Fast Processing
- Flexibility
- Fault tolerance
- In-memory computing
- Real-time processing
- Better analytics

## Spark Architecture ##

- Spark Drive
- Cluster Manager
- Executors
- Spark Context
- Resilient Distributed Datasets (RDDs)

## Cluster and Configuration ##

 a cluster is a set of computers that work together to process data in parallel. The cluster is managed by a cluster manager, which coordinates the allocation of resources and scheduling of tasks across the nodes in the cluster
 
 ## RDD ##
 
 RDD stands for Resilient Distributed Dataset, which is a fundamental data structure in Spark. An RDD is an immutable, fault-tolerant collection of elements that can be processed in parallel across multiple nodes in a distributed computing environment.
 
 ## Data Frame ##
 
 A DataFrame is a distributed collection of data organized into named columns, similar to a table in a relational database. DataFrames are a higher-level abstraction built on top of RDDs (Resilient Distributed Datasets), which provide a more structured and optimized view of data.
 
 ## What is Dataset ##
 
 A Dataset is a distributed collection of data that provides the benefits of both RDDs and DataFrames in Spark. Like RDDs, Datasets are strongly typed and provide compile-time type safety, while also supporting rich functions and transformations. Like DataFrames, Datasets are optimized for efficient processing and provide a more structured and efficient view of data.





## PySpark Introduction ##

 PySpark is a Python API for Spark released by the Apache Spark community to support Python with Spark. Using PySpark, one can easily integrate and work with RDDs in Python programming language too.


##  Pyspark Concepts ##

PySpark is a Python library that provides an interface to Apache Spark, a distributed computing framework for processing large-scale data. PySpark enables users to write Spark applications using Python APIs, making it easier for Python developers to take advantage of Spark's capabilities.

Here are some key concepts in PySpark:

RDD (Resilient Distributed Datasets): RDD is the basic data structure in PySpark, which is an immutable distributed collection of objects. RDDs can be created from data stored in Hadoop Distributed File System (HDFS), or by transforming other RDDs. RDDs can be processed in parallel across a cluster of machines.

Transformations: Transformations are operations that are performed on RDDs to create a new RDD. Transformations are lazy, meaning they are not executed immediately, but only when an action is performed on the RDD.

Actions: Actions are operations that trigger the computation of RDDs and return a result or output. Examples of actions include count(), collect(), saveAsTextFile(), and reduce().

SparkContext: SparkContext is the entry point to any PySpark application. It is responsible for setting up the Spark application and coordinating the execution of tasks across a cluster of machines.

DataFrame: DataFrame is a distributed collection of data organized into named columns. It is similar to a table in a relational database or a data frame in R or Python. DataFrame APIs provide a more user-friendly way to manipulate data than RDD APIs.

Spark SQL: Spark SQL is a Spark module for structured data processing. It provides a programming interface to work with structured data using SQL or DataFrame APIs.

MLlib: MLlib is a Spark module for machine learning. It provides scalable implementations of common machine learning algorithms and tools for building machine learning pipelines.

Spark Streaming: Spark Streaming is a Spark module for processing real-time data streams. It provides an API for processing data streams in micro-batches, which enables scalable and fault-tolerant stream processing.

These are just a few of the key concepts in PySpark. Understanding these concepts is essential for building scalable and efficient Spark applications using PySpark.



 ## Need to learn about different read methods ##
 
 
In PySpark, there are various methods to read data from different data sources. Here are some of the most commonly used read methods:

textFile(): This method is used to read data from text files. It returns an RDD where each element in the RDD is a line in the text file.

wholeTextFiles(): This method is used to read data from multiple text files, where each file is represented as a key-value pair, with the file path as the key and the contents of the file as the value.

csv(): This method is used to read data from CSV files. It returns a DataFrame where each row in the DataFrame represents a row in the CSV file.

json(): This method is used to read data from JSON files. It returns a DataFrame where each row in the DataFrame represents a JSON object.

parquet(): This method is used to read data from Parquet files. Parquet is a columnar storage format that is optimized for performance. It returns a DataFrame where each row in the DataFrame represents a row in the Parquet file.

jdbc(): This method is used to read data from a JDBC source, such as a database. It returns a DataFrame that represents the result of the SQL query that is executed on the JDBC source.

table(): This method is used to read data from a table in a Spark catalog. It returns a DataFrame that represents the data in the table.

These are just a few examples of the different read methods available in PySpark. Each method has its own set of parameters and options, so it's important to read the PySpark documentation to understand how to use them effectively.

 ## Functions, case classes, traits ##
 
 In PySpark, there are several programming constructs that are commonly used, including functions, case classes, and traits. Here's a brief overview of each:

Functions: Functions in PySpark are similar to functions in other programming languages. They are reusable blocks of code that take inputs and return outputs. PySpark functions can be defined using the def keyword, and can be passed as arguments to other functions or used in higher-order functions. Functions are commonly used in PySpark for data processing, such as filtering, mapping, and aggregating data.

Case classes: Case classes in PySpark are similar to classes in other programming languages, but are designed to be used for immutable data structures. Case classes can be defined using the case class keyword, and can have fields with default values. They are commonly used in PySpark to define data structures that are used in RDDs and DataFrames.

Traits: Traits in PySpark are similar to interfaces in other programming languages. They define a set of methods that must be implemented by any class that implements the trait. Traits can be defined using the trait keyword, and can be mixed in with other classes using the with keyword. Traits are commonly used in PySpark for code reuse and to enforce a specific interface for a group of classes.

Overall, these programming constructs are key building blocks for developing PySpark applications that are efficient, maintainable, and scalable. Understanding how to use functions, case classes, and traits effectively can help you write clean and modular code that is easier to debug and maintain.


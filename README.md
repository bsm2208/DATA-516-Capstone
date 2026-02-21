# DATA-516-Capstone

## Capstone Project: Production-Ready Event Analytics Pipeline
Jennifer Kim
Data 512, Fall 2025

## Scenario: 
As a data engineer at an e-commerce company, my task is to build a production-grade analytics pipeline for e-commerce event data generated at a rate of 500K-750k every 5 minutes and a reliable analytical dataset that can be used for analysis and experiments. The data files are in gzipped JSON line format and use Hive-style partitioning for automatic Glue/Athena partition discovery.

The technical requirements are that I extend the CloudFormation template to add my pipeline infrastructure, handle incremental streams of data, and support the five required queries. The purpose of the queries is to help the business understand the conversion funnel, hourly revenue, top 10 products, category performance and user activity. 

## Overall plan:
1. Extend the CloudFormation template to include pipeline infrastructure
    -	Add one bucket dedicated to the transformed analytical dataset and one bucket for the CloudFormation template, ETL script and SQL queries 
    -	Create a Glue database, Athena workgroup and Glue crawler. A workgroup enables users to isolate queries, assign a specific query results location and see query-level metrics. The Glue crawler is included as an optional resource. I will be creating an Athena table pointing to the Parquet outputs using Data Definition Language(DDL) to explicitly define the partition structure and schema. 
    -	Set parameters for the Athena query results prefix and the Glue role name (LabRole) 	
    -	Finally, declare the dedicated two bucket names in the outputs section
      
2.	Using a custom ETL script, create and run a Glue job via the AWS console.
    -	Using built-in Spark functions under the hood, the Glue job will extract and transform gzipped JSON lines files to parquet format and write them to the dedicated bucket defined in the ETL script. 
    -	The bookmark function will be enabled to handle incremental data arriving every five minutes. With this function, Glue job will process new data without processing data that has already been processed. 
    -	Writing a proper ETL script is essential for a successful Glue Run. 
        o	Read with DynamicFrame- this is required for bookmarks
        o	Include the transformation_ctx parameter, which is used for indexing the bookmark key to search for the bookmark state. Without this parameter, the bookmarks are not enabled for the dynamic frame
        o	Convert to Spark DataFrame for transformations and convert back to Dynamic Frame before writing
        o	I used the configuration: Glue 4.0, worker type G.1X, two workers and max concurrency of 1. G.1X is a cost-efficient and scalable option for data transformations and queries.
     	
3.	After the Glue job finishes running, run MSCK REPAIR TABLE in Athena to look for new partitions and verify the new partitions have been added
   
5.	Run the five analytical queries


## Design Choices:
#### 1.	Transform raw data to Parquet format 
Parquet, which is a columnar storage format, is excellent for analytical queries since it enables compression by column and projection and predicate pushdown. Compression by column reduces disk space and I/O query processing because homogenous values compress better, and projection pushdown enables the query to read only selected columns. 

Predicate pushdown uses min and max values from data block predicates to skip row groups. As for alternatives, the JSON lines format (Bronze layer) can technically be kept since Athena allows JSON-data querying. However, as 500-750k events are being created every five minutes, it is an expensive and inefficient choice because rows are stored contiguously, and the entire rows must be scanned for query processing even when the query only requires a few columns. ORC (Optimized Row Columnar) could be used; it is often results in smaller files than Parquet format and its indexes can expedite processing. 

For my business objective, I believe Parquet’s efficient data compression and encoding make it highly suitable for processing fast-accumulating data. Parquet format and partitions enable fewer bytes to be scanned, which reduces costs for services that charge by data scanned, such as Athena. For compression, I will use Snappy because it enables high compression and decompression speed.

#### 2.	Partitioning 
The event files use Hive-style partitioning as follows: 

events/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/events-{timestamp}.jsonl.gz

The required queries do not need the data to be partitioned by minute. So, I will use year, month, day and hour as partition keys when defining the Glue Job. This involves deriving partitions using the Pyspark SQL functions year, month, dayofmonth and hour from the timestamp in the ETL script. 

#### 3.	Athena vs. Redshift
The EventBridge scheduler and Lambda are defined in the provided CloudFormation. I will be adding my pipeline resources to the template and run ad-hoc queries. Athena allows users to run interactive ad hoc queries directly on data in S3, so I believe Athena is a better choice for my project. Since Athena is serverless, users do not need to set up infrastructure. 

Additionally, the five SQL queries are not complex to the extent that they require joining data from multiple resources (which Redshift better supports). Besides, Athena will give the business peace of mind because it has a pay-per-query model, whereas Redshift employs a cluster-based pricing model which can add up costs depending on how long the cluster needs to be maintained. With that said, the business may wish to reevaluate its options if it needs to run frequent queries in the future (storage versus query volume). 

#### 4.	AWS Glue bookmark
As data will be flowing in every five minutes, I will use the Glue job bookmark feature to track data that has already been processed and prevent reprocessing. Another approach is to make it event-driven (S3 → EventBridge → Glue). The advantage is that the business can enjoy near-real time, decoupled architecture, which allows the rest of the services to continue to run even if one service experiences a failure. However, it involves more set up and may drive up costs. 

#### 5.	Silver layer vs. Gold layer
The silver layer is best for transforming raw JSON lines files into a clean, partitioned Parquet dataset optimized for production-grade analytics. The purpose of the analytical dataset is to gain insight into the conversion funnel, hourly revenue, top 10 products by view count, category performance and user activity. Since these queries do not require complex joins or business-ready aggregates, the silver layer is the most cost-efficient and appropriate option for this data pipeline.

## Pipeline
•	Raw data is read into the source events bucket by AWS Lambda, triggered by EventBridge (bronze layer)

•	AWS Glue job (serverless Spark) reads the data files and transform them to partitioned Parquet files

•	The transformed Parquet files are stored in the analytical S3 bucket (silver layer)

•	Athena queries the analytical bucket using the metadata from the Glue data catalog.

<img width="468" height="361" alt="image" src="https://github.com/user-attachments/assets/6c9f2e9b-f0f2-4d24-abe2-064697fc02d1" />
                
## Validation:
After deploying the updated CloudFormation template, I allowed EventBridge to run for approximately one hour to collect an hour worth of raw events data. Then I created and executed the Glue job, disabled EventBridge to prevent additional ingestion, ran MSCK REPAIR TABLE Athena to add the new partitions. In order to validate the old data was not reprocessed, I ran the command SELECT COUNT(*), which returned 11,993,605.

Next, I enabled EventBridge again for around 15 minutes and took the same step above to get a second batch of data. Then, I ran the validation query again, and it returned 13,956,360. Since this value is less than double the initial count, it confirms that the Glue job bookmark successfully processed the new files without reprocessing the already-processed old files. I also checked and saw that the new partitions corresponded to the hour in which second round of ingestion occurred. 

## Required Queries and Performance:
#### Query 1: Conversion Funnel
I ran the explain function directly in Athena. The following are screenshots of the relevant parts of the query plan show evidence of partition pruning and projection/predicate pushdown. 

<img width="175" height="114" alt="image" src="https://github.com/user-attachments/assets/85e5cb65-a6fb-44bc-9c43-03abdafacede" />


<img width="468" height="39" alt="image" src="https://github.com/user-attachments/assets/e6fe2332-453b-4418-8753-a7701b3b50f8" />

The actual query took 1.452 seconds and scanned 14.37MB.

<img width="251" height="265" alt="image" src="https://github.com/user-attachments/assets/66d93d35-7cad-47b0-abc2-38d30e7271b0" />

#### Query 2: Hourly Revenue
The following screenshots provide evidence of predicate/projection pushdown.

<img width="329" height="179" alt="image" src="https://github.com/user-attachments/assets/142b75df-3cfe-434b-96c7-a94b9af065a3" />


<img width="468" height="49" alt="image" src="https://github.com/user-attachments/assets/0bfa0be6-7175-4b0e-b965-ea2ba1235299" />

The actual query took 1.613 seconds and scanned 36.99MB.

<img width="313" height="60" alt="image" src="https://github.com/user-attachments/assets/532d3f69-9bb0-4193-af9e-f49054ddbc2c" />


#### Query 3 : Top 10 Products
Its query plan also shows evidence of projection/predicate pushdown as follows:

<img width="468" height="38" alt="image" src="https://github.com/user-attachments/assets/d3587704-7a99-4d23-b957-4dbcb55fb449" />

The query took 900ms and scanned 14.37MB.

<img width="293" height="247" alt="image" src="https://github.com/user-attachments/assets/6694d4c1-669a-4ebd-beda-a36dce98e1db" />


#### Query 4 : Category Performance
There appears to be no evidence of predicate pushdown. However, I can find evidence of projection pushdown and partition pruning.
  

<img width="256" height="140" alt="image" src="https://github.com/user-attachments/assets/19d5934c-f1a6-4bd4-b19c-749564843c62" />

<img width="463" height="31" alt="image" src="https://github.com/user-attachments/assets/eff0029a-d4b9-4e8e-b95f-e433383365cc" />

The query took 1.152 seconds and scanned 6.33MB

<img width="416" height="181" alt="image" src="https://github.com/user-attachments/assets/9c639c2c-b550-41cb-9336-e4a014d50eba" />

#### Query 5: User Activity
Its query plan also shows evidence of projection pushdown and partition pruning only. 

<img width="468" height="149" alt="image" src="https://github.com/user-attachments/assets/ec663a86-0d6a-4fc9-8a1c-7854785fc403" />

This query took 2.191 seconds and scanned 149.07MB.

<img width="468" height="45" alt="image" src="https://github.com/user-attachments/assets/53bc03dc-790c-4064-bebf-d62af20291ee" />


I believe queries 4 and 5 did not have predicate pushdown due to the absence of a where clause.
All five queries ran efficiently, under 3 seconds. I saw evidence of predicate and projection pushdown and partition pruning, which indicates that the Parquet format reduces scan size. In terms of scalability, while the queries performed well without errors, it would be prudent to continuously monitor performance and consider further approaches to achieve efficiency and reduce costs. 

## Reflection:
I tried my best to build a working analytics pipeline using the concepts I learned throughout the quarter, including extensively reviewing AWS documentation and previous assignments to refresh my memory and fill knowledge gaps. My priority was to build a reliable tool that meets all the basic requirements. As I continue to expand my knowledge, I hope to experiment with more sophisticated tools with greater confidence. This project was a nice culmination of all the concepts taught in class, as well as a hands-on opportunity to tackle a real business problem. 



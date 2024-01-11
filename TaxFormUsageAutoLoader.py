from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, LongType, TimestampType
from datetime import datetime
import json
from pyspark.sql.functions import col, current_timestamp, input_file_name, size
from pyspark.sql.functions import lit, count, trim, sum, avg, when
import configparser
from functools import reduce
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.dbutils import DBUtils
config = configparser.ConfigParser()
allcoloumnsnames = []
actualcoloumnsnames = []
try:
    env = dbutils.widgets.get("env")
    dbfs_config_path = 'dbfs:/FileStore/ConfigFiles/dev/{}_autoloader_config.ini'.format(env)
    dbutils = DBUtils(spark)
    config_data = dbutils.fs.head(dbfs_config_path)
    config.read_string(config_data)
    json_file_path = config.get('Paths', 'json_file_path')
    source_path = config.get('Paths', 'source_path')
    checkpoint_path = config.get('Paths', 'checkpoint_path')
    table_name = config.get('Table', 'table_name')
    error_table = config.get('Table', 'error_table')
    auditlogs_table = config.get('Table', 'auditlogs_table')

    with open(json_file_path, 'r') as schemaFile:
        schemaContent = schemaFile.read()
        schemaDict = json.loads(schemaContent)
        schemaColumns = schemaDict["properties"]

    def FieldType(fieldType):
        columnType = StringType()
        if fieldType == "integer":
            columnType = IntegerType()
        elif fieldType == "string":
            columnType = StringType()
        elif fieldType == "float":
            columnType = FloatType()
        elif fieldType == "Long":
            columnType = LongType()
        elif fieldType == "date":
            columnType = DateType()
        return columnType

    mandatecolumns = schemaDict.get("required")
    def getStructFieldArray(properties, parentcolumnname):
        structFieldArray = []
        for columnName, columnValue in properties.items():
            columnType = columnValue["type"]
            #print(columnType)
            if columnType == "object":
                mandatecolumns.extend(columnValue.get("required"))
                subStructFieldArray = getStructFieldArray(columnValue["properties"], columnName)
                structFieldArray.append(StructField(columnName, StructType(subStructFieldArray), True))
            else:
                structFieldArray.append(StructField(columnName, FieldType(columnType), True))
                allcoloumnsnames.append(columnName)
                actualcoloumnsnames.append(columnName if parentcolumnname == "" else parentcolumnname + '.' + columnName)
        return structFieldArray
    #print(schemaColumns)
    structFieldsArray = getStructFieldArray(schemaColumns, "")

    structType = StructType(structFieldsArray)
    schema = structType
    mandatecolumns = list(set(allcoloumnsnames).intersection(mandatecolumns))
    timestamp_filter = datetime.strptime("2023-11-17", "%Y-%m-%d")
    specificTime = timestamp_filter.strftime("%Y-%m-%dT%H:%M:%S")

    taxformsource_df = spark.readStream.format("cloudFiles") \
        .option("compression", "gzip")\
        .option("multiline", True)\
        .option("cloudFiles.format", "json")\
        .option("modifiedAfter", specificTime)\
        .option("checkpointLocation", checkpoint_path)\
        .schema(schema)\
        .load(source_path)
    
    #.option("checkpointLocation", checkpoint_path)
    url = spark.conf.get("spark.databricks.workspaceUrl")
    autoloader_pipeline = "https://{}/?o=344793466100907#notebook/131972473181943/command/3001802430091526".format(url)
    current_time = datetime.now()
    epoch_time = current_time.strftime("%Y%m%d%H%M%S") #int(current_time.timestamp())

    #print(actualcoloumnsnames)
    selectcolumns = [col(column_name).alias(column_name.split(".")[-1]) for column_name in actualcoloumnsnames]
    #print(selectcolumns)
    df = taxformsource_df.columns
    taxformtransformed_df = taxformsource_df.select(*selectcolumns,
        col("_metadata.file_path").alias("filepath"),
        col("_metadata.file_name").alias("filename"),
        col("_metadata.file_size").alias("filesize"),
        col("_metadata.file_modification_time").alias("filemodificationtime"),
        current_timestamp().alias("recordinsertdatetime"),
        lit(epoch_time).cast("long").alias("batchid") 
    )

    conditions = [col(column_name).isNull() | (trim(col(column_name)) == "") for column_name in mandatecolumns]
    row_condition = conditions[0]
    for condition in conditions[1:]:
        row_condition = row_condition | condition
    taxformtransformed_df = taxformtransformed_df.withColumn("has_null_values", when(row_condition, 1).otherwise(0))
    ExecutionStartTime = current_time

    def process_audit_log_batch(batch_df, epoch_id):
        errormessage = ''
        aggegated_rows = []
        jobstatus = "inprogress"
        values = (epoch_time, "TaxFormUsageAutoLoader", "taxraw", "Taxformsusage", jobstatus, str(current_time), autoloader_pipeline, 0)
        columns = "BatchID, ProcessName, SchemaName, ObjectName, ExecutionStatus, ExecutionStartTime, PipelineURL, UpdatedRecordsCount"
        query = "INSERT INTO {} ({}) VALUES {}".format(auditlogs_table, columns, values)
        spark.sql(query)
        try:
            filewise_counts = batch_df.groupBy("filename").agg(
                count("*").alias("recordcount"),
                sum("has_null_values").alias("rejectedrecordcount")
                ).orderBy("filename")
            goodrecords = batch_df.filter(batch_df.has_null_values == 0).drop("has_null_values")
            goodrecords.write.format("delta").mode("append").saveAsTable(table_name)
            badrecords = batch_df.filter(batch_df.has_null_values == 1).drop("has_null_values")
            badrecords.write.format("delta").mode("append").saveAsTable(error_table)
            aggegated_rows = filewise_counts.toJSON().collect()
            
            jobstatus = "success"
        except Exception as e:
            errormessage = str(e).replace('\'', '')
            jobstatus = "failed"
        
        fileNames = []
        rcount = 0
        rrcount = 0
        print(aggegated_rows)
        for each_row in aggegated_rows:
            eachrow = json.loads(each_row)
            rcount = rcount+eachrow["recordcount"]
            rrcount = rrcount+eachrow["rejectedrecordcount"]
            fileNames.append(eachrow["filename"])
            
        update_query = "update {} set ExecutionStatus='{}', ExecutionEndTime='{}',  FileName='{}', InsertedRecordsCount={}, RejectedRecordCount={}, \
        ErrorMessage='{}' where BatchID={}".format(auditlogs_table, jobstatus, str(datetime.now()), ' ', rcount, rrcount, errormessage, epoch_time)
        spark.sql(update_query)


    taxformtransformed_df.writeStream.trigger(once=True).foreachBatch(process_audit_log_batch).option("checkpointLocation", checkpoint_path + "/checkpoint_g_b_a_tables").start()
    

except Exception as ee:
    print("================")
    print(ee)
    print("exception::")
    
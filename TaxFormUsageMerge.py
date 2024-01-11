from pyspark.sql.functions import col, count, concat, cast, year, month, current_timestamp, lit
from datetime import datetime
from pyspark.sql.functions import current_timestamp

try :
    current_time = datetime.now()
    epoch_time = current_time.strftime("%Y%m%d%H%M%S")
    url = spark.conf.get("spark.databricks.workspaceUrl")
    TaxFormUsageMerge_pipeline = "https://{}/?o=344793466100907#notebook/2673524415506054/command/2673524415506055".format(url)
    ProcessName = "TaxFormUsageRawToSilver"
    ObjectName = "formusagefact"
    auditlogs_table = "taa_us_taxform_dev.taxformaudit.pipeline_audit"
    silver_table = "taa_us_taxform_dev.taxformlakehouse.formusagefact"
    temp_table = "taa_us_taxform_dev.taxformlakehouse.taxformusagetemp"
    delete_temp = "truncate table {}".format(temp_table)
    spark.sql(delete_temp)
    
    select_query = """select max(batchid) as batchid from taa_us_taxform_dev.taxformaudit.pipeline_audit 
    where ProcessName = '{}' and ObjectName = '{}' and ExecutionStatus = 'success' """.format(ProcessName, ObjectName)
    batchid = spark.sql(select_query).collect()
    batchid = [v["batchid"] for v in batchid][0]
    print("_________")
    print(batchid)
    print("-------------")
    where_condition = " where batchid > {}".format(batchid) if batchid else ""
    delta_query = """
            WITH delta_records AS (
            SELECT
                CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP(time)) AS INT), 
                CAST(EXTRACT(MONTH FROM TIMESTAMP(time)) AS INT)) AS FormUsedYearMonth,
                taxyear AS taxyear,
                product AS product,
                firmaccountnumber AS firmaccountnumber,
                taxsystem AS taxsystem,
                formnumber AS formnumber,
                MAX(batchid) AS max_batchid
            FROM taa_us_taxform_dev.taxformraw.taxformusage {}
            GROUP BY
                FormUsedYearMonth,
                taxyear,
                product,
                firmaccountnumber,
                taxsystem,
                formnumber
        )
            SELECT * FROM delta_records
        """.format(where_condition)
    df = spark.sql(delta_query)
    #df.show()
    delta_records_df = df.select(
        col("FormUsedYearMonth").cast("int"),col("taxyear"),col("product"),col("firmaccountnumber"),col("taxsystem"),col("formnumber").cast("Long"))

    if delta_records_df.count() > 0:
        delta_records_df.write.format("delta").mode("append").saveAsTable(temp_table)
        latest_max_batchid = df.select(col("max_batchid")).distinct().collect()
        latest_max_batchid = [v["max_batchid"] for v in latest_max_batchid][0]
        print(latest_max_batchid)
        jobstatus = "inprogress"
        #values = (latest_batchid, ProcessName, "taxformlakehouse", ObjectName, jobstatus)
        values = (latest_max_batchid, ProcessName, "taxformlakehouse", ObjectName, jobstatus, str(current_time), ' ', TaxFormUsageMerge_pipeline, ' ', 0, 0)
        columns = "BatchID, ProcessName, SchemaName, ObjectName, ExecutionStatus, ExecutionStartTime, ErrorMessage, PipelineURL, FileName, InsertedRecordsCount, RejectedRecordCount"
        query = "INSERT INTO {} ({}) VALUES {}".format(auditlogs_table, columns, values)
        spark.sql(query)
        raw_temp_silver = """
                SELECT 
                    CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP(tu.time)) AS INT), CAST(EXTRACT(MONTH FROM TIMESTAMP(tu.time)) AS INT)) AS FormUsedYearMonth, 
                    tu.taxyear, tu.product, tu.firmaccountnumber, tu.taxsystem, tu.formnumber,
                    COUNT(*) AS FormUsageCount,
                    MAX(tu.BatchID) AS max_batchid
                FROM 
                    taa_us_taxform_dev.taxformraw.taxformusage AS tu
                INNER JOIN 
                    taa_us_taxform_dev.taxformlakehouse.taxformusagetemp AS ts
                ON  
                    CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP(tu.time)) AS INT), CAST(EXTRACT(MONTH FROM TIMESTAMP(tu.time)) AS INT)) = ts.FormUsedYearMonth and
                    tu.taxyear = ts.taxyear and
                    tu.product = ts.product and
                    tu.firmaccountnumber = ts.firmaccountnumber and
                    tu.taxsystem = ts.taxsystem and
                    tu.formnumber = ts.formnumber
                GROUP BY 
                    CONCAT(CAST(EXTRACT(YEAR FROM TIMESTAMP(tu.time)) AS INT), CAST(EXTRACT(MONTH FROM TIMESTAMP(tu.time)) AS INT)), 
                    tu.taxyear, tu.product, tu.firmaccountnumber, tu.taxsystem, tu.formnumber

            """

        raw_temp_silver_df = spark.sql(raw_temp_silver)
        #raw_temp_silver_df.show()
        raw_temp_silver_df.createOrReplaceTempView("raw_temp_silver_temp_view")
    
        merge_query = f"""
                MERGE INTO taa_us_taxform_dev.taxformlakehouse.formusagefact AS TARGET
                USING raw_temp_silver_temp_view AS SOURCE
                ON 
                    TARGET.FormUsedYearMonth = SOURCE.FormUsedYearMonth AND
                    TARGET.TaxYear = SOURCE.taxyear AND
                    TARGET.Product = SOURCE.product AND
                    TARGET.FirmAccountNumber = SOURCE.firmaccountnumber AND
                    TARGET.TaxSystem = SOURCE.taxsystem AND
                    TARGET.FormNumber = SOURCE.formnumber
                WHEN MATCHED THEN 
                UPDATE SET
                    TARGET.BatchID = SOURCE.max_batchid,
                    TARGET.FormUsageCount = SOURCE.FormUsageCount,
                    TARGET.RecordUpdateDateTime = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN 
                INSERT (
                    FormUsedYearMonth,
                    TaxYear,
                    Product,
                    FirmAccountNumber,
                    TaxSystem,
                    FormNumber,
                    FormUsageCount,
                    BatchID,
                    RecordInsertDateTime 
                )
                VALUES (
                    SOURCE.FormUsedYearMonth,
                    SOURCE.taxyear,
                    SOURCE.product,
                    SOURCE.firmaccountnumber,
                    SOURCE.taxsystem,
                    SOURCE.formnumber,
                    SOURCE.FormUsageCount,
                    SOURCE.max_batchid,  
                    CURRENT_TIMESTAMP()
                )
        """

        merge_result_df = spark.sql(merge_query)
        inserted_count = merge_result_df.select("num_inserted_rows").collect()[0][0]
        updated_count = merge_result_df.select("num_updated_rows").collect()[0][0]

        print(f"Inserted records count: {inserted_count}")
        print(f"Updated records count: {updated_count}")
        jobstatuss = "success"
        execution_end_time = current_time.strftime("%Y-%m-%d %H:%M:%S")
        scolumns = "ExecutionStatus, ExecutionEndTime, InsertedRecordsCount, UpdatedRecordsCount"
        squery = """UPDATE {table} SET ExecutionStatus = '{status}', ExecutionEndTime = '{end_time}', InsertedRecordsCount = {ins_count}, UpdatedRecordsCount = {upd_count} WHERE BatchID = {batch_id} AND ProcessName = "TaxFormUsageRawToSilver"
                    """.format(table=auditlogs_table, status=jobstatuss, end_time=execution_end_time, ins_count=(inserted_count), upd_count=(updated_count), batch_id=latest_max_batchid)
        spark.sql(squery)
    else:
        jobstatus = "success"
        values = (epoch_time, ProcessName, "taxformlakehouse", ObjectName, jobstatus, str(current_time), str(current_time),"No Batchs processed", TaxFormUsageMerge_pipeline, " ", 0, 0, 0)
        columns = "BatchID, ProcessName, SchemaName, ObjectName, ExecutionStatus, ExecutionStartTime, ExecutionEndTime, ErrorMessage, PipelineURL, FileName, InsertedRecordsCount, UpdatedRecordsCount, RejectedRecordCount"
        query = "INSERT INTO {} ({}) VALUES {}".format(auditlogs_table, columns, values)
        spark.sql(query)
except Exception as ee:
    print(str(ee))
    ErrorMessage = (str(ee))
    ErrorMessage = ErrorMessage.replace("'", "''")
    ejobstatus = "failed"
    ObjectName = "formusagefact"
    error_columns = "ErrorMessage"
    equery = """UPDATE {table} SET ExecutionStatus = '{status}', ErrorMessage = '{error}' WHERE BatchID = {batch_id} and ObjectName = '{object_name}'""".format(table=auditlogs_table, status=ejobstatus, error=ErrorMessage, batch_id=latest_max_batchid, object_name=ObjectName)
    spark.sql(equery)
    
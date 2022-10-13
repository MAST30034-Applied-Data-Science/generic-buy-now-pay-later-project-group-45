
################## Run the script in following command in terminal #########################


# python3 path/of/ranking.py —-path path/to/curated/data/after/preprocessing --output path/to/directory/where/rank/is
# can ignore inputs of --path and --output, but directory needs to be in correct order


# create modeling spark session
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName('Project 2')
    .config('spark.sql.repl.eagerEval.enabled', True)
    .config('spark.sql.parquet.cacheMetadata', 'true')
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .getOrCreate()
)
import pandas as pd
from pyspark.sql import functions as F
import pathlib
import sys
import re



# read command line input
directory = []
for i in sys.argv:
    if re.match(r"[\/\w+]+", i):
       directory.append(i)

data_directory = directory[1]
output_directory = directory[1]+"/rank"
selected_merchant_directory = directory[1]+"/selected"

date = str(sys.argv[-1])


# create directory of rank folder, containing all ranks of merchants
pathlib.Path(output_directory).mkdir(parents=True, exist_ok=True)
pathlib.Path(selected_merchant_directory).mkdir(parents=True, exist_ok=True)


# read curated data
data = spark.read.parquet(data_directory+'/final_dataset')


# calculate most recent annual transaction amount

clean_transaction = spark.read.parquet(data_directory+'/clean_full_dataset')

# most recent year starts at 2021.08.28
annual_transaction_count = clean_transaction.filter(F.col("order_datetime")>date)
annual_merchant_transaction = annual_transaction_count.groupby("merchant_abn").count().select("merchant_abn", "count")
full_data = data.join(annual_merchant_transaction, on="merchant_abn", how="left")

# calculate proportion of potential afterpay users
full_data = full_data.withColumn("ap_rate", (F.col("consumer_scaled_spare_money")*0.1+F.col("ap_percentage_by_gender").cast("float")))


# Allocate business segments for the all merchants (Recreation, electronics, fashion and household)
busi_area_type = [
    "fashion", "fashion", "electronics", "recreation",
    "recreation", "household", "recreation", "household",
    "recreation", "household", "electronics", "household",
    "recreation", "household", "household", "electronics",
    "fashion", "fashion", "recreation", "electronics",
    "recreation", "recreation", "household", "household", "household"]

busi_area= full_data.groupby('business_area').count().select("business_area").toPandas()['business_area'].to_list()


allocation_sdf=spark.createDataFrame(pd.DataFrame(list(zip(busi_area, busi_area_type)),
               columns =['business_area', 'business_segment']))

# merge segment dataset
full_data = full_data.join(allocation_sdf, on = "business_area")


# calculate expected transaction counts for next year
full_data = full_data.withColumn("expected_transaction", (F.col("count")*
                                 (1+F.col("annual_turnover_percentage")/100)).cast("int"))
                                 
# there are ap_rate of less than 0, replace this rate by 0.001
full_data = full_data.withColumn("ap_rate", F.when(full_data["ap_rate"] < 0, 0.001).otherwise(full_data["ap_rate"]))


# potential afterpay user = transaction*1.077*aprate
# calculate expected revenue of merchant with fraud data
full_data = full_data.withColumn("expected_revenue", \
                        ((F.col("expected_transaction")*1.077*F.col("ap_rate")).cast("int")\
                        *F.col("avg_total_value"))*F.col("take_rate")/100)

# select only useful columns of dataset
ranking = full_data.select("merchant_abn", "name", "business_area", "expected_revenue", "avg_total_value",\
                           "fraud_rate", "ap_rate", "revenue_level", "business_segment", "expected_transaction")

# calculate true predicted revenue of merchants by revenue * true percentage of revenue
ranking = ranking.withColumn("true_revenue", (F.col("expected_revenue")*(1-F.col("fraud_rate"))))\
                .orderBy(F.col("true_revenue").desc())

# remove useless columns
ranking = ranking.drop("expected_revenue", "fraud_rate", "ap_rate")

# save ranking of all merchants
ranking.write.mode("overwrite").parquet(output_directory+"/ranking/")
all_selected = ranking.select("name", "merchant_abn").limit(100)
all_selected.write.mode("overwrite").parquet(selected_merchant_directory+"/top_100_selected/")

# save ranking of household segment
household_rank = ranking.filter(F.col("business_segment")=="household").orderBy(F.col("true_revenue").desc())
household_selected = household_rank.select("name", "merchant_abn").limit(10)

household_rank.write.mode("overwrite").parquet(output_directory+"/household_rank/")
household_selected.write.mode("overwrite").parquet(selected_merchant_directory+"/selected_household_merchant/")

# save ranking of recreation segment
recreation_rank = ranking.filter(F.col("business_segment")=="recreation").orderBy(F.col("true_revenue").desc())
recreation_selected = recreation_rank.select("name", "merchant_abn").limit(10)

recreation_rank.write.mode("overwrite").parquet(output_directory+"/recreation_rank/")
recreation_selected.write.mode("overwrite").parquet(selected_merchant_directory+"/selected_recreation_merchant/")

# save ranking of electronic segment
electronic_rank = ranking.filter(F.col("business_segment")=="electronics").orderBy(F.col("true_revenue").desc())
electronic_selected = electronic_rank.select("name", "merchant_abn").limit(10)

electronic_rank.write.mode("overwrite").parquet(output_directory+"/electronic_rank/")
recreation_selected.write.mode("overwrite").parquet(selected_merchant_directory+"/selected_electronic_merchant/")

# save ranking of fashion segment
fashion_rank = ranking.filter(F.col("business_segment")=="fashion").orderBy(F.col("true_revenue").desc())
fashion_selected = fashion_rank.select("name", "merchant_abn").limit(10)

fashion_rank.write.mode("overwrite").parquet(output_directory+"/fashion_rank/")
fashion_selected.write.mode("overwrite").parquet(selected_merchant_directory+"/selected_fashion_merchant/")

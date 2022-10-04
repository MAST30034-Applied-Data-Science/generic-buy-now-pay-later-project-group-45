import sys
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import lpad
import os
import re
import urllib.request
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from sklearn import preprocessing
import numpy as np


# read command line input, and store working directories for data folder and export folder
directory = []
for i in sys.argv:
    if re.match(r"[\/\w+]+", i):
       directory.append(i)

data_directory = directory[1]
output_directory = directory[2]

#
file_name = os.listdir(data_directory)


# create preprocess spark session
spark = (
    SparkSession.builder.appName('Project 2 proprocess')
    .config('spark.sql.repl.eagerEval.enabled', True)
    .config('spark.sql.parquet.cacheMetadata', 'true')
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .getOrCreate()
)


################################ read merchants data ###############################

merchants = spark.read.parquet(data_directory+"/tbl_merchants.parquet")

# In the coloumn "tags", tags, renueve level and "take_rate" are seperated by either "), (" or "], [", replace these by ### for splitting
merchants = merchants.withColumn("tags",F.regexp_replace(F.regexp_replace(F.col("tags"),"\){1},{1}\s?\(","###"),"\]{1},{1}\s?\[","###"))

# split tags data to three columns, denoted as business_area, revenue_level and take_rate
split_merchants = merchants.withColumn("business_area", F.split(F.col("tags"), "###").getItem(0))\
                           .withColumn("revenue_level", F.split(F.col("tags"), "###").getItem(1))\
                           .withColumn("take_rate", F.split(F.col("tags"), "###").getItem(2))


# remove remaining ()[] punctuation in column business_area and take_rate
# obtain take rate in numeric form from take_rate column,
# in the form of take rate: d.dd, therefore item 1 is the numeric value
# convert string in business_area to lower form
# convert string in revenue_level to lower form, just in case if there's a typo

curated_merchant = split_merchants\
            .withColumn("business_area", F.regexp_replace("business_area", "[\[\]\(\)]", ""))\
            .withColumn("take_rate", F.regexp_replace("take_rate", "[\[\]\(\)]", ""))\
            .withColumn("take_rate", F.split(F.col("take_rate"), ":").getItem(1))\
            .withColumn("business_area", F.lower(F.col('business_area')))\
            .withColumn("revenue_level", F.lower(F.col('revenue_level')))


final_merchant = curated_merchant.drop('tags')

# The concepts are indeed the same but with some extra spaces. so remove extra spaces in "business_area"
final_merchant = final_merchant.withColumn("business_area", F.regexp_replace("business_area", "\s+", " "))



############################## read consumer dataset ##############################

consumer = spark.read.parquet(data_directory+"/consumer_user_details.parquet")
consumer_detail = spark.read.option('sep', "|").csv(data_directory+"/tbl_consumer.csv",header = True)
consumer = consumer.join(consumer_detail,on="consumer_id")


############################## get all transaction data file name ##############################

transaction_name_pattern = re.compile("transactions_[0-9]{8}_[0-9]{8}_snapshot")
# create a name list of transaction data


transaction_name = []
transaction_time = []
# record names that match transaction name pattern
for name in file_name:
    if (transaction_name_pattern.match(name)):
        transaction_name.append(name)
        date = name.split("_")
        transaction_time.append(date[1])
        transaction_time.append(date[2])



################## preprocess steps for transaction data #####################

# read a dataset first for future joining dataset
transactions = spark.read.parquet(data_directory+"/"+transaction_name[0])

# get start and end date of this patch of data, and filter the dataset to obtain transaction within this period
start_date = transaction_time[0][:4]+"-"+transaction_time[0][4:6]+"-"+transaction_time[0][6:]
end_date = transaction_time[1][:4]+"-"+transaction_time[1][4:6]+"-"+transaction_time[1][6:]
transactions = transactions.filter(F.col("order_datetime")<end_date)
transactions = transactions.filter(F.col("order_datetime")>start_date)


for i in range(1, len(transaction_name)):
    # read new 6 months data
    data = spark.read.parquet(data_directory+"/"+transaction_name[i])
    
    # get the start and end date of the 6 month transaction
    start_date = transaction_time[i*2][:4]+"-"+transaction_time[i*2][4:6]+"-"+transaction_time[i*2][6:]
    end_date = transaction_time[i*2+1][:4]+"-"+transaction_time[i*2+1][4:6]+"-"+transaction_time[i*2+1][6:]
    
    # filter the 6 month dataset so transactions are within the date boundary
    data = data.filter(F.col("order_datetime")<end_date)
    data = data.filter(F.col("order_datetime")>start_date)
    
    # union the monthly transaction
    transactions = transactions.union(data)


################## Download census data #####################

ABS_INCOME_URL = 'https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_POA_for_AUS_short-header.zip'
ABS_INCOME_URL_STATE = 'https://www.abs.gov.au/census/find-census-data/datapacks/download/2021_GCP_STE_for_AUS_short-header.zip'
ABS_DATA_Dir = data_directory+"/censusData"

# read the zip file, unzip the folder and export files in censusData folder
with urllib.request.urlopen(ABS_INCOME_URL) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(ABS_DATA_Dir)
# read the zip file, unzip the folder and export files in censusData folder
with urllib.request.urlopen(ABS_INCOME_URL_STATE) as zipresp:
    with ZipFile(BytesIO(zipresp.read())) as zfile:
        zfile.extractall(ABS_DATA_Dir)


################## read census data #####################
census = spark.read.csv(data_directory+"/censusData/2021Census_G02_AUST_POA.csv", header = True)

census_state = spark.read.csv(data_directory+"/censusData/2021 Census GCP States and Territories for AUS/2021Census_G02_AUST_STE.csv", header = True)
# remove POA prefix to obtain only numeric postcodes
census = census.withColumn("postcode", F.regexp_replace("POA_CODE_2021", "POA", ""))


################## read fraud dataset ####################################
merchant_fraud = spark.read.csv(data_directory+"/merchant_fraud_probability.csv", header = True)

# rename fraud_probability so it's distinct from consumer's
merchant_fraud = merchant_fraud.withColumnRenamed("fraud_probability","fraud_prob_merch")

consumer_fraud = spark.read.csv(data_directory+"/consumer_fraud_probability.csv", header = True)
consumer_fraud = consumer_fraud.withColumnRenamed("fraud_probability","fraud_prob_cons")


################## merge all dataframe ####################################

# consumer name and adddress is no longer useful
consumer = consumer.drop("name","address")

# join consumer and merchant data with transaction
consumer_transaction= transactions.join(consumer, on = "user_id",how="inner")

# joining merchants on merchant_abn from merchant data will remove merchants that are not listed but appears in transaction
transaction_merch_cons = consumer_transaction.join(final_merchant,on = ["merchant_abn"])

data_with_fraud = transaction_merch_cons.join(consumer_fraud, on = ["user_id","order_datetime"], how = "left")
data_with_fraud = data_with_fraud.join(merchant_fraud, on = ["merchant_abn","order_datetime"], how = "left")

# null values are created when joining fraud dataset, fill the null values with 0.001
full_dataset_filna = data_with_fraud.withColumn("fraud_prob_cons", F.col("fraud_prob_cons").cast("float")).fillna(0.001)
full_dataset_filna = full_dataset_filna.withColumn("fraud_prob_merch", F.col("fraud_prob_merch").cast("float")).fillna(0.001)


# there are postcodes that has only three digits
# adding lead zeros for post code

data_remove_fraud = full_dataset_filna.withColumn('postcode',lpad(full_dataset_filna['postcode'],4,'0'))

full_dataset = data_remove_fraud.join(census, on = ["postcode"], how = "left")

full_dataset = full_dataset.withColumn('postcode',lpad(full_dataset['postcode'],4,'0'))

full_dataset = full_dataset.drop("POA_CODE_2021")

dataset_with_null = full_dataset.filter(F.col("Median_tot_prsnl_inc_weekly").isNull()).groupby("postcode").count()

full_dataset = full_dataset.filter(F.col("Median_tot_prsnl_inc_weekly").isNotNull())



# for the unseen postcodes in the transactions, it will be allocated with average income of that state
dataset_with_null = dataset_with_null.withColumn(
    'STE_CODE_2021',
    F.when(((F.col("postcode") >= 1000) & (F.col("postcode") <= 2599 )) | ((F.col("postcode") >= 2619) & (F.col("postcode") <= 2898 )) | ((F.col("postcode") >= 2921) & (F.col("postcode") <= 2999 )), 1)\

    .when(((F.col("postcode") >= '0200') & (F.col("postcode") <= '0299' )) | ((F.col("postcode") >= 2600) & (F.col("postcode") <= 2618 )) | ((F.col("postcode") >= 2900) & (F.col("postcode") <= 2920 )), 8)\

    .when(((F.col("postcode") >= 3000) & (F.col("postcode") <= 3999 )) | ((F.col("postcode") >= 8000) & (F.col("postcode") <= 8999 )),2)\

    .when(((F.col("postcode") >= 4000) & (F.col("postcode") <= 4999 )) | ((F.col("postcode") >= 9000) & (F.col("postcode") <= 9999 )),3)\

    .when(((F.col("postcode") >= 5000) & (F.col("postcode") <= 5999  )),4)\

    .when(((F.col("postcode") >= 6000) & (F.col("postcode") <= 6797 )) | ((F.col("postcode") >= 6800) & (F.col("postcode") <= 6999 )),5)\

    .when(((F.col("postcode") >= 7000) & (F.col("postcode") <= 8000 )), 6)\
    
    .otherwise(7)
)


dataset_with_null = dataset_with_null.join(census_state, ['STE_CODE_2021']).drop("STE_CODE_2021","count")

dataset_with_null = data_remove_fraud.join(dataset_with_null, on = ["postcode"], how = "right")

full_dataset = full_dataset.union(dataset_with_null)

full_dataset = full_dataset.drop("order_id")


# remove some of the merchants that have little transaction and sales amount


count_sdf = full_dataset.groupBy(F.col('merchant_abn')).count()
count_df = count_sdf.toPandas()
count = count_df['count']
least_freq_merchants = count_df[count_df['count'] < 10]


amount = full_dataset.groupBy(F.col('merchant_abn')).sum('dollar_value')
amount_df = amount.toPandas()
least_amount_merchants = amount_df[amount_df['sum(dollar_value)'] < 50000]


least_merchants = pd.merge(least_freq_merchants, least_amount_merchants)
removed_merchants = least_merchants["merchant_abn"].to_list()


clean_full = full_dataset.filter(~full_dataset.merchant_abn.isin(removed_merchants))


############ allocate business area type for merchants ################
# the mapping is based on common knowledge and references from industry information published by Australian Government.
#  https://business.gov.au/planning/industry-information

busi_area = clean_full.groupby('business_area').count().sort("business_area").select("business_area").toPandas()['business_area'].to_list()

busi_area_type = ["Retail trade", "Arts and recreation services", "Retail trade", "Retail trade", "Retail trade", "Retail trade", "Professional, scientific and technical services", "Retail trade",
"Retail trade", "Retail trade", "Retail trade", "Manufacturing", "Retail trade", "Other services", "Retail trade", "Retail trade", "Retail trade", "Retail trade",
"Retail trade", "Manufacturing", "Retail trade", "Retail trade", "Information media and telecommunications", "Retail trade", "Other services"]

allocation_df = pd.DataFrame(list(zip(busi_area, busi_area_type)),
               columns =['business_area', 'business_area_type'])

######################## read turnover data ############################

turnover = pd.read_csv(data_directory+"/turnover/Business turnover indicator, change in turnover, seasonally adjusted.csv", header=1)
turnover = turnover[["Unnamed: 0","July 2021 to July 2022 (%)"]]
turnover = turnover.rename(columns={"Unnamed: 0": "business_area_type", "July 2021 to July 2022 (%)": "annual_turnover_percentage"})
allocation_df = allocation_df.merge(turnover, on="business_area_type", how="left")


# convert pandas to pyspark dataframe to merge with clean full dataset
allocation_sdf=spark.createDataFrame(allocation_df)
# add the business_area_type into clean_full_dataset
clean_full_dataset = clean_full.join(allocation_sdf, clean_full.business_area == allocation_sdf.business_area).drop(allocation_sdf.business_area)

# save dataset
clean_full_dataset.write.mode("overwrite").parquet(output_directory+"/clean_full_dataset/")




#########################  calculate gender percentage ####################################

# count the number of comsumers by gender and merchant
gender_count_sdf = clean_full_dataset.groupBy(["merchant_abn", "gender"]).count().sort("merchant_abn")

# count the total number of consumers for each merchant
total_count = gender_count_sdf.groupBy('merchant_abn').sum('count').sort("merchant_abn")

gender_count_sdf = gender_count_sdf.join(total_count, on='merchant_abn').sort("merchant_abn")
gender_count_sdf = gender_count_sdf.withColumnRenamed("sum(count)","total_transactions_count")

# calculate the consumer gender percentage for each gender and for each merchants, save the percentage as "gender_percentage"
gender_count_sdf = gender_count_sdf.withColumn("gender_percentage", F.col("count")/F.col("total_transactions_count"))


# separate gender percentage by male, female and undisclosed
male_percentage = gender_count_sdf.filter("gender == 'Male'").select(F.col("merchant_abn"),F.col("gender_percentage")).withColumnRenamed("gender_percentage","male_consumer_percentage")
female_percentage = gender_count_sdf.filter("gender == 'Female'").select(F.col("merchant_abn"),F.col("gender_percentage")).withColumnRenamed("gender_percentage","female_consumer_percentage")
undisclosed_percentage = gender_count_sdf.filter("gender == 'Undisclosed'").select(F.col("merchant_abn"),F.col("gender_percentage")).withColumnRenamed("gender_percentage","undisclosed_consumer_percentage")


# combine the 3 gender percentages together into 1 dataframe
agg_df = male_percentage.join(female_percentage, on="merchant_abn")
agg_df = agg_df.join(undisclosed_percentage, on="merchant_abn")
# add the total transaction count into aggregated dataframe and rename the column name
agg_df = agg_df.join(total_count, on="merchant_abn")
agg_df = agg_df.withColumnRenamed("sum(count)","total_transactions_count")



######################### calculate the average of census data ####################################
temp = clean_full_dataset.groupBy("merchant_abn") \
    .agg(F.mean("Median_age_persons").alias("avg_comsumer_age"), \
         F.mean("Median_tot_prsnl_inc_weekly").alias("avg_consumer_weekly_income"), \
         F.mean("Median_rent_weekly").alias("avg_comsumer_weekly_rent"), \
         F.mean("dollar_value").alias("avg_total_value"),\
     )

agg_df = agg_df.join(temp, on="merchant_abn")
temp2 = clean_full_dataset.select("merchant_abn","name", "business_area", "revenue_level", "take_rate", "business_area_type")
agg_df = agg_df.join(temp2, on="merchant_abn")
agg_df = agg_df.distinct()
agg_df = agg_df.drop("avg_comsumer_age")

agg_df = agg_df.withColumn('avg_consumer_weekly_spare_money', F.col('avg_consumer_weekly_income') - F.col('avg_comsumer_weekly_rent'))
# remove income and rent
agg_df = agg_df.drop('avg_consumer_weekly_income')
agg_df = agg_df.drop('avg_comsumer_weekly_rent')

spare_money_list = agg_df.select('avg_consumer_weekly_spare_money').rdd.flatMap(lambda x: x).collect()


# normal standardise the spare money and reverse the signs
scaled = preprocessing.scale(spare_money_list) * -1
scaled = scaled.tolist()
# convert to pandas and append list as new column
agg_df_pandas = agg_df.toPandas()
agg_df_pandas['consumer_scaled_spare_money'] = scaled


# convert back to pyspark
agg_df = spark.createDataFrame(agg_df_pandas)
agg_df = agg_df.drop("avg_consumer_weekly_spare_money")


# bnpl user proportion obtained from https://www.roymorgan.com/findings/women-more-likely-to-use-buy-now-pay-later-services
male_ap_percentage = 0.055
female_ap_percentage = 0.116
undisclosed_ap_percentage = np.mean([male_ap_percentage, female_ap_percentage])

# calculate proportion of afterpay users in a merchant
gender = agg_df.select("male_consumer_percentage", "female_consumer_percentage","undisclosed_consumer_percentage")
gender = gender.withColumn("ap_percentage_by_gender", F.col("male_consumer_percentage")*male_ap_percentage + F.col("female_consumer_percentage")*female_ap_percentage + F.col("undisclosed_consumer_percentage")*undisclosed_ap_percentage)


# merge the proportion dataframe
agg_df = agg_df.join(gender, on=["male_consumer_percentage", "female_consumer_percentage","undisclosed_consumer_percentage"]).drop("male_consumer_percentage", "female_consumer_percentage", "undisclosed_consumer_percentage")

turnover_df=spark.createDataFrame(turnover)
agg_df = agg_df.join(turnover_df, on="business_area_type", how="left")

################## add fraud rate of each merchant #################
# select only fraud column
full_fraud_data = clean_full_dataset.select("merchant_abn", "fraud_prob_cons", "fraud_prob_merch")
full_fraud_data = full_fraud_data.withColumn("fraud", F.when((F.col("fraud_prob_cons") > 0.0011) | (F.col("fraud_prob_merch") > 0.0011),1).otherwise(0))
fraud = full_fraud_data.select("merchant_abn", "fraud")

fraud_count = fraud.filter(F.col("fraud")==1).groupby("merchant_abn", "fraud").count().orderBy("merchant_abn")
fraud_count = fraud_count.select("merchant_abn", "count")
total_merchant_transaction = fraud.groupby("merchant_abn").count().orderBy("merchant_abn")
total_merchant_transaction = total_merchant_transaction.withColumnRenamed("count","total_transaction")

fraud = fraud_count.join(total_merchant_transaction, on="merchant_abn").orderBy("merchant_abn")
fraud = fraud.withColumn("fraud_rate", F.col("count")/F.col("total_transaction"))
fraud = fraud.drop("count", "total_transaction")

agg_df = agg_df.join(fraud, on="merchant_abn")

agg_df = agg_df.drop("fraud_prob_cons", "fraud_prob_merch", ).distinct()



# save dataset
agg_df.write.mode("overwrite").parquet(output_directory+"/final_dataset/")

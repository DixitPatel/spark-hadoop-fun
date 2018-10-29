# Get highest averagely rated item for each category in a reviews dataset from mongodb
from pyspark import SparkContext, SparkConf
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.window import Window


if __name__ == "__main__":
    mongoReviewsUri = "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews"
    mongoMetaUri = "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata"
    spark = SparkSession.builder.appName("partB1").getOrCreate()

    #get items with more than 100 reviews and avg of overall count.

    c2_sql = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",mongoReviewsUri).load()
    c2_sql.createOrReplaceTempView("temp1")
    c2_df = spark.sql("SELECT asin,count(*) as reviewCount,avg(overall) as avg FROM temp1 group by asin having reviewCount >= 100 ")
                
    #metadata
    metadata_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner").option("uri",mongoMetaUri).load()
    metadata_df.createOrReplaceTempView("temp2")
    metadata_result_df = spark.sql("SELECT asin as id, title, categories from temp2")
    #cc
    inner_join_df = c2_df.join(metadata_result_df,c2_df.asin == metadata_result_df.id,'inner')

    exploded = inner_join_df.withColumn("categories", explode(inner_join_df.categories))


    windowSpec = Window.partitionBy(exploded['categories']).orderBy(exploded["avg"].desc())
    a1 = exploded.withColumn("rank",F.dense_rank().over(windowSpec)).where(F.col("rank") == 1).drop("rank")
    #a1.show()
    #breaking ties
    final = a1.orderBy(a1["categories"])
    #final.show()
    #write to tsv
    column_order = ["categories","title","reviewCount","avg"]
    #requires pandas >=0.19
    output = final[column_order].toPandas()
    header = ["categories","Item title","number of reviews","average user rating"]
    output.columns=header
    with open("./output.txt", 'a') as f:
        output.to_csv(f,sep='\t',mode='a', header=True,index=False)
    
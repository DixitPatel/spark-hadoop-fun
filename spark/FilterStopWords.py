from pyspark.sql import SparkSession
import re
import sys
from operator import add


NON_ALPHABETIC_REGEX = re.compile(r"[^a-zA-Z]")

STOP_WORDS = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any","are",
                  "arent", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both",
                  "but", "by", "cant", "cannot", "could", "couldnt", "did", "didnt", "do", "does", "doesnt",
                  "doing","dont", "down", "during", "each", "few", "for", "from", "further", "had", "hadnt",
                  "has", "hasnt","have", "havent", "having", "he", "hed", "hell", "hes", "her", "here", "heres",
                  "hers", "herself","him", "himself", "his", "how", "hows", "i", "id", "ill", "im", "ive", "if",
                  "in", "into", "is", "isnt","it", "its", "its", "itself", "lets", "me", "more", "most", "mustnt",
                  "my", "myself", "no", "nor","not", "of", "off", "on", "once", "only", "or", "other", "ought",
                  "our", "ours", "ourselves", "out","over", "own", "same", "shant", "she", "shed", "shell",
                  "shes", "should", "shouldnt", "so", "some","such", "than", "that", "thats", "the", "their",
                  "theirs", "them", "themselves", "then", "there","theres", "these", "they", "theyd", "theyll",
                  "theyre", "theyve", "this", "those", "through", "to","too", "under", "until", "up", "very",
                  "was", "wasnt", "we", "wed", "well", "were", "weve", "were","werent", "what", "whats", "when",
                  "whens", "where", "wheres", "which", "while", "who", "whos", "whom","why", "whys", "with",
                  "wont", "would", "wouldnt", "you", "youd", "youll", "youre", "youve", "your","yours",
                  "yourself", "yourselves"]

def wordCount(lines):
    counts = lines.flatMap(lambda x: x.split(' ')) \
                  .map(lambda x: re.sub(NON_ALPHABETIC_REGEX, "", x))\
                  .map(lambda x: str(x).lower()) \
                  .filter(lambda x: str(x) != "" ) \
                  .filter(lambda x: str(x) not in STOP_WORDS) \
                  .map(lambda x: (x, 1)) \
                  .reduceByKey(add)

    return counts

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import lit

    spark = SparkSession.builder.appName("myApp").config("spark.mongodb.input.uri","mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews").getOrCreate()

    # read from MongoDB collection
    df_reviews = spark.read.format("com.mongodb.spark.sql").load()
    df_metadata = spark.read.format("com.mongodb.spark.sql").option("uri","mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata").load()
    # SQL

    print("DF1 schema: ")
    df_reviews.printSchema()
    print("DF2 schema: ")
    df_metadata.printSchema()

    print ("Bucketing")
    B1=df_reviews.filter(df_reviews["overall"] == 1.0)
    B2=df_reviews.filter(df_reviews["overall"] == 2.0)
    B3=df_reviews.filter(df_reviews["overall"] == 3.0)
    B4=df_reviews.filter(df_reviews["overall"] == 4.0)
    B5=df_reviews.filter(df_reviews["overall"] == 5.0)

    text1 = B1.select("reviewText").rdd.map(lambda r:r[0])
    text2 = B2.select("reviewText").rdd.map(lambda r:r[0])
    text3 = B3.select("reviewText").rdd.map(lambda r:r[0])
    text4 = B4.select("reviewText").rdd.map(lambda r:r[0])
    text5 = B5.select("reviewText").rdd.map(lambda r:r[0])

    counts = counts.takeOrdered(2000, key=lambda item: (-item[1], item[0]))

    count1 = wordCount(text1).takeOrdered(500, key=lambda item: (-item[1], item[0]))
    count2 = wordCount(text2).takeOrdered(500, key=lambda item: (-item[1], item[0]))
    count3 = wordCount(text3).takeOrdered(500, key=lambda item: (-item[1], item[0]))
    count4 = wordCount(text4).takeOrdered(500, key=lambda item: (-item[1], item[0]))
    count5 = wordCount(text5).takeOrdered(500, key=lambda item: (-item[1], item[0]))


    count1 = spark.sparkContext.parallelize(count1)
    count1.map(lambda line: "\t".join([str(x) for x in line])).repartition(1).saveAsTextFile(sys.argv[1]+"/rated1")

    count2 = spark.sparkContext.parallelize(count2)
    count2.map(lambda line: "\t".join([str(x) for x in line])).repartition(1).saveAsTextFile(sys.argv[1]+"/rated2")

    count3 = spark.sparkContext.parallelize(count3)
    count3.map(lambda line: "\t".join([str(x) for x in line])).repartition(1).saveAsTextFile(sys.argv[1]+"/rated3")

    count4 = spark.sparkContext.parallelize(count4)
    count4.map(lambda line: "\t".join([str(x) for x in line])).repartition(1).saveAsTextFile(sys.argv[1]+"/rated4")

    count5 = spark.sparkContext.parallelize(count5)
    count5.map(lambda line: "\t".join([str(x) for x in line])).repartition(1).saveAsTextFile(sys.argv[1]+"/rated5")



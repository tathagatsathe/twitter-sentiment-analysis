from flask import Flask, render_template, Response, Markup, stream_with_context, escape, request
from threading import Thread
from json import dumps, loads
from kafka import KafkaProducer, KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType, TimestampType, LongType
from textblob import TextBlob
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os, requests, json

# import warnings

# warnings.filterwarnings("ignore")

scala_version = '2.12'
spark_version = '3.2.2'

load_dotenv()

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]
jdbc_connector_jar = os.getenv('jdbc_connector_jar')

os.environ['PYSPARK_SUBMIT_ARGS'] = f'--jars {jdbc_connector_jar} pyspark-shell'
schema = StructType([
    StructField("data", StructType([
        StructField("edit_history_tweet_ids",ArrayType(StringType()),True),
        StructField("id",StringType(),True),
        StructField("text",StringType(),True),
        StructField("created_at",StringType())]), True),
    StructField("matching_rules",  ArrayType(StructType([
        StructField("id",StringType(),True),
        StructField("tag",StringType(),True)])), True)
])

spark = SparkSession \
        .builder \
        .appName("twitterApp") \
        .config("spark.jars.packages", ",".join(packages))\
        .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "twitter") \
    .option("startingOffsets", "latest") \
    .load()  

parsed_df = kafka_df \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

selected_fields = ["data.id","data.text","data.created_at",col("matching_rules").getItem(0).getItem("tag").alias("tag")]

transformed_df = parsed_df.select(selected_fields)

def get_sentiment(text):
    if text is not None and isinstance(text, str):
        blob = TextBlob(text)
        sentiment = blob.sentiment.polarity
        return sentiment, "positive" if sentiment > 0 else "negative" if sentiment < 0 else "neutral"
    else:
        return None, None

get_sentimentInt = udf(lambda text: get_sentiment(text)[0], DoubleType())
get_sentimentName = udf(lambda text: get_sentiment(text)[1], StringType())

# # Apply the UDF to the streaming DataFrame
sentiment_df = transformed_df.withColumn("sentimentVal", get_sentimentInt("text")) \
                             .withColumn("sentimentName", get_sentimentName("text"))

# Write the transformed data to the console
query = sentiment_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

user = os.getenv('user')
password = os.getenv('password')
db_uri = f"mysql+pymysql://{user}:{password}@localhost/twitter"

# Define the function to write each micro-batch of the streaming DataFrame to MySQL
def write_to_mysql(batch_df, batch_id):
    
    # batch_df.show()
    selected_cols_df = batch_df.select(
        batch_df.id.cast(LongType()),
        batch_df.text.cast(StringType()),
        batch_df.tag.cast(StringType()),
        batch_df.sentimentVal.cast(DoubleType()),
        batch_df.sentimentName.cast(StringType()),
        batch_df.created_at.cast(TimestampType())
    )
    # selected_cols_df.show()
    # Convert the Spark DataFrame to a Pandas DataFrame
    pandas_df = selected_cols_df.toPandas()
    
    # Write the Pandas DataFrame to MySQL
    engine = create_engine(db_uri,encoding="utf-8")
    pandas_df.to_sql(name='sentiments', con=engine, if_exists='append', index=False)

# Start the streaming query and write each micro-batch to MySQL
sentiment_df.writeStream.foreachBatch(write_to_mysql).start()

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], 
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

# Define a function to write the streaming data to Kafka
def write_to_kafka(batch_df, batch_id):
    # Convert the DataFrame to a Pandas DataFrame
    pandas_df = batch_df.toPandas()

    # Convert the Pandas DataFrame to a list of dictionaries
    records = pandas_df.to_dict(orient='records')

    # Write each record to Kafka
    for record in records:
        producer.send('sentiment', value=record)

sentiment_df.writeStream.foreachBatch(write_to_kafka).start()

bearer_token = os.getenv('TWITTER_BEARER_TOKEN')

url = 'https://api.twitter.com/2/tweets/search/stream?tweet.fields=created_at'
headers = {'Authorization': f'Bearer {bearer_token}'}

def add_rule(rule):
    
    add_rules_url = "https://api.twitter.com/2/tweets/search/stream/rules"
    data = {
        "add": [
            {"value": rule, "tag": rule}
    ]}
    response = requests.post(add_rules_url,headers=headers,json=data)

    if response.status_code !=201:
        print(f"Error {response.status_code}: {response.text}")
    else:
        print("Rules added Successfully.")

def stream_tweets():
    tweets_producer = KafkaProducer(bootstrap_servers='localhost:9092')

    # send GET request to API endpoint
    response = requests.get(url, headers=headers, stream=True)

    # parse JSON data from response and publish to Kafka topic
    for line in response.iter_lines():
        if line:
            tweets_producer.send('twitter', value=line)

    tweets_producer.close()

app = Flask(__name__)

@app.route('/')
def index():
    return '''
        <div style="margin:100px auto;text-align: center;">
            <form method="POST" action="/stream" style="display: inline-block;">
                <input type="text" name="text" style="border:1px solid gray;border-radius:5px; height:30px;width: 300px;">
                <input type="submit" value="Add rule" style="border:0;border-radius:5px; height:30px;width: 100px;color:white;background-color:#1DA1F2;">
            </form>
        </div>
    '''
# , methods=['POST']
@app.route("/stream")
def stream():
    # rule = request.form["text"]

    # add_rule(rule)
    # t = Thread(target=stream_tweets)
    # t.start()
    def generate():
        # Consume the Kafka messages
        consumer = KafkaConsumer('sentiment', bootstrap_servers=['localhost:9092'], 
                                 auto_offset_reset='latest', enable_auto_commit=True,
                                 value_deserializer=lambda x: loads(x.decode('utf-8')))

        for message in consumer:
            tweet = message.value["text"]
            sentiment = message.value["sentimentName"]
            if sentiment == "positive":
                color = "green"
            elif sentiment == "negative":
                color = "red"
            else:
                color = "gray"
            # yield Markup(f"<p style='color:{color}'><b>{tweet}</b></p>\n\n")
            yield Markup(f"<div style='border:1px solid gray;border-top:0px;width:800px;border-radius:0px;padding:5px;margin:0px auto;'><p style='color:{color};'><b>{tweet}</b></p></div>")          

    return Response(generate())

from analysis import plots
@app.route('/analysis')
def analysis():
    return plots()

if __name__ == '__main__':
    app.run(debug=True)

            
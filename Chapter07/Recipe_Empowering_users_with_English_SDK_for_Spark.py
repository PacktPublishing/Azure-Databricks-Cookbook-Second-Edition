# Import necessary libraries
from pyspark.sql import SparkSession
import os
from langchain.chat_models import ChatOpenAI
from pyspark_ai import SparkAI

# Step 1: Set up the OpenAI API key as an environment variable
os.environ['OPENAI_API_KEY'] = '<your-openai-api-key>'  # Replace with your actual API key

# Step 2: Initialize and activate the SDK
spark = SparkSession.builder.appName("DataAnalysisWithDatabricks").getOrCreate()
chatOpenAI = ChatOpenAI(model='gpt-4')
spark_ai = SparkAI(llm=chatOpenAI)
spark_ai.activate()

# Example 1: Basic DataFrame Operations
# Load data
df = spark_ai._spark.sql("SELECT * FROM samples.nyctaxi.trips")

# Average calculation using English instructions
avg_trip_distance_df = df.ai.transform("Calculate the average trip distance for each day in January 2016.")
avg_trip_distance_df.display()

# Data visualization
df.ai.plot("Show a bar chart of trip counts per day in January 2016.")

# Get a plain English explanation of the data
explanation = df.ai.explain("Describe the contents of this DataFrame.")
print(explanation)

# Example 2: Advanced Features
# Ingest external data
auto_sales_df = spark_ai.create_df("Load data for 2022 USA national auto sales by brand.")

# Define and use User-Defined Functions (UDFs) in an intuitive way
@spark_ai.udf
def classify_trip_distance(distance: float) -> str:
    """Categorize trip distance as 'Short', 'Medium', or 'Long'."""
    if distance < 5:
        return 'Short'
    elif distance < 15:
        return 'Medium'
    else:
        return 'Long'

classified_trips_df = df.withColumn("Trip_Length", classify_trip_distance(df["trip_distance"]))
classified_trips_df.show()

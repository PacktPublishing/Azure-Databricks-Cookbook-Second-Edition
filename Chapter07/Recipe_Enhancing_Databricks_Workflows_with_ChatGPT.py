Let’s begin with a structured approach to design, and then conclude with prompts that build upon
previous ones. These prompts highlight how chat history can influence the evolution of prompts.
Follow these steps:
1. Begin by identifying the data sources you plan to stream. Use the following prompt:
Design an Azure Databricks streaming architecture that
ingests data from [specify data sources, e.g., IoT
devices, web logs, etc.] using Azure Event Hubs.
2. Describe how Azure Databricks will process the streaming data. Mention the use of Azure
Databricks notebooks and Spark Structured Streaming:
The architecture should utilize Azure Databricks for
real-time processing of streaming data, employing Spark
Structured Streaming for data manipulation and analysis.
3. Highlight the integration with Delta Lake for managing and storing the processed data. For
example, you can use the following prompt:
Include steps for integrating the processed data with
Delta Lake for efficient management and storage,
leveraging Delta Lake's capabilities for incremental
processing.
4. Specify the requirements for real-time analytics and model serving, such as using Azure
Databricks for machine learning and AI on streaming data. For example, you can enter the
following prompt:
Outline the setup for conducting real-time analytics on
the streaming data and serving machine learning models in
real time.
5. Mention the need for security and compliance in your architecture with another prompt:
Ensure the architecture adheres to Azure's security
and compliance standards, including the use of Azure
Databricks' built-in security features.
Enhancing Databricks workflows with ChatGPT-3.5 or GPT-4 chat integration 5
6. Include some considerations for monitoring and logging by running the following prompt:
Detail the monitoring and logging mechanisms using
Azure's native tools for tracking the performance and
health of the streaming architecture.
7. Define the output modes (such as complete, append, update) and sinks for the processed data.
8. If necessary, include steps to use sample data for testing and initializing the stream.





Note*****
ChatGPT’s output will always vary. The output you receive should be similar to the example
output shown but will not match it exactly.
Prompt #1
I want you to act as an Azure Databricks Data Architect. I need
you to design an Azure Databricks streaming architecture that
ingests data from using Azure Event Hubs and Azure Data Lake
Storage Gen2. I would like the data to be ingested using Azure
Databricks and Azure Data Factory.
Prompt #2
Based on the above, please expand the architecture to add a step that after the Data is ingested, the
data needs to be accessed by Microsoft Fabric.
Prompt #3
Based on the above, can you provide detailed build directions?
Prompt #4
Based on the above, can you provide suggestions on how to secure the
architecture?
Prompt # 5
Based on the above, can you provide detailed build directions with
sample code?
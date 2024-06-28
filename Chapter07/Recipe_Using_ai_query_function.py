
# Recipe: Using the ai_query function

%sql
-- Step 1: Data Summarization
-- Use an AI model to create a concise summary of Azure Databricks SQL.
SELECT ai_query('my-external-model-openai-chat', 'Describe Azure Databricks SQL in 30 words.') AS summary;

-- Step 2: Spam Classification
-- Classify incoming messages as spam using a custom AI model that evaluates message attributes.
SELECT text,
       ai_query(endpoint => 'spam-classification-endpoint',
                request => named_struct('timestamp', timestamp, 'sender', from_number, 'text', text),
                returnType => 'BOOLEAN') AS is_spam
FROM messages;

-- Step 3: Grammar Correction
-- Automatically correct grammatical errors in text data using generative AI.
SELECT *,
       ai_fix_grammar(text) AS corrected_text
FROM articles;

-- Step 4: Revenue Forecasting
-- Employ an AI model to forecast weekly retail revenue, assisting in strategic planning.
SELECT ai_query('weekly-forecast',
                request => struct(*),
                returnType => 'FLOAT') AS predicted_revenue
FROM retail_revenue;

-- Step 5: Interactive Customer Support
-- Engage in an interactive AI-driven chat to handle customer inquiries in real-time.
SELECT ai_query('custom-llama-2-7b-chat',
                request => named_struct("messages", ARRAY(named_struct("role", "user", "content", "What is ML?"))),
                returnType => 'STRUCT<candidates:ARRAY<STRING>>') AS chat_response;

-- Create a new table or replace the existing 'account_activity_cleansed' table.
-- This table will store cleansed account activity data.
CREATE OR REPLACE TABLE demo_dataset.account_activity_cleansed
AS
-- Select columns from the external Spanner table 'account_activity_raw' using EXTERNAL_QUERY.
SELECT
  transaction_id,
  account_id,
  -- Cast the timestamp column to the TIMESTAMP data type.
  cast(timestamp as timestamp) as ts_val,
  location,
  device_type,
  ip_address,
  -- Cast the transaction_amount column to the NUMERIC data type.
  cast(transaction_amount as numeric) as transaction_amount,
  transaction_type,
  -- Cast the successful_login column to the BOOL data type.
  CAST(successful_login AS BOOL) successful_login,
  -- Cast the unusual_activity column to the BOOL data type. This will be the target variable for the model.
  CAST(unusual_activity AS BOOL) as unusual_activity,
  -- Generate a random number between 0 and 1 for each row. This will be used to split the data into training, evaluation, and prediction sets.
  RAND() AS classifier
FROM
  -- Query the external Spanner table 'account_activity_raw' via the connection 'demo-spanner-conn'.
  EXTERNAL_QUERY("my_spanner_db.us-central1.demo-spanner-conn", "SELECT * FROM account_activity_raw");


-- This query demonstrates how to check the distribution of data into training, evaluation, and prediction categories
-- based on the 'classifier' column values.
SELECT case when classifier<0.8 then "training"       -- 80% for training
            when classifier between 0.8 and 0.9 then "evaluation" -- 10% for evaluation
            else "prediction" end as category,        -- 10% for prediction
count(*)
from demo_dataset.account_activity_cleansed 
group by category;


-- Create or replace a BQML model named 'fraud_model' in the 'demo_dataset' dataset.
CREATE OR REPLACE MODEL
`demo_dataset.fraud_model`
OPTIONS
( -- Specify the model type as Logistic Regression.
  model_type='LOGISTIC_REG',
  -- Automatically adjust class weights to handle potential imbalance in the target variable ('unusual_activity').
  auto_class_weights=TRUE,
  -- Indicate that the data is already split using the 'classifier' column; the model training will only use the specified subset.
  data_split_method='NO_SPLIT',
  -- Specify the column to be predicted by the model.
  input_label_cols=['unusual_activity'],
  -- Set the maximum number of training iterations.
  max_iterations=15
) AS
-- Select all columns except the 'classifier' column used for splitting.
SELECT * EXCEPT(classifier)
FROM
`demo_dataset.account_activity_cleansed`
-- Use only the data designated for training (classifier value less than 0.8).
WHERE
classifier < 0.8;

-- Evaluate the performance of the trained 'fraud_model'.
SELECT
*
FROM
-- Use the ML.EVALUATE function to get evaluation metrics.
ML.EVALUATE (MODEL `demo_dataset.fraud_model`,
  (
  -- Select all columns from the cleansed data.
  SELECT
    *
  FROM
    `demo_dataset.account_activity_cleansed`
  -- Use only the data designated for evaluation (classifier value between 0.8 and 0.9).
  WHERE
    classifier between 0.8 and 0.9
  )
);

-- Predict unusual activity using the 'fraud_model' and explain the predictions.
SELECT
*
FROM
-- Use ML.EXPLAIN_PREDICT to get predictions along with feature attributions (explanations).
ML.EXPLAIN_PREDICT (MODEL `demo_dataset.fraud_model`,
  (
  -- Select all columns from the cleansed data.
  SELECT
    *
  FROM
    `demo_dataset.account_activity_cleansed`
  -- Use only the data designated for prediction (classifier value greater than 0.9).
  WHERE
    classifier > 0.9
  )
)
-- Filter the results to show only the instances where the model's prediction differs from the actual 'unusual_activity' label.
-- This helps in analyzing misclassifications.
where predicted_unusual_activity <> unusual_activity;



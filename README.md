
![bqml-demo](https://github.com/user-attachments/assets/7d98da3b-29cc-48fc-90f2-a940cbec25df)


# BigQuery ML Account Fraud Detection Demo

This SQL script demonstrates a basic workflow for building, evaluating, and explaining a machine learning model for account fraud detection using BigQuery ML. The script uses data from an external Cloud Spanner table.

## Overview

The script performs the following steps:

1.  **Data Cleansing and Preparation:** Creates a new table (`datasherlock_usc1.account_activity_cleansed`) by querying raw account activity data from a Cloud Spanner table (`account_activity_raw`) via an external connection (`demo-spanner-conn`). During this process, it:
    *   Casts data types for timestamp, transaction amount, successful login, and unusual activity columns.
    *   Adds a `classifier` column with random values between 0 and 1 to facilitate splitting the data into training, evaluation, and prediction sets.
2.  **Data Split Verification:** Includes a query to check the distribution of data across the training (80%), evaluation (10%), and prediction (10%) sets based on the `classifier` column.
3.  **Model Training:** Creates or replaces a BigQuery ML Logistic Regression model (`datasherlock_usc1.fraud_model`) using the cleansed data. Key options used:
    *   `model_type='LOGISTIC_REG'`: Specifies the model algorithm.
    *   `auto_class_weights=TRUE`: Helps handle class imbalance in the target variable (`unusual_activity`).
    *   `data_split_method='NO_SPLIT'`: Informs BQML that the data is already split manually.
    *   `input_label_cols=['unusual_activity']`: Defines the target variable.
    *   The training uses only the data where `classifier < 0.8`.
4.  **Model Evaluation:** Evaluates the trained model's performance using `ML.EVALUATE` on the evaluation dataset (`classifier between 0.8 and 0.9`).
5.  **Prediction and Explanation:** Uses `ML.EXPLAIN_PREDICT` to predict unusual activity on the prediction dataset (`classifier > 0.9`) and provides feature attributions (explanations) for the predictions. The results are filtered to show only instances where the prediction differs from the actual label (misclassifications).
6.  **(Commented Out) Example Update:** Includes a commented-out example `UPDATE` statement showing how to modify data in the cleansed table.

## Prerequisites

*   Access to a Google Cloud project with BigQuery and Cloud Spanner APIs enabled.
*   A BigQuery dataset named `datasherlock_usc1`.
*   A BigQuery connection named `datasherlock.us-central1.demo-spanner-conn` configured to access a Cloud Spanner instance.
*   A Cloud Spanner table named `account_activity_raw` within the connected Spanner database, containing the necessary columns (`transaction_id`, `account_id`, `timestamp`, `location`, `device_type`, `ip_address`, `transaction_amount`, `transaction_type`, `successful_login`, `unusual_activity`).

## How to Use

1.  **Ensure Prerequisites:** Verify that all prerequisites listed above are met. Pay close attention to the dataset name (`datasherlock_usc1`) and the connection name (`datasherlock.us-central1.demo-spanner-conn`) used in the script; adjust them if your environment uses different names.
2.  **Execute the Script:** Run the SQL commands sequentially in the BigQuery console, using the `bq` command-line tool, or through a BigQuery client library.

## Expected Outputs

*   A BigQuery table named `datasherlock_usc1.account_activity_cleansed` containing the prepared data.
*   Query results showing the distribution of data into training, evaluation, and prediction sets.
*   A BigQuery ML model named `datasherlock_usc1.fraud_model`.
*   Query results from `ML.EVALUATE` showing model performance metrics (e.g., precision, recall, accuracy, f1-score, roc_auc).
*   Query results from `ML.EXPLAIN_PREDICT` showing predictions, actual labels, and feature attributions for misclassified instances in the prediction set.


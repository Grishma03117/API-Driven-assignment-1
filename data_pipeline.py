import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from prefect import get_run_logger, task, flow
from prefect.artifacts import create_markdown_artifact
import io
import base64

# Load dataset
@task(log_prints=True)
def load_data(file_path):
    logger = get_run_logger()  # Prefect logger
    logger.info(f"Loading dataset from {file_path}...")
    data = pd.read_csv(file_path)
    logger.info(f"Data loaded successfully. First 5 rows: \n{data.head().to_string()}")
    return data

# Data preprocessing
@task(log_prints=True)
def preprocess_data(data):
    logger = get_run_logger()  # Prefect logger

    # Summary statistics
    logger.info("Summary Statistics:")
    logger.info(data.describe().to_string())  # Log summary statistics

    # Check for missing values
    logger.info("Missing Values:")
    logger.info(data.isnull().sum().to_string())  # Log missing values

    # Impute missing values for numeric columns
    for column in data.select_dtypes(include=[np.number]).columns:
        logger.info(f"Imputing missing values for column: {column}")
        data[column].fillna(data[column].mean(), inplace=True)

    # Impute missing values for categorical columns
    for column in data.select_dtypes(include=['object']).columns:
        logger.info(f"Imputing missing values for categorical column: {column}")
        data[column].fillna(data[column].mode()[0], inplace=True)  # Using mode for categorical columns

    return data

# Exploratory Data Analysis (EDA)
@task(log_prints=True)
def exploratory_data_analysis(data):
    logger = get_run_logger()  # Prefect logger

    # Correlation coefficients for numeric features
    logger.info("Calculating Correlation Matrix for numeric features:")
    numeric_data = data.select_dtypes(include=[np.number])
    correlation_matrix = numeric_data.corr()
    logger.info(correlation_matrix.to_string())

    # Heatmap of Correlation Matrix
    logger.info("Displaying Heatmap of Correlation Matrix...")

    # Save heatmap to a BytesIO object
    heatmap_buffer = io.BytesIO()
    plt.figure(figsize=(10, 6))
    sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
    plt.title('Heatmap of Correlation Matrix')
    plt.savefig(heatmap_buffer, format='png')
    plt.close()  # Close the plot to avoid display issues
    
    # Convert buffer to a base64-encoded image
    heatmap_buffer.seek(0)  # Go to the start of the buffer
    heatmap_image = base64.b64encode(heatmap_buffer.getvalue()).decode('utf-8')
    heatmap_markdown = f"![Heatmap](data:image/png;base64,{heatmap_image})"
    
    # Binning a continuous variable into categories
    logger.info("Binning the first numeric feature into 'Low', 'Medium', 'High' categories...")
    numeric_col = numeric_data.columns[0] if not numeric_data.empty else None
    if numeric_col:
        data['binned_feature'] = pd.cut(data[numeric_col], bins=3, labels=['Low', 'Medium', 'High'])
        logger.info(f"Binned feature based on {numeric_col}:")
        logger.info(data[['binned_feature', numeric_col]].head().to_string())
        
    # Create and log artifact
    create_markdown_artifact(f"### Heatmap of Correlation Matrix\n\n{heatmap_markdown}")
    logger.info("Heatmap artifact created successfully.")

# Define your flow
@flow(name="Data processing")
def main_flow(file_path):
    data = load_data(file_path)
    processed_data = preprocess_data(data)
    
    # Save/Rewrite processed data to a file
    processed_data_path = r"processed_data.csv"
    processed_data.to_csv(processed_data_path, index=False)
    print(f"Processed data saved to {processed_data_path}")
    
    exploratory_data_analysis(processed_data)

# Run the flow with a sample file path
if __name__ == "__main__":
    file_path = r"Crime_Data_from_2020_to_Present_1.csv";
    main_flow.serve(
        name="data-pipeline",
        parameters={"file_path": file_path}, 
        tags=["DataOps", "EDA"], 
        description="Deployment to process data every two minutes", 
        cron="*/2 * * * *"
    )

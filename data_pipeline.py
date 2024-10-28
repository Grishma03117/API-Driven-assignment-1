import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from prefect import get_run_logger, task, flow
from prefect.artifacts import create_markdown_artifact
# <<<<<<< main
import io, os
# =======
from scipy.stats import chi2_contingency, pointbiserialr, f_oneway
import io
# >>>>>>> main
import base64
from pprint import pprint, pformat

# Load dataset
@task(log_prints=True)
def load_data(file_path):
    logger = get_run_logger()
    try:
        logger.info(f"Loading dataset from {file_path}...")
        data = pd.read_csv(file_path)
        logger.info("Data loaded successfully. First 5 rows:\n" + pformat(data.head().to_dict(orient="records")))
        return data
    except Exception as e:
        logger.error("Error loading data:\n" + pformat({"file_path": file_path, "error": str(e)}))
        raise

# Data preprocessing
@task(log_prints=True)
def preprocess_data(data):
    logger = get_run_logger()
    try:
        logger.info("Starting data preprocessing...")
        
        # Summary statistics
        logger.info("Summary Statistics:\n" + pformat(data.describe().to_dict()))

        # Check for missing values
        missing_values = data.isnull().sum()
        logger.info("Missing Values:\n" + pformat(missing_values.to_dict()))

        # Impute missing values for numeric columns
        for column in data.select_dtypes(include=[np.number]).columns:
            missing_count = data[column].isnull().sum()
            if missing_count > 0:
                logger.info(f"Imputing {missing_count} missing values in numeric column: '{column}'")
                data[column].fillna(data[column].mean(), inplace=True)

        # Impute missing values for categorical columns
        for column in data.select_dtypes(include=['object']).columns:
            missing_count = data[column].isnull().sum()
            if missing_count > 0:
                logger.info(f"Imputing {missing_count} missing values in categorical column: '{column}'")
                data[column].fillna(data[column].mode()[0], inplace=True)

# <<<<<<< main
        logger.info("Data preprocessing complete.")
        return data
    except Exception as e:
        logger.error("Error during data preprocessing:\n" + pformat({"error": str(e)}))
        raise

# =======
    return data
   
    
# >>>>>>> main
# Exploratory Data Analysis (EDA)
@task(log_prints=True)
def exploratory_data_analysis(data):
    logger = get_run_logger()
    try:
        # Correlation coefficients for numeric features
        numeric_data = data.select_dtypes(include=[np.number])
        correlation_matrix = numeric_data.corr()
        logger.info("Correlation Matrix for numeric features:\n" + pformat(correlation_matrix.to_dict()))

# <<<<<<< main
        # Save the heatmap locally
        local_heatmap_path = os.path.join(os.getcwd(), "heatmap.png")
        heatmap_path = 'heatmap.png'
        plt.figure(figsize=(10, 6))
        sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', linewidths=0.5)
        plt.title('Heatmap of Correlation Matrix')
        plt.savefig(local_heatmap_path)  # Save locally
        plt.close()  # Close the plot to avoid display issues
        logger.info(f"Heatmap saved locally at {local_heatmap_path}")
# =======
    # Correlation coefficients for numeric features (Pearson/Spearman)
    logger.info("Calculating Correlation Matrix for numeric features:")
    numeric_data = data.select_dtypes(include=[np.number])
    correlation_matrix = numeric_data.corr(method='pearson')  # or 'spearman' if needed
    logger.info(correlation_matrix.to_string())
# >>>>>>> main

        # Convert heatmap image to a base64-encoded string for artifact
        with open(heatmap_path, "rb") as image_file:
            encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
            heatmap_markdown = f"![Heatmap](data:image/png;base64,{encoded_image})"

# <<<<<<< main
        numeric_col = numeric_data.columns[0] if not numeric_data.empty else None
        if numeric_col:
            data['binned_feature'] = pd.cut(data[numeric_col], bins=3, labels=['Low', 'Medium', 'High'])
            logger.info(f"Binned feature based on {numeric_col}:\n" + pformat(data[['binned_feature', numeric_col]].head().to_dict(orient="records")))

        create_markdown_artifact(f"### Heatmap of Correlation Matrix\n\n{heatmap_markdown}")
        logger.info("Heatmap artifact created successfully.")
    except Exception as e:
        logger.error("Error during EDA:\n" + pformat({"error": str(e)}))
        raise
# =======
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
    
    # Create and log artifact
    create_markdown_artifact(f"### Heatmap of Correlation Matrix\n\n{heatmap_markdown}")
    logger.info("Heatmap artifact created successfully.")

    # Binning a continuous variable into categories
    logger.info("Binning the first numeric feature into 'Low', 'Medium', 'High' categories...")
    numeric_col = numeric_data.columns[0] if not numeric_data.empty else None
    if numeric_col:
        data['binned_feature'] = pd.cut(data[numeric_col], bins=3, labels=['Low', 'Medium', 'High'])
        logger.info(f"Binned feature based on {numeric_col}:")
        logger.info(data[['binned_feature', numeric_col]].head().to_string())
        
        
    # Univariate analysis (Histograms and Box Plots)
    logger.info("Generating histograms and box plots for numeric features...")
    if 'Vict Age' in data.columns and not data['Vict Age'].isnull().all():
    #for column in numeric_data.columns:
      plot_buffer = io.BytesIO()
      plt.figure(figsize=(12, 5))

    # Histogram
      plt.subplot(1, 2, 1)
      sns.histplot(data['Vict Age'], kde=True)
      plt.title(f'Histogram of {'Vict Age'}')
      plt.xlabel('Vict Age')
        
    # Box Plot
      plt.subplot(1, 2, 2)
      sns.boxplot(x=data['Vict Age'])
      plt.title(f'Box Plot of {'Vict Age'}')
      plt.xlabel('Vict Age')

      plt.tight_layout()
    #plt.show()  # Display plots in the notebook
        
    # Save the plot to the buffer
      plt.savefig(plot_buffer, format='png')
      plt.close()

    # Convert buffer to base64-encoded image
      plot_buffer.seek(0)
      plot_image = base64.b64encode(plot_buffer.getvalue()).decode('utf-8')
      plot_markdown = f"![Histogram and Boxplot for Vict Age](data:image/png;base64,{plot_image})"
    
    # Create artifact for the histogram and box plot
      create_markdown_artifact(f"### Histogram and Boxplot for Vict Age\n\n{plot_markdown}")
      logger.info(f"Artifact for 'Vict Age' created successfully.")
    
    
# >>>>>>> main

@flow(name="Data processing")
def main_flow(file_path):
    try:
        data = load_data(file_path)
        processed_data = preprocess_data(data)
        processed_data_path = "processed_data_datapipeline.csv"
        processed_data.to_csv(processed_data_path, index=False)
        logger = get_run_logger()
        logger.info(f"Processed data saved to {processed_data_path}")
        exploratory_data_analysis(processed_data)
    except Exception as e:
        logger = get_run_logger()
        logger.error("Error in main flow:\n" + pformat({"error": str(e)}))
        raise

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

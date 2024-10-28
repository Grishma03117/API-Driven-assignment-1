# Import necessary libraries
import pandas as pd
from prefect import get_run_logger, task, flow
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder

from pprint import pformat

from data_pipeline import main_flow


@task(log_prints=True)
def create_categorical_variables(X, y):
    logger = get_run_logger()
    try:
        logger.info("Preprocessing categorical variables and handling missing values...")

        # Handle missing values
        X = X.fillna('Unknown')
        y = y.fillna('Unknown')

        # Convert categorical variables to numeric using LabelEncoder
        le_sex = LabelEncoder()
        le_area = LabelEncoder()
        le_premise = LabelEncoder()

        X['Vict Sex'] = le_sex.fit_transform(X['Vict Sex'])
        X['AREA'] = le_area.fit_transform(X['AREA'])
        X['Premis Desc'] = le_premise.fit_transform(X['Premis Desc'])

        logger.info("Categorical variable transformation complete:\n" + pformat(X.head().to_dict(orient="records")))
        return X, y
    except Exception as e:
        logger.error("Error in categorical variable creation:\n" + pformat({"error": str(e)}))
        raise

# Feature Importance
@task(log_prints=True)
def feature_importance(X, model):
    logger = get_run_logger()
    try:
        feature_importances = model.feature_importances_
        features = X.columns

        # Log feature importance in a structured way
        importance_dict = {feature: importance for feature, importance in zip(features, feature_importances)}
        logger.info("Feature importances:\n" + pformat(importance_dict))
    except Exception as e:
        logger.error("Error calculating feature importance:\n" + pformat({"error": str(e)}))
        raise

@task(log_prints=True)
def train_model(X, y):
    logger = get_run_logger()
    try:
        logger.info("Splitting data into training and testing sets...")

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # Initialize and train the Random Forest Classifier
        rf_model = RandomForestClassifier(n_estimators=50, max_depth=25, random_state=42)
        rf_model.fit(X_train, y_train)
        logger.info("Model training complete.")
        return X_train, X_test, y_train, y_test, rf_model
    except Exception as e:
        logger.error("Error during model training:\n" + pformat({"error": str(e)}))
        raise

@task(log_prints=True)
def evaluate_model(X_test, y_test, model):
    logger = get_run_logger()
    try:
        logger.info("Evaluating the model...")

        # Make predictions and evaluate
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        logger.info(f"Model Accuracy: {accuracy:.4f}")
        logger.info("\nClassification Report:\n" + pformat(classification_report(y_test, y_pred, output_dict=True)))
    except Exception as e:
        logger.error("Error during model evaluation:\n" + pformat({"error": str(e)}))
        raise
    
@flow(name="Machine Learning Model Training")
def ml_flow(file_path):
    logger = get_run_logger()
    try:
        logger.info("Starting the machine learning pipeline...")

        # Load the dataset
        df = pd.read_csv(file_path)
        logger.info("Dataset loaded for model training.")

        # Select relevant features and target variable
        X = df[['Vict Age', 'Vict Sex', 'TIME OCC', 'AREA', 'Premis Desc']]
        y = df['Weapon Desc']

        # Preprocess data and create categorical variables
        X, y = create_categorical_variables(X, y)

        # Train the model
        X_train, X_test, y_train, y_test, model = train_model(X, y)

        # Evaluate model
        evaluate_model(X_test, y_test, model)

        # Log feature importance
        feature_importance(X, model)
        logger.info("Machine learning pipeline completed.")
    except Exception as e:
        logger.error("Error in machine learning flow:\n" + pformat({"error": str(e)}))
        raise
    

if __name__ == "__main__":
    file_path = r"processed_data_datapipeline.csv";
    ml_flow.serve(
        name="machine-learning-pipeline",
        parameters={"file_path": file_path}, 
        tags=["MLOps", "Model training"], 
        description="Machine Learning pipeline deployment to run every 2 days at midnight", 
        cron="* * */2 * *"
    )

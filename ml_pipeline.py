# Import necessary libraries
import pandas as pd
from prefect import get_run_logger, task, flow
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score
from sklearn.preprocessing import LabelEncoder

from data_pipeline import main_flow

@task(log_prints=True)
def create_categorical_variables(X, y):
    # Preprocess the data
    # Handle missing values if any
    X = X.fillna('Unknown')
    y = y.fillna('Unknown')

    # Convert categorical variables to numeric using LabelEncoder    
    le_sex = LabelEncoder()
    le_area = LabelEncoder()
    le_premise = LabelEncoder()

    X['Vict Sex'] = le_sex.fit_transform(X['Vict Sex'])
    X['AREA'] = le_area.fit_transform(X['AREA'])
    X['Premis Desc'] = le_premise.fit_transform(X['Premis Desc'])
    
    return X, y

# Feature Importance
@task(log_prints=True)
def feature_importance(X, model):
    feature_importances = model.feature_importances_
    features = X.columns

    # Display feature importance
    for feature, importance in zip(features, feature_importances):
        print(f"{feature}: {importance:.4f}")

@task(log_prints=True)
def train_model(X, y):
    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the Random Forest Classifier
    rf_model = RandomForestClassifier(n_estimators=50, max_depth=25, random_state=42)
    rf_model.fit(X_train, y_train)
    
    return X_train, X_test, y_train, y_test, rf_model

@task(log_prints=True)
def evaluate_model(X_test, y_test, model):
    # Make predictions
    y_pred = model.predict(X_test)

    # Evaluate the model
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy}")
    print("\nClassification Report:")
    print(classification_report(y_test, y_pred))
    
@flow(name="Machine Learning Model Training")
def ml_flow(file_path):
    # Load the dataset (replace 'your_data.csv' with the actual file path)
    df = pd.read_csv(file_path)

    # Select relevant features (X) and target variable (y)
    # Features: Age, Sex, Time of Occurrence, Area, Premise Description
    X = df[['Vict Age', 'Vict Sex', 'TIME OCC', 'AREA', 'Premis Desc']]

    # Target: Crime Description or Mode of Killing (you can adjust to your column name, e.g., 'Crm Cd Desc' or 'Weapon Desc')
    y = df['Weapon Desc']  # or df['Weapon Desc']
    
    X,y = create_categorical_variables(X, y)
    
    X_train, X_test, y_train, y_test, model = train_model(X, y)
    
    evaluate_model(X_test, y_test, model)
    
    feature_importance(X, model)
    

if __name__ == "__main__":
    file_path = r"processed_data.csv";
    ml_flow.serve(
        name="machine-learning-pipeline",
        parameters={"file_path": file_path}, 
        tags=["MLOps", "Model training"], 
        description="Machine Learning pipeline deployment to run every 2 days at midnight", 
        #cron="* * */2 * *"
    )

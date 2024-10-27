# API-Driven-assignment-1

### To setup and run the project
1. Clone the repository and move into the directory of the project
2. Run the command `pip install -r requirements.txt` to replicate the env with all required dependencies

#### Prefect Cloud setup
1. Go to prefect cloud https://app.prefect.cloud/ and create a free account if not done already
2. Create an api key and save for future uses
3. On local machine, run the command `prefect cloud login` and provide the api key for authentication
4. You are logged in to prefect cloud and can see all the deployments on prefect web-based UI

#### Deployment steps
1. Update the credentials.txt file by adding the Prefect api key downloaded in the above steps
2. To run the DataOps deployment, run the command `python data_pipeline.py`
Let the prefect server run and you'll get an endpoint in the CLI which will redirect you to the prefect clous webpage which has the run being shown.
**Note:** Once run, the run will be initiated after 2 minutes and will continue every 2 minutes unless [hysically shut down by Ctrl+C in the CLI where the python command was run
3. Same for MLOps deployment, run `python ml_pipeline.py`
4. To get the deployment details through prefect API, first fill the required IDs in the credentials.txt and then run `python api_requests.py`

#### Project overall architecture
This project contains the following files:
1. **Crime_Data_from_2020_to_Present_1.csv**
This file contains the dataset which will be used for data preprocessing as well as model training to derive insights whenever required.

2. **data_pipeline.py** - DataOps
This file has the logic for data preprocessing including data cleanup, filling of empty values, exploratory data analysis, feature prioritization, Normalization as well as binning. The results of feature prioritization will be generated in the form of a heatmap which can be seen in the **artifacts** section of the prefcet flow run.
The processed data will be stored in the file `preprocessed_data`.
This code is deployed to prefect cloud, with the run scheduled for every 2 minutes

3. **ml_pipeline.py** - MLOps
This file is responsible for loading the preprocessed data from `preprocessed_data` file and train the model as per the required use case. It will also do a 70-30 training testing mechanism where 70% of the data is used to train the model and 30% is used to test the model. After that the prediction metrics of the mdoel is derived.
This code is deployed to prefect cloud, with the run scheduled for once every 2 days.

4. **api_requests.py**
This file utilizes the Prefect cloud APIs to retrieve the details of the deployment, flow, logs for both the DataOps and MLOps cycle. It has a user interactive menu program.

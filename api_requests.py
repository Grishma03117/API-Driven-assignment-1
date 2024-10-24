import requests
from datetime import datetime

token = "pnu_nExhzoNzfJFWp62c8srJWDr8csqSw61dcnGJ" 
account_id = "5e8c03a4-2ea0-4759-8183-e587f0dbee28"
workspace_id = "ec453e67-49ac-47ad-be9e-ea38dad0a9a3"
base_url = f"https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}"
data_pipeline_deployment_id = "16527423-18e5-42f8-93e7-4637637387b0"
ml_pipeline_deployment_id = "3d00768c-5a2e-4206-9e18-bee0e4660a81"
data_processing_flow_id = "9d8b4ebf-26f1-47d6-8ae8-9da1c281a433"
machine_learning_flow_id = "3570eaf9-1072-44b0-ad50-5d154775f90a"
data_processing_flow_run_id = "ce140598-326f-47be-bd4d-d57e9ebc13dc"
machine_learning_flow_run_id = "19b0b261-3229-4fc7-be36-be6bc62fbb8c"

# Set up the headers
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Function to get deployments
def get_deployment(deployment_id):
    url = f"{base_url}/deployments/{deployment_id}"
    response = requests.get(url, headers=headers)
    return response.json()

# Function to display deployment details
def display_deployment_info(deployment, pipeline_name):
    print("\n")
    print("-" * 20)
    print(f"Details about deployment for {pipeline_name}: \n")
    print(f" - Name of deployment: {deployment['name']}")
    print(f" - Flow ID: {deployment['flow_id']}")
    print(f" - Created at: {datetime.strptime(deployment['created'], '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%d-%m-%Y %H:%M:%S %Z')}")
    print(f" - Last Updated at: {datetime.strptime(deployment['updated'], '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%d-%m-%Y %H:%M:%S %Z')}")
    print(f" - Schedule: {deployment.get('schedule', {}).get('cron')}")
    print(f" - Tags: {deployment.get('tags', [])}")
    print("-" * 20)

def get_flow(flow_name):
    url = f"{base_url}/flows/{flow_name}"
    response = requests.get(url, headers=headers)
    flow_run = response.json()
    if flow_run:
        return flow_run
    else:
        return None

def display_flow_run_details(flow_run, pipeline_name):
    if flow_run:
        print(flow_run)
        print("\n")
        print("-" * 20)
        print(f" - Last successful flow for {pipeline_name}:")
        print(f" - Flow Run ID: {flow_run['id']}")
        print(f" - Flow Name: {flow_run['name']}")
        print(f" - Created at: {datetime.strptime(flow_run['created'], '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%d-%m-%Y %H:%M:%S %Z')}")
        print(f" - Updated at: {datetime.strptime(flow_run['updated'], '%Y-%m-%dT%H:%M:%S.%f%z').strftime('%d-%m-%Y %H:%M:%S %Z')}")
        print("-" * 20)
    else:
        print(f"No successful flow runs found for {pipeline_name}.")

def get_100_logs():
    url = f"{base_url}/logs/filter"
    data = {
        "offset": 0,
        "sort": "TIMESTAMP_ASC",
        "logs": {
            "operator": "and_",
            "flow_run_id": {
                "any_": [
                    data_processing_flow_run_id, machine_learning_flow_run_id
                ]
            }
        },
        "limit": 100
    }
    response = requests.post(url, headers=headers, json=data)
    flow_runs = response.json()
    if flow_runs:
        print(flow_runs)
    else:
        print('no logs found for the given run')

# Main function to fetch and display details
def main():
    while True:
        print("\nMenu:")
        print("1. Get DataOps pipeline deployment details")
        print("2. Get MLOps pipeline deployment details")
        print("3. Get DataOps flow details")
        print("4. Get MLOps flow details")
        print("5. Get 100 logs")
        print("6. Exit")

        choice = input("Enter your choice (1-6): ")

        if choice == "1":
            deployment = get_deployment(data_pipeline_deployment_id)
            display_deployment_info(deployment, 'DataOps')
        elif choice == "2":
            deployment = get_deployment(ml_pipeline_deployment_id)
            display_deployment_info(deployment, 'MLOps')
        elif choice == "3":
            flow_run = get_flow(data_processing_flow_id)
            display_flow_run_details(flow_run, "DataOps")
        elif choice == "4":
            flow_run = get_flow(machine_learning_flow_id)
            display_flow_run_details(flow_run, "MLOps")
        elif choice == "5":
            get_100_logs()
        elif choice == "6":
            print("Exiting program. Goodbye!")
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 6.")

if __name__ == "__main__":
    main()
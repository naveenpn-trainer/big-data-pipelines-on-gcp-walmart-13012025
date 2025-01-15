from google.cloud import bigquery


def create_dataset(project_id, dataset_name):
    bigquery_client = bigquery.Client(project=project_id)

    dataset_id = f"{project_id}.{dataset_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "ASIA-SOUTH1"

    new_dataset = bigquery_client.create_dataset(dataset, exists_ok=True)
    print(f"Dataset {new_dataset.dataset_id} created successfully.")


if __name__ == '__main__':
    create_dataset("red-splice-429501-q7", "demo_db04")

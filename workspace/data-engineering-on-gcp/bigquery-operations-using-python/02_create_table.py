from google.cloud import bigquery


def create_table(project_id, dataset_name, table_name):
    bigquery_client = bigquery.Client(project=project_id)

    dataset_id = f"{project_id}.{dataset_name}"
    table_id = f"{dataset_id}.{table_name}"  # Full table ID: project_id.dataset_name.table_name

    schema = [
        bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("dob", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("gender", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("country", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("region", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("city", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("asset", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("marital_status", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)

    new_table = bigquery_client.create_table(table, exists_ok=True)  # exists_ok=True avoids errors if the table exists
    print(f"Table {new_table.table_id} created successfully in dataset {dataset_name}.")


if __name__ == '__main__':
    project_id = "red-splice-429501-q7"
    dataset_name = "demo_db"
    table_name = "customers"

    create_table(project_id, dataset_name, table_name)

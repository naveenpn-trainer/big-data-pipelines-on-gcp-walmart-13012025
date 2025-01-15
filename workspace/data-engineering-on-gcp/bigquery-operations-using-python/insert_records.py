from google.cloud import bigquery


def insert_records(project_id, dataset_name, table_name):
    bigquery_client = bigquery.Client(project=project_id)

    table_id = f"{project_id}.{dataset_name}.{table_name}"

    rows_to_insert = [
        {
            "id": 1,
            "name": "Alice",
            "dob": "1990-05-15",
            "email": "alice@example.com",
            "gender": "Female",
            "country": "USA",
            "region": "North America",
            "city": "New York",
            "asset": 50000,
            "marital_status": "Single"
        },
        {
            "id": 2,
            "name": "Bob",
            "dob": "1985-10-20",
            "email": "bob@example.com",
            "gender": "Male",
            "country": "India",
            "region": "Asia",
            "city": "Mumbai",
            "asset": 75000,
            "marital_status": "Married"
        }
    ]

    errors = bigquery_client.insert_rows_json(table_id, rows_to_insert)

    if errors == []:
        print(f"Successfully inserted {len(rows_to_insert)} rows into table {table_name}.")
    else:
        print(f"Failed to insert rows: {errors}")


if __name__ == '__main__':
    project_id = "red-splice-429501-q7"
    dataset_name = "demo_db"
    table_name = "customers"

    insert_records(project_id, dataset_name, table_name)

from google.cloud import bigquery


def insert_records(project_id, dataset_name, table_name):
    bigquery_client = bigquery.Client(project=project_id)
    query_job = bigquery_client.query(f"""
                               SELECT country, count(*) as total_count 
                               FROM `{project_id}.{dataset_name}.{table_name}`
                               GROUP BY country
                               """)
    records = query_job.result()
    for record in records:
        print(f"Country: {record['country']}, Total Count: {record['total_count']}")


if __name__ == '__main__':
    project_id = "red-splice-429501-q7"
    dataset_name = "demo_db"
    table_name = "customers"

    insert_records(project_id, dataset_name, table_name)

import base64
import json
import pandas as pd
import io  # Import io for StringIO
from google.cloud import storage, bigquery

def process_pubsub(event, context):
    """Triggered by a Pub/Sub message when a file is uploaded."""
    # Decode Pub/Sub message
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message_data = json.loads(pubsub_message)
    file_name = message_data.get('name')  # Name of the uploaded file
    bucket_name = message_data.get('bucket')  # Name of the bucket

    if not file_name or not bucket_name:
        print("Error: Missing 'name' or 'bucket' in the Pub/Sub message.")
        return

    print(f"Processing file {file_name} from bucket {bucket_name}...")

    # Initialize Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Read the uploaded file
    data_content = blob.download_as_string()
    data = pd.read_csv(io.StringIO(data_content.decode('utf-8')))  # Corrected StringIO usage

    # Determine which file is being processed and handle accordingly
    dataset_id = "ecommerce"  # Replace with your BigQuery dataset ID

    if file_name == "customers.csv":
        table_id = "customers"
        save_to_bigquery(data, dataset_id, table_id)
    elif file_name == "orders.csv":
        print("Handling date columns in orders.csv...")
        data = handle_date_columns(data)
        table_id = "orders"
        save_to_bigquery(data, dataset_id, table_id)
    elif file_name == "products.csv":
        table_id = "products"
        save_to_bigquery(data, dataset_id, table_id)
    elif file_name == "order_details.csv":
        print("Performing transformations on order_details.csv...")
        transformed_data = transform_order_details(data)
        table_id = "transaction"
        save_to_bigquery(transformed_data, dataset_id, table_id)
    else:
        print(f"File {file_name} is not part of the ETL pipeline. Skipping.")

    print(f"Processing of {file_name} complete.")


def handle_date_columns(data):
    """Converts date columns in the orders.csv file to datetime format."""
    date_columns = ["Order Date", "Ship Date"]  # Define the date columns

    for col in date_columns:
        if col in data.columns:
            try:
                data[col] = pd.to_datetime(data[col], errors='coerce')
                print(f"Converted {col} to datetime.")
            except Exception as e:
                print(f"Error converting column {col} to datetime: {e}")

    return data


def transform_order_details(data):
    """Transforms order_details.csv with the required calculations."""
    
    # Add Order_Product_ID column
    data['Order_Product_ID'] = data['Order ID'].astype(str) + "-" + data['Product ID'].astype(str)
    
    # Calculate required order-level metrics
    transaction = data.groupby('Order ID').agg(
        Number_Products_in_Order=('Product ID', 'nunique'),
        Total_Order_Profit=('Profit', 'sum'),
        Total_Order_Sales=('Sales', 'sum'),
    ).reset_index()

    # Calculate product-level metrics
    product_metrics = data.groupby('Product ID').agg(
        Total_Product_Profit=('Profit', 'sum'),
        Total_Product_Quantity=('Quantity', 'sum'),
        Total_Product_Sales=('Sales', 'sum'),
        Average_Product_Discount=('Discount', 'mean'),
        Average_Product_Profit=('Profit', 'mean')
    ).reset_index()

    # Merge order-level metrics with product-level details
    transaction = transaction.merge(
        data[['Order ID', 'Product ID', 'Order_Product_ID']].drop_duplicates(), 
        on='Order ID', 
        how='left'
    )
    transaction = transaction.merge(product_metrics, on='Product ID', how='left')

    # Include original columns in the transaction
    transaction = transaction.merge(
        data[['Order_Product_ID', 'Profit', 'Quantity', 'Sales', 'Discount']].drop_duplicates(),
        on='Order_Product_ID',
        how='left'
    )

    return transaction




def save_to_bigquery(df, dataset_id, table_id):
    """Saves a Pandas DataFrame to BigQuery."""
    client = bigquery.Client()

    # Define the BigQuery table name
    table_ref = client.dataset(dataset_id).table(table_id)

    # Write the DataFrame to BigQuery
    job = client.load_table_from_dataframe(df, table_ref)
    job.result()  # Wait for the job to complete

    print(f"Data successfully written to BigQuery table {dataset_id}.{table_id}.")

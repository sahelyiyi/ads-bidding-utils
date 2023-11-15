import base64
import io
import pandas as pd
from minio import Minio
from minio.error import S3Error
from kubernetes import client, config


def get_minio_client_and_bucket():
    """
    Load MinIO configuration data from Kubernetes secrets and configmap and create the MinIO client.

    Returns:
    Tuple: A tuple containing MinIO client, the bucket.
    """
    try:
        # Load Kubernetes in-cluster configuration
        config.load_incluster_config()

        # Create a client instance
        v1 = client.CoreV1Api()

        # Access the MinIO secret data
        secret = v1.read_namespaced_secret(name='minio-secret', namespace='kubeflow')
        minio_access_key = secret.data['AWS_ACCESS_KEY_ID']
        minio_secret_key = secret.data['AWS_SECRET_ACCESS_KEY']
        # Decode base64
        minio_access_key = base64.b64decode(minio_access_key).decode('utf-8')
        minio_secret_key = base64.b64decode(minio_secret_key).decode('utf-8')

        # Access the ConfigMap data
        config_map = v1.read_namespaced_config_map(name='ads-bidding-configmap', namespace='kubeflow')
        minio_endpoint = config_map.data['MINIO_ENDPOINT']
        minio_bucket = config_map.data['MINIO_BUCKET']

    except Exception as e:
        # Handle exceptions, e.g., Kubernetes configuration error or missing resources
        raise RuntimeError(f"Error loading MinIO data: {e}")

    # Initialize MinIO client
    minio_client = Minio(
        minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
        secure=False  # Set to True for secure (HTTPS) connection
    )

    return minio_client, minio_bucket


def load_data_from_minio(minio_client, minio_bucket, minio_data_path, sep=';'):
    """
    Load data from MinIO.

    Parameters:
    - minio_client: MinIO client.
    - minio_bucket: MinIO bucket where the data is stored.
    - minio_data_path: Path to the data within the MinIO bucket.
    - sep (str, optional): Delimiter used in the CSV file. Default is ';'.

    Returns:
    - pd.DataFrame: Loaded data.
    """

    try:
        # Download data from MinIO
        data = minio_client.get_object(minio_bucket, minio_data_path)
        df = pd.read_csv(data, sep=sep)

        return df
    except S3Error as e:
        print(f"Error loading data from MinIO: {e}")
        return None


def save_model_to_minio(model, minio_client, minio_bucket, minio_data_path, temp_data_path='temp_model.h5'):
    """
    Save model to MinIO.

    Parameters:
    - model: keras model to be saved.
    - minio_client: MinIO client.
    - minio_bucket: MinIO bucket where the data will be stored.
    - minio_data_path: Path to save the data within the MinIO bucket.
    """
    model.save(temp_data_path)

    # Upload the model file to Minio
    try:
        minio_client.fput_object(
            bucket_name=minio_bucket,
            object_name=minio_data_path,
            file_path=temp_data_path
        )
        print(f"Data saved to MinIO: {minio_bucket}/{minio_data_path}")
    except S3Error as e:
        print(f"Error saving data to MinIO: {e}")


def save_file_to_minio(minio_client, minio_bucket, minio_data_path, local_data_path):
    """
    Save model to MinIO.

    Parameters:
    - minio_client: MinIO client.
    - minio_bucket: MinIO bucket where the data will be stored.
    - minio_data_path: Path to save the data within the MinIO bucket.
    - local_data_path: Local data to be saved to MinIO.
    """

    # Upload the model file to Minio
    try:
        minio_client.fput_object(
            bucket_name=minio_bucket,
            object_name=minio_data_path,
            file_path=local_data_path
        )
        print(f"Data saved to MinIO: {minio_bucket}/{minio_data_path}")
    except S3Error as e:
        print(f"Error saving data to MinIO: {e}")


def load_model_from_minio(minio_client, minio_bucket, minio_data_path, local_model_file_path='temp_model.h5'):
    """
    Load data from MinIO.

    Parameters:
    - minio_client: MinIO client.
    - minio_bucket: MinIO bucket where the data is stored.
    - minio_data_path: Path to the keras model within the MinIO bucket.

    Returns:
    - str: The local model file path.
    """

    try:
        # Download the Keras model from MinIO
        minio_client.fget_object(minio_bucket, minio_data_path, local_model_file_path)
        return local_model_file_path

    except S3Error as e:
        print(f"Error loading data from MinIO: {e}")
        return None


def stream_csv_object_to_fixed_sized_dataframes(minio_client, minio_bucket, object_name, chunk_size=5000000, df_chunk_size=5000, sep=';'):
    """
    Stream a large CSV object from Minio in chunks and concatenate them into a Pandas DataFrame.

    Parameters:
    - minio_client (Minio): An initialized Minio client.
    - minio_bucket (str): Name of the Minio bucket containing the CSV object.
    - object_name (str): Name of the CSV object to stream.
    - chunk_size (int): Size of each chunk to read from the object.
    - sep (str, optional): Delimiter used in the CSV file. Default is ';'.


    Returns:
    - Generator of Pandas DataFrames: Yields Pandas DataFrames for each chunk of the CSV object.
    """

    try:
        # Get the object metadata to determine its size
        object_info = minio_client.stat_object(minio_bucket, object_name)
        object_size = object_info.size

        # Initialize variables to handle incomplete rows
        remaining_line = ""
        remaining_df = None

        columns = None
        # Iterate over chunks of the CSV object
        for offset in range(0, object_size, chunk_size):
            length = min(chunk_size, object_size - offset)
            data = minio_client.get_object(minio_bucket, object_name, offset=offset, length=length)

            # Read the chunk into a Pandas DataFrame
            chunk_data = data.read().decode('utf-8')
            lines = (remaining_line + chunk_data).split('\n')

            remaining_line = lines[-1]

            header = 'infer'
            if offset != 0:
                header = None

            # df_chunk = pd.read_csv(io.StringIO('\n'.join(lines[:-1])), sep=sep, index_col=0, header=header)
            df_chunk = pd.read_csv(io.StringIO('\n'.join(lines[:-1])), sep=sep, header=header)

            # Handle column names for the second chunks onwards
            if offset == 0:
                columns = df_chunk.columns.values
            else:
                df_chunk.columns = columns

            if remaining_df is not None:
                df_chunk = pd.concat([remaining_df, df_chunk])
                remaining_df = None

            df_offset = 0
            for df_offset in range(0, len(df_chunk)-df_chunk_size+1, df_chunk_size):
                yield df_chunk[df_offset:df_offset+df_chunk_size]

            if len(df_chunk) < df_chunk_size:
                remaining_df = df_chunk
            else:
                remaining_df = df_chunk[df_offset+df_chunk_size:]

        for df_offset in range(0, len(remaining_df), df_chunk_size):
            yield remaining_df[df_offset:df_offset + df_chunk_size]

    except Exception as e:
        print(f"Error streaming data from MinIO: {e}")
        return None

















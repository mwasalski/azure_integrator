# azure_integrator

A small Python wrapper around [Azure Blob Storage](https://learn.microsoft.com/azure/storage/blobs/)
for everyday data work: list containers and blobs, create and delete containers,
and turn Parquet blobs into pandas DataFrames or local Excel files.

One `AzureBlobStorage` instance is bound to one container (the "default"
container), but account-level operations and one-off reads from other containers
are available too.

## Installation

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Only `azure-storage-blob` and `python-dotenv` are needed for the core class.
`pandas`, `pyarrow` and `openpyxl` are imported lazily and only required by
`parquet_to_df()` and `df_to_excel()`.

## Configuration

Copy `.env.example` to `.env` and fill in your storage account details:

```env
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
AZURE_STORAGE_CONTAINER_NAME="my-container"
```

The connection string is in the Azure Portal under
**Storage account → Security + networking → Access keys**.

`.env` is listed in `.gitignore` — keep it that way, a connection string grants
full access to the storage account.

Both values can also be passed directly to the constructor, which takes
precedence over the environment:

```python
storage = AzureBlobStorage(connection_string="...", container_name="my-container")
```

## Usage

```python
from azure_integrator import AzureBlobStorage

storage = AzureBlobStorage()          # reads .env

storage.list_containers()             # ['my-container', 'archive']
storage.blob_list()                   # every blob in my-container
storage.blob_list(prefix="sales/")    # only blobs under sales/

# Read one Parquet blob into a DataFrame
df = storage.parquet_to_df("sales/2025.parquet")

# Convert every Parquet blob in the container into .xlsx files
storage.df_to_excel(output_dir="exports", prefix="sales/")

# Raw bytes of any blob
raw = storage.download_bytes("notes.txt")
```

Running the module directly prints the containers and the blobs in the default
container — a quick way to check that your `.env` works:

```bash
python azure_integrator.py
```

## API

| Method | Description |
| --- | --- |
| `AzureBlobStorage(connection_string=None, container_name=None)` | Connect to the account and bind to a container. Raises `ValueError` if a setting is missing, `ConnectionError` if Azure rejects the connection string. |
| `blob_list(prefix=None, verbose=True)` | Blob names in the default container, optionally filtered by prefix. Returns `[]` and prints the error if the listing fails. |
| `download_bytes(blob_name, container_name=None)` | Blob contents as `bytes`. Raises `FileNotFoundError` if the blob is missing. |
| `list_containers(verbose=True)` | All container names in the storage account. |
| `create_container(new_container)` | Create a container, or return the existing one if the name is taken. Returns a `ContainerClient`. |
| `delete_container(container_to_delete)` | Delete a container and its contents. Returns `True`, or `False` if it did not exist. |
| `parquet_to_df(blob_name, container_name=None)` | Read a Parquet blob into a `pandas.DataFrame`. |
| `df_to_excel(container_name=None, output_dir=".", prefix=None)` | Write every Parquet blob in the container to a local `.xlsx`. Non-Parquet blobs are skipped; `/` in blob names becomes `_` in file names. Returns the paths written. |

`verbose=True` keeps the printing behaviour of the original script; pass
`verbose=False` when using these methods inside other code.

### A note on `delete_container`

Deletion is immediate and takes every blob in the container with it. Azure also
blocks re-creating a container under the same name for up to 30 seconds
afterwards. If you delete the instance's own default container, the instance
warns you and you should build a new `AzureBlobStorage`.

## Requirements

- Python 3.9+ (uses `from __future__ import annotations` for the type hints)
- An Azure Storage account

## License

GPL-3.0 — see [LICENSE](LICENSE).

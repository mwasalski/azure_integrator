"""Small, dependency-light wrapper around Azure Blob Storage.

Credentials are read from the constructor arguments first, then from the
environment (a local ``.env`` file is loaded automatically):

    AZURE_STORAGE_CONNECTION_STRING   connection string of the storage account
    AZURE_STORAGE_CONTAINER_NAME      default container to work with

Example:
    >>> storage = AzureBlobStorage()
    >>> storage.list_containers()
    >>> storage.blob_list()
"""

from __future__ import annotations

import io
import os
import re
from pathlib import Path

from azure.core.exceptions import AzureError, ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


class AzureBlobStorage:
    """Connection to a single container in an Azure Storage account."""

    def __init__(self, connection_string: str | None = None, container_name: str | None = None):
        """Initialize the connection to Azure Blob Storage.

        Args:
            connection_string: Azure storage connection string. Falls back to
                ``AZURE_STORAGE_CONNECTION_STRING``.
            container_name: Name of the default container. Falls back to
                ``AZURE_STORAGE_CONTAINER_NAME``.

        Raises:
            ValueError: One of the two settings is missing.
            ConnectionError: The connection string was rejected by Azure.
        """
        load_dotenv()

        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        # ``container_name`` (lowercase) is kept for backwards compatibility with
        # older .env files that used that name.
        self.container_name = (
            container_name
            or os.getenv("AZURE_STORAGE_CONTAINER_NAME")
            or os.getenv("container_name")
        )

        # Validate *before* touching the SDK, otherwise the user gets an opaque
        # SDK error instead of a message telling them what to fix.
        if not self.connection_string:
            raise ValueError(
                "Missing connection string. Pass it as an argument or set "
                "AZURE_STORAGE_CONNECTION_STRING in your .env file."
            )
        if not self.container_name:
            raise ValueError(
                "Missing container name. Pass it as an argument or set "
                "AZURE_STORAGE_CONTAINER_NAME in your .env file."
            )

        # blob_service_client  -> account level (list/create/delete containers)
        # container_client     -> the single container this instance works on
        try:
            self.blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            self.container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
        except (AzureError, ValueError) as exc:
            raise ConnectionError(f"Failed to connect to Azure Storage: {exc}") from exc

    # ------------------------------------------------------------------ blobs

    def blob_list(self, prefix: str | None = None, verbose: bool = True) -> list[str]:
        """List blob names in the current container.

        Args:
            prefix: Only return blobs whose name starts with this prefix.
            verbose: Print the names as well as returning them.

        Returns:
            The blob names, or an empty list if the listing failed.
        """
        try:
            blob_names = [blob.name for blob in self.container_client.list_blobs(name_starts_with=prefix)]
        except AzureError as exc:
            print(f"Error listing blobs: {exc}")
            return []

        if verbose:
            print(f"\nFiles in {self.container_name} container:")
            for name in blob_names:
                print(f"- {name}")

        return blob_names

    def download_bytes(self, blob_name: str, container_name: str | None = None) -> bytes:
        """Download a single blob into memory.

        Args:
            blob_name: Name of the blob.
            container_name: Container to read from; defaults to the current one.

        Raises:
            FileNotFoundError: The blob does not exist.
        """
        container = self._container(container_name)
        try:
            return container.download_blob(blob_name).readall()
        except ResourceNotFoundError as exc:
            raise FileNotFoundError(
                f"Blob '{blob_name}' not found in container '{container.container_name}'."
            ) from exc

    # ------------------------------------------------------------- containers

    def list_containers(self, verbose: bool = True) -> list[str]:
        """Return the names of all containers in the storage account."""
        container_names = [c.name for c in self.blob_service_client.list_containers()]
        if verbose:
            print(f"Available containers: {container_names}")
        return container_names

    def create_container(self, new_container: str):
        """Create a container if it does not exist yet.

        The instance keeps working on its own container -- the new one is only
        returned, never swapped in.

        Args:
            new_container: Name of the container to create.

        Returns:
            ContainerClient: Client for ``new_container``.
        """
        try:
            return self.blob_service_client.create_container(new_container)
        except ResourceExistsError:
            print(f"Container {new_container} already exists")
            return self.blob_service_client.get_container_client(new_container)

    def delete_container(self, container_to_delete: str) -> bool:
        """Delete a container and everything in it.

        Args:
            container_to_delete: Name of the container to delete.

        Returns:
            True if it was deleted, False if it did not exist.
        """
        try:
            self.blob_service_client.delete_container(container_to_delete)
        except ResourceNotFoundError:
            print(f"Container {container_to_delete} does not exist")
            return False

        if container_to_delete == self.container_name:
            print(
                f"Warning: {container_to_delete} was this instance's default container; "
                "create a new AzureBlobStorage to keep working."
            )
        return True

    # ---------------------------------------------------------------- pandas

    def parquet_to_df(self, blob_name: str, container_name: str | None = None):
        """Read a Parquet blob into a pandas DataFrame.

        Args:
            blob_name: Name of the Parquet blob.
            container_name: Container to read from; defaults to the current one.

        Returns:
            pandas.DataFrame: The blob's contents.
        """
        import pandas as pd  # optional dependency, imported on demand

        raw = self.download_bytes(blob_name, container_name)
        return pd.read_parquet(io.BytesIO(raw))

    def df_to_excel(
        self,
        container_name: str | None = None,
        output_dir: str | Path = ".",
        prefix: str | None = None,
    ) -> list[Path]:
        """Convert every Parquet blob in a container into a local ``.xlsx`` file.

        Args:
            container_name: Container to read from; defaults to the current one.
            output_dir: Directory the Excel files are written to (created if needed).
            prefix: Only convert blobs whose name starts with this prefix.

        Returns:
            Paths of the files that were written.
        """
        container = self._container(container_name)
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        written: list[Path] = []
        for blob in container.list_blobs(name_starts_with=prefix):
            if not blob.name.lower().endswith(".parquet"):
                continue

            df = self.parquet_to_df(blob.name, container_name)
            # Blob names may contain "/" -- flatten them into a safe file name.
            stem = re.sub(r"[\\/]+", "_", blob.name[: -len(".parquet")])
            target = output_dir / f"{stem}.xlsx"
            df.to_excel(target, index=False)
            written.append(target)
            print(f"Saved {blob.name} -> {target}")

        if not written:
            print(f"No .parquet files found in container '{container.container_name}'")
        return written

    # --------------------------------------------------------------- internal

    def _container(self, container_name: str | None):
        """Return the client for ``container_name``, or the default one."""
        if container_name is None or container_name == self.container_name:
            return self.container_client
        return self.blob_service_client.get_container_client(container_name)


if __name__ == "__main__":
    storage = AzureBlobStorage()
    storage.list_containers()
    storage.blob_list()

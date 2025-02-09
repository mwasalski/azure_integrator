# Final version
class AzureBlobStorage:
    def __init__(self, connection_string=None, container_name=None):
        """
        Initializes connection to Azure Blob Storage
        Args:
            connection_string (str, optional): Azure storage connection string, recommended to use .env file
            container_name (str, optional): Container name, recommended to use .env file
        """
        # Importing libraries
        from azure.storage.blob import BlobServiceClient as bsc
        import os
        from dotenv import load_dotenv
        

        # Przypisanie bibliotek do self, żeby można było używać ich w funkcjach
        self.bsc = bsc
        self.os = os
        
        # Load environment variables
        load_dotenv()  # Nie ma potrzeby przypisywania do self

        # Pobranie danych połączenia
        # Można podać dane w parameters albo użyć .env file
        self.connection_string = connection_string or os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        self.container_name = container_name or os.getenv('container_name')

        
        # Tworzy klienta (połączenie) do Azure Storage Account używając connection string
        # Jest to główny punkt dostępu do naszego konta w Azure Storage
        # Pozwala na:
        # Przeglądanie wszystkich kontenerów
        # Tworzenie nowych kontenerów
        # Zarządzanie uprawnieniami
        # Wykonywanie operacji na poziomie całego konta
        self.blob_service_client = self.bsc.from_connection_string(self.connection_string)
        
        # Tworzy klienta (połączenie) do konkretnego kontenera w Azure Storage
        # Pozwala na:
        # Przeglądanie wszystkich obiektów (blobs) w kontenerze
        # Tworzenie nowych obiektów
        # Zarządzanie uprawnieniami
        # Wykonywanie operacji na poziomie konkretnego kontenera
        self.container_client = self.blob_service_client.get_container_client(self.container_name)
    
        # Walidacja danych
        # Jeżeli coś jest nie tak z connection string albo container name to zwraca błąd
        if not self.connection_string: 
            raise ValueError("You are missing connection string. Check .env file or parameters if you didn't provide it")
        elif not self.container_name:
            raise ValueError("You are missing container name. Check .env file or parameters if you didn't provide it")


        # Próba otwarcia drzwi (try)
        # Jeśli coś pójdzie nie tak (except):
        # Klucz nie pasuje
        # Drzwi są zablokowane
        # Brak uprawnień
        # Informujemy użytkownika o problemie (raise) z dokładnym opisem co się stało
        
        # Jest to mechanizm zabezpieczający, który:
        # Wyłapuje wszystkie możliwe błędy podczas łączenia
        # Informuje użytkownika w czytelny sposób co poszło nie tak
        # Pozwala na odpowiednią reakcję na błędy
        try:
            # Próba połączenia z Azure Storage
            self.blob_service_client = self.bsc.from_connection_string(self.connection_string)
            self.container_client = self.blob_service_client.get_container_client(self.container_name)
        except Exception as e:
            # Jeśli połączenie się nie powiedzie, zgłoś błąd z informacją co poszło nie tak
            # str(e) pokazuje szczegóły oryginalnego błędu
            raise ConnectionError(f"Failed to connect to Azure Storage: {str(e)}")

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   # Listing blobs 
    
    def blob_list(self):
        """
        Lists all blobs in the container
        Returns:
            list: List of blob names in the container
        """
        try:
            blobs = self.container_client.list_blobs()
            blob_names = [blob.name for blob in blobs]
            
            # Print blobs
            print(f"\nFiles in {self.container_name} container:")
            for name in blob_names:
                print(f"- {name}")
                
            return blob_names
            
        except Exception as e:
            print(f"Error listing blobs: {str(e)}")
            return []
        
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   # Creating container
    def create_container(self, new_container):
        """
        Creates a new container in Azure Blob Storage
        Returns:
            str: Message indicating the container was created successfully
        """
        self.containers = self.blob_service_client.list_containers()
        self.containers_list = []
        for self.container in self.containers:
            self.containers_list.append(self.container.name)
            
        if new_container not in self.containers_list:
            self.container_client = self.blob_service_client.create_container(new_container)
        else:
            print(f"Container {new_container} already exists")
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   # Delete container
    def delete_container(self, container_to_delete):

        """
        Deletes a container in Azure Blob Storage
        Args:
            container_name (str): Name of the container to delete
        """
        self.container_client = self.blob_service_client.delete_container(container_to_delete)
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   # Create dataframes from parquet files
    def parquet_to_df(self, blob_name):
        """
        """
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   # Create excel files from dataframes
    def df_to_excel(self, container_name = None):

        blob_list = self.container_client.list_blobs()
        for blob in blob_list:
            df = self.bsc.load_blob_to_dataframe(self.connection_string, container_name)
            df.to_excel(blob.name)
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   # List containers
    def list_containers(self):
        self.containers = self.blob_service_client.list_containers()
        self.containers_list = []
        for self.container in self.containers:
            self.containers_list.append(self.container.name)
        
        print(f"Available containers: {self.containers_list}") 
# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------




# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------
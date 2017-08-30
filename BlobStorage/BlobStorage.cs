using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace BlobStorage 
{
    public class BlobStorage
    {

        // TODO:
        // METADATA paa blob (https://docs.microsoft.com/en-us/azure/storage/blobs/storage-properties-metadata)


        private string _azureBlobConnectionString;
        private CloudStorageAccount _storageAccount;
        private CloudBlobClient _blobClient;

        public BlobStorage()
        {
            _azureBlobConnectionString = "StorageConnectionString";
            _storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting(_azureBlobConnectionString));
            _blobClient = _storageAccount.CreateCloudBlobClient();
        }


        public async Task<CloudBlobContainer> SetUpBlobContainerAsync(string containerName)
        {
            CloudBlobContainer container = _blobClient.GetContainerReference(containerName);

            try
            {
                // The call below will fail if the sample is configured to use the storage emulator in the connection string, but 
                // the emulator is not running.
                // Change the retry policy for this call so that if it fails, it fails quickly.
                BlobRequestOptions requestOptions = new BlobRequestOptions() { RetryPolicy = new NoRetry() };
                await container.CreateIfNotExistsAsync(requestOptions, null);
            }
            catch (StorageException e)
            {
                Console.WriteLine(
                    "If you are running with the default connection string, please make sure you have started the storage emulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            return container;
        }



        public async Task AddFileToContainerAsync(CloudBlobContainer cloudBlobContainer, FileInfo fileInfo, string mimeType)
        {
            CloudBlockBlob blockBlob = cloudBlobContainer.GetBlockBlobReference(fileInfo.Name);

            // Set the blob's content type so that the browser knows to treat it as an image.
            blockBlob.Properties.ContentType = mimeType;
            await blockBlob.UploadFromFileAsync(fileInfo.FullName);
        }


        public void GetContainerContent(CloudBlobContainer cloudBlobContainer)
        {
            // TODO: trenger vi denne
            foreach (CloudBlockBlob blob in cloudBlobContainer.ListBlobs())
            {
                Console.WriteLine("- {0} (type: {1})", blob.Uri, blob.GetType());
            }
        }

        public IEnumerable<IListBlobItem> GetCloudBlockBlobsForContainer(CloudBlobContainer cloudBlobContainer)
        {
            return cloudBlobContainer.ListBlobs();
        }



        public void AddOrUpdateContainerMetadata(CloudBlobContainer cloudBlobContainer, string tag, string value)
        {
            cloudBlobContainer.Metadata.Add(tag, value);
        }




        public CloudBlobContainer GetContainerFromContainerName(string containerName)
        {
            return _blobClient.GetContainerReference(containerName);
        }

        public void GetAllContainers()
        {
            // TODO: trenger vi denne
            foreach (CloudBlobContainer container in _blobClient.ListContainers())
            {
                Console.WriteLine("- {0} (name: {1})", container.Uri, container.Name);
            }
        }


        public async Task DownloadFileFromBlobStorageAsync(CloudBlockBlob cloudBlockBlob, string filePathForUpload)
        {
            await cloudBlockBlob.DownloadToFileAsync(filePathForUpload, FileMode.Create);
        }



        public async Task DeleteContainer(string containerName)
        {
            await DeleteContainer(GetContainerFromContainerName(containerName));
        }

        public async Task DeleteContainer(CloudBlobContainer cloudBlobContainer)
        {
            await cloudBlobContainer.DeleteIfExistsAsync();
        }

    }

}
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Core.Cryptography;
using Azure.Identity;
using Azure.Security.KeyVault.Keys.Cryptography;
using Azure.Security.KeyVault.Keys;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Blobs;
using Azure.Storage;

namespace TMSDWFileEncryptionDecryptionFunc
{
    public static class EncryptFullParquetFile
    {
        [FunctionName("EncryptFullParquetFile")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("EncryptFullParquetFile  Function Started");


            string responseMessage = "";
            try
            {

                string saConnectionString = "DefaultEndpointsProtocol=https;AccountName=tmsdwcryptosa;AccountKey=csEODN4sWDfiPP0YYAxGnUF7Qb1SNkB4yjyOFp7R40ekQTPWmyPyZRKxaHfQrYU4P68fHlRRU4vm+AStXJtVhw==;EndpointSuffix=core.windows.net";
                string blobContainerName = "parquetfiles";
                //string inputBlobName = string.Empty;
                string inputBlobName = "plainfiles/userdata1.parquet";
                //string encryptedBlobName = String.Empty;
                string encryptedBlobName = "FullFileEncryptedOut/fullencrypteduserdata.parquet";


                string keyVaultUri = "https://tms-encryption-kv.vault.azure.net";
                string keyVaultKeyName = "tms-file-encryption-key";
                var keyClient = new KeyClient(new Uri(keyVaultUri), new DefaultAzureCredential());
                KeyVaultKey kvkey = await keyClient.GetKeyAsync(keyVaultKeyName);

                var cryptoClient = new CryptographyClient(kvkey.Id, new DefaultAzureCredential());

                BlobClient inputBlobClient = new BlobServiceClient(saConnectionString).GetBlobContainerClient(blobContainerName).GetBlobClient(inputBlobName);
                // Download and decrypt the encrypted contents from the blob.
                MemoryStream outputStream = new MemoryStream();
                inputBlobClient.DownloadTo(outputStream);
                outputStream.Position = 0;
                Stream content = outputStream;
                //await inputBlobClient.DeleteIfExistsAsync();
                #region encrypt

                // Your key and key resolver instances, either through Azure Key Vault SDK or an external implementation.
                IKeyEncryptionKey key = cryptoClient;
                IKeyEncryptionKeyResolver keyResolver = new KeyResolver(new DefaultAzureCredential());


                // Create the encryption options to be used for upload and download.
                ClientSideEncryptionOptions encryptionOptions = new ClientSideEncryptionOptions(ClientSideEncryptionVersion.V2_0)
                {
                    KeyEncryptionKey = key,
                    KeyResolver = keyResolver,
                    // String value that the client library will use when calling IKeyEncryptionKey.WrapKey()
                    KeyWrapAlgorithm = "RSA-OAEP"
                };

                // Set the encryption options on the client options.
                BlobClientOptions options = new SpecializedBlobClientOptions() { ClientSideEncryption = encryptionOptions };

                // Create blob client with client-side encryption enabled.
                // Client-side encryption options are passed from service clients to container clients, 
                // and from container clients to blob clients.
                // Attempting to construct a BlockBlobClient, PageBlobClient, or AppendBlobClient from a BlobContainerClient
                // with client-side encryption options present will throw, as this functionality is only supported with BlobClient.
                BlobClient blob = new BlobServiceClient(saConnectionString, options).GetBlobContainerClient(blobContainerName).GetBlobClient(encryptedBlobName);

                // Upload the encrypted contents to the blob.
                blob.Upload(content);


                #endregion


                responseMessage = "Encryption successfull";

            }
            catch (Exception ex)
            {
                responseMessage = ex.Message;
            }
            log.LogInformation("EncryptFullParquetFile  Function Ended");
            return new OkObjectResult(responseMessage);
        }
    }
}

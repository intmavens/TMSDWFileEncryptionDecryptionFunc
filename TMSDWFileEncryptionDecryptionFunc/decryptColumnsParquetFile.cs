using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Data.Encryption.Cryptography;
using System.Collections.Generic;
using Azure.Storage.Blobs;

namespace TMSDWFileEncryptionDecryptionFunc
{
    public static class decryptColumnsParquetFile
    {
        [FunctionName("decryptColumnsParquetFile")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string responseMessage = "Success";
            try
            {
                string saConnectionString = "DefaultEndpointsProtocol=https;AccountName=tmsdwcryptosa;AccountKey=csEODN4sWDfiPP0YYAxGnUF7Qb1SNkB4yjyOFp7R40ekQTPWmyPyZRKxaHfQrYU4P68fHlRRU4vm+AStXJtVhw==;EndpointSuffix=core.windows.net";
                string blobContainerName = "parquetfiles";
                string inputBlobName = "encryptedfiles/encrypteduserdata1.parquet";
                string decryptedBlobName = "decryptedfiles/decrypteduserdata1.parquet";
                string keyObjPath = @"C:\Users\Administrator\Desktop\temp\protectedDataEncryptionKeyValue.key";

                log.LogInformation("decryptColumnsParquetFile Function Started");

                string strRequestBody = await new StreamReader(req.Body).ReadToEndAsync();
                if (!String.IsNullOrEmpty(strRequestBody))
                {
                    ColumnCryptoghapherRequest columnCryptoghapherRequest;
                    Dictionary<string, string> columnsToEncryptOrDecrypt = new Dictionary<string, string>();
                    try
                    {
                        columnCryptoghapherRequest = JsonConvert.DeserializeObject<ColumnCryptoghapherRequest>(strRequestBody);
                        if (columnCryptoghapherRequest != null &&
                            !string.IsNullOrEmpty(columnCryptoghapherRequest.InputBlob) &&
                            !string.IsNullOrEmpty(columnCryptoghapherRequest.OutputBlob) &&
                            columnCryptoghapherRequest.columnsToEncryptOrDecrypt != null)
                        {
                            columnsToEncryptOrDecrypt = columnCryptoghapherRequest.columnsToEncryptOrDecrypt;
                            inputBlobName = columnCryptoghapherRequest.InputBlob;
                            decryptedBlobName = columnCryptoghapherRequest.OutputBlob;
                        }
                    }
                    catch (Exception exp)
                    {
                        throw new Exception("Request Body is not a valid JSON" + exp.Message);
                    }
                }
                else
                {
                    throw new Exception("Request Body is null");
                }
                //Get the Encryption Key
                ProtectedDataEncryptionKey encryptionKey = ColumnCryptoghrapherHelper.getEncyryptionKey(keyObjPath);

                Stream inputStream;
                //Read blob content
                try
                {
                    inputStream = ColumnCryptoghrapherHelper.getBlobContentToStream(saConnectionString, blobContainerName, inputBlobName);
                }
                catch (Exception exp)
                {
                    throw new Exception("Could not read Blob Content. " + exp.Message);
                }
                MemoryStream decryptedMemoryStream = ColumnCryptoghrapherHelper.decryptColumnsParquetFile(inputStream, encryptionKey);
                //Write Encrypted output to Blob
                BlobClient blobClient = new BlobServiceClient(saConnectionString).GetBlobContainerClient(blobContainerName).GetBlobClient(decryptedBlobName);
                blobClient.Upload(decryptedMemoryStream);

                log.LogInformation("decryptColumnsParquetFile Function Ended");
            }
            catch (Exception exp)
            {
                responseMessage = exp.Message;
            }
            return new OkObjectResult(responseMessage);
        }
    }
}

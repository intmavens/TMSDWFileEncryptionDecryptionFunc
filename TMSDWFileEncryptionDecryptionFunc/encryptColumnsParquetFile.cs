using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Xml.Linq;
using System.Collections.Generic;
using Azure.Storage.Blobs;
using Microsoft.Data.Encryption.Cryptography;
using System.Web.Http;
using Azure.Core;
using System.Net;
using System.Net.Http;
using System.IO.Compression;

namespace TMSDWFileEncryptionDecryptionFunc
{
    public static class ColumnCryptoghrapher
    {
        [FunctionName("encryptColumnsParquetFile")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            string responseMessage = "Success";
            try
            {
                string saConnectionString = "DefaultEndpointsProtocol=https;AccountName=tmsdwcryptosa;AccountKey=csEODN4sWDfiPP0YYAxGnUF7Qb1SNkB4yjyOFp7R40ekQTPWmyPyZRKxaHfQrYU4P68fHlRRU4vm+AStXJtVhw==;EndpointSuffix=core.windows.net";
                string blobContainerName = "parquetfiles";
                //string inputBlobName = string.Empty;
                string inputBlobName = "plainfiles/userdata1.parquet";
                //string encryptedBlobName = String.Empty;
                string encryptedBlobName = "encryptedfiles/encrypteduserdata1.parquet";
                string keyObjPath = @"C:\Users\Administrator\Desktop\temp\protectedDataEncryptionKeyValue.key";

                log.LogInformation("encryptColumnsParquetFile Function Started");
                string strRequestBody = await new StreamReader(req.Body).ReadToEndAsync();
                if (!String.IsNullOrEmpty(strRequestBody))
                {
                    //ColumnCryptoghapherRequest columnCryptoghapherRequest;
                    //Dictionary<string, string> columnsToEncryptOrDecrypt = new Dictionary<string, string>();
                    //try
                    //{
                    //    columnCryptoghapherRequest = JsonConvert.DeserializeObject<ColumnCryptoghapherRequest>(strRequestBody);
                    //    if (columnCryptoghapherRequest != null &&
                    //        !string.IsNullOrEmpty(columnCryptoghapherRequest.InputBlob) &&
                    //        !string.IsNullOrEmpty(columnCryptoghapherRequest.OutputBlob) &&
                    //        columnCryptoghapherRequest.columnsToEncryptOrDecrypt != null)
                    //    {
                    //        //columnsToEncryptOrDecrypt = columnCryptoghapherRequest.columnsToEncryptOrDecrypt;
                    //        //inputBlobName = columnCryptoghapherRequest.InputBlob;
                    //        //encryptedBlobName = columnCryptoghapherRequest.OutputBlob;
                    //    }
                    //}
                    //catch (Exception exp)
                    //{
                    //    throw new Exception("Request Body is not a valid JSON" + exp.Message);
                    //}
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
                MemoryStream encryptedMemoryStream = ColumnCryptoghrapherHelper.encryptColumnsParquetFile(inputStream, encryptionKey, saConnectionString, blobContainerName, encryptedBlobName);
                //encryptedMemoryStream.Position = 0;


                log.LogInformation("encryptColumnsParquetFile Function Ended");
            }
            catch (Exception exp)
            {
                //return StatusCode((int)HttpStatusCode.InternalServerError, exp.Message);    
                return new ExceptionResult(exp, true);
                //return new InternalServerErrorResult();
            }
            return new OkObjectResult(responseMessage);
        }
    }

}

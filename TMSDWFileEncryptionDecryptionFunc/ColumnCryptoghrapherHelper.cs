using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Encryption.Cryptography;
using Microsoft.Data.Encryption.Cryptography.Serializers;
using Microsoft.Data.Encryption.FileEncryption;
using Azure.Identity;
using Microsoft.Data.Encryption.AzureKeyVaultProvider;
using System.IO;
using Azure.Storage.Blobs;
using Parquet;
using Azure.Storage.Blobs.Models;
using System.Web;
using System.IO.Pipes;
using Microsoft.AspNetCore.Hosting.Server;
//using MimeMapping;

namespace TMSDWFileEncryptionDecryptionFunc
{
    internal class ColumnCryptoghrapherHelper
    {
        public static MemoryStream encryptColumnsParquetFile(Stream inputStream, ProtectedDataEncryptionKey encryptionKey, string saConnectionString, string blobContainerName, string encryptedBlobName)
        {
            //Stream testFileStream = File.OpenWrite(@"C:\Users\Administrator\Desktop\TestEncrypted.parquet");

            //MemoryStream encryptedMemoryStream = new MemoryStream();
            //Stream encryptedMemoryStream;
            //FileStream encryptedMemoryStream = new FileStream();

            try
            {
                // Create reader
                using ParquetFileReader reader = new ParquetFileReader(inputStream);

                //Get Headers, in Dictionary object
                Dictionary<string, int> keyValuePairsHeaders = new Dictionary<string, int>();
                keyValuePairsHeaders = reader.Read().Last().ToDictionary(row => row.Name, row => row.Index);

                // Copy source settings as target settings
                List<FileEncryptionSettings> writerSettings = reader.FileEncryptionSettings.Select(s => Copy(s)).ToList();

                // Modify a few column settings
                writerSettings[4] = new FileEncryptionSettings<string>(encryptionKey, EncryptionType.Randomized, StandardSerializerFactory.Default.GetDefaultSerializer<string?>());
                // Modify a few column settings
                //writerSettings[0] = new FileEncryptionSettings<DateTimeOffset?>(encryptionKey, SqlSerializerFactory.Default.GetDefaultSerializer<DateTimeOffset?>());
                //writerSettings[3] = new FileEncryptionSettings<string>(encryptionKey, EncryptionType.Deterministic, new SqlVarCharSerializer(size: 255));
                //writerSettings[10] = new FileEncryptionSettings<double?>(encryptionKey, StandardSerializerFactory.Default.GetDefaultSerializer<double?>());

                using (var encryptedMemoryStream = new FileStream(@"C:\\Prashant\FileEncryptionDecryption_ParquetFile\test.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite))
                {

                    // Create and pass the target settings to the writer
                    using ParquetFileWriter writer = new ParquetFileWriter(encryptedMemoryStream, writerSettings);

                    //encryptedMemoryStream.Seek(0, SeekOrigin.Begin);
                    //BlobClient blobClient = new BlobServiceClient(saConnectionString).GetBlobContainerClient(blobContainerName).GetBlobClient(encryptedBlobName);

                    //var blobOptions = new BlockBlobOpenWriteOptions
                    //{
                    //    HttpHeaders = new BlobHttpHeaders
                    //    {
                    //        ContentType = MimeMapping.GetMimeMapping("parquet"),
                    //    },
                    //    // progress updates about data transfers
                    //    ProgressHandler = new Progress<long>(
                    //    progress => Console.WriteLine("Progress: {0} bytes written", progress))


                    //};
                    //using (var encryptedMemoryStream = await blobClient.OpenWriteAsync(true, blobOptions).ConfigureAwait(false))
                    //    using ParquetFileWriter writer = new ParquetFileWriter(testFileStream, writerSettings);

                    // Process the file - Transformation
                    ColumnarCryptographer cryptographer = new ColumnarCryptographer(reader, writer);

                    cryptographer.Transform();

                    encryptedMemoryStream.Seek(0, SeekOrigin.Begin);
                    //encryptedMemoryStream.Position = 0;


                    //// Create reader
                    //using ParquetFileReader reader2 = new ParquetFileReader(encryptedMemoryStream);

                    ////Get Headers, in Dictionary object
                    //Dictionary<string, int> keyValuePairsHeaders2 = new Dictionary<string, int>();
                    //keyValuePairsHeaders2 = reader2.Read().Last().ToDictionary(row => row.Name, row => row.Index);

                    //testFileStream.Close();

                    //Write Encrypted output to Blob
                    //if (encryptedMemoryStream != null && encryptedMemoryStream.Length > 0)
                    //{
                    //using (var fileStream = new FileStream(@"C:\\Prashant\FileEncryptionDecryption_ParquetFile\test.txt", FileMode.OpenOrCreate, FileAccess.ReadWrite))
                    //{
                    //FileStream fileStream = new FileStream();
                    encryptedMemoryStream.Position = 0;

                    var blobWriteOptions = new BlockBlobOpenWriteOptions
                    {
                        HttpHeaders = new BlobHttpHeaders
                        {
                            ContentType = "parquet",
                        },
                        // progress updates about data transfers
                        ProgressHandler = new Progress<long>(
                    progress => Console.WriteLine("Progress: {0} bytes written", progress))
                    };

                    BlobClient blobClient = new BlobServiceClient(saConnectionString).GetBlobContainerClient(blobContainerName).GetBlobClient(encryptedBlobName);
                    blobClient.Upload(encryptedMemoryStream, true);
                }
                //}
                //else
                //{
                //    throw new Exception("Encrypted Stream can't be Empty");
                //}

            }
            catch (Exception exp)
            {
                throw new Exception("Exception occured in encryptColumnsParquetFile. Message- " + exp.Message);
            }
            return null;
        }
        public static MemoryStream decryptColumnsParquetFile(Stream encryptedFileStream, ProtectedDataEncryptionKey encryptionKey)
        {
            MemoryStream decryptedMemoryStream = new MemoryStream();
            try
            {
                // Create reader
                using ParquetFileReader reader = new ParquetFileReader(encryptedFileStream, GetEncryptionKeyStoreProvider());

                // Copy source settings as target settings
                List<FileEncryptionSettings> writerSettings = reader.FileEncryptionSettings.Select(s => Copy(s)).ToList();

                // Modify a few column settings
                writerSettings[4] = new FileEncryptionSettings<string>(encryptionKey, EncryptionType.Plaintext, StandardSerializerFactory.Default.GetDefaultSerializer<string?>());
                //// Modify a few column settings
                //writerSettings[0] = new FileEncryptionSettings<DateTimeOffset?>(encryptionKey, EncryptionType.Plaintext, SqlSerializerFactory.Default.GetDefaultSerializer<DateTimeOffset?>());
                //writerSettings[3] = new FileEncryptionSettings<string>(encryptionKey, EncryptionType.Plaintext, SqlSerializerFactory.Default.GetDefaultSerializer<string?>());
                //writerSettings[10] = new FileEncryptionSettings<double?>(encryptionKey, EncryptionType.Plaintext, StandardSerializerFactory.Default.GetDefaultSerializer<double?>());


                // Create and pass the target settings to the writer
                using ParquetFileWriter writer = new ParquetFileWriter(decryptedMemoryStream, writerSettings);
                // Process the file - Transformation
                ColumnarCryptographer decryptCryptographer = new ColumnarCryptographer(reader, writer);
                decryptCryptographer.Transform();
                decryptedMemoryStream.Position = 0;
            }
            catch (Exception exp)
            {
                throw new Exception("Exception occured in decryptColumnsParquetFile. Message- " + exp.Message);
            }
            return decryptedMemoryStream;
        }
        public static FileEncryptionSettings Copy(FileEncryptionSettings encryptionSettings)
        {
            Type genericType = encryptionSettings.GetType().GenericTypeArguments[0];
            Type settingsType = typeof(FileEncryptionSettings<>).MakeGenericType(genericType);
            return (FileEncryptionSettings)Activator.CreateInstance(
                settingsType,
                new object[] {
                    encryptionSettings.DataEncryptionKey,
                        encryptionSettings.EncryptionType,
                        encryptionSettings.GetSerializer ()
                }
            );
        }
        public static ProtectedDataEncryptionKey getEncyryptionKey(string keyObjPath)
        {
            byte[] keyObj = System.IO.File.ReadAllBytes(keyObjPath);
            //Key Valut Path
            string azureKeyVaultKeyPath = "https://tms-encryption-kv.vault.azure.net/keys/tms-file-encryption-key/103382890a4a430db3aa5acb86171b06";

            // New Token Credential to authenticate from Azure
            Azure.Core.TokenCredential tokenCredential = new DefaultAzureCredential();

            // Azure Key Vault provider that allows client applications to access a key encryption key is stored in Microsoft Azure Key Vault.
            EncryptionKeyStoreProvider azureKeyProvider = new AzureKeyVaultKeyStoreProvider(tokenCredential);

            // Represents the key encryption key that encrypts and decrypts the data encryption key
            KeyEncryptionKey keyEncryptionKey = new KeyEncryptionKey("KEK", azureKeyVaultKeyPath, azureKeyProvider);

            // Represents the encryption key that encrypts and decrypts the data items
            return new ProtectedDataEncryptionKey("DEK", keyEncryptionKey, keyObj);
        }
        public static Dictionary<string, EncryptionKeyStoreProvider> GetEncryptionKeyStoreProvider()
        {
            // New Token Credential to authenticate from Azure
            Azure.Core.TokenCredential tokenCredential = new DefaultAzureCredential();

            // Azure Key Vault provider that allows client applications to access a key encryption key is stored in Microsoft Azure Key Vault.
            EncryptionKeyStoreProvider azureKeyProvider = new AzureKeyVaultKeyStoreProvider(tokenCredential);

            Dictionary<string, EncryptionKeyStoreProvider> encryptionKeyStoreProviders = new Dictionary<string, EncryptionKeyStoreProvider>();
            encryptionKeyStoreProviders.Add("AZURE_KEY_VAULT", azureKeyProvider);
            return encryptionKeyStoreProviders;
        }
        public static Stream getBlobContentToStream(string saConnectionString, string blobContainerName, string inputBlobName)
        {
            BlobClient blobClient = new BlobServiceClient(saConnectionString).GetBlobContainerClient(blobContainerName).GetBlobClient(inputBlobName);

            //Read the blob
            MemoryStream memoryStreamBlob = new MemoryStream();
            blobClient.DownloadTo(memoryStreamBlob);
            memoryStreamBlob.Position = 0;
            return memoryStreamBlob;
        }

    }
}

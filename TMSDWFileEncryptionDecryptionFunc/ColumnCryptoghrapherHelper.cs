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

namespace TMSDWFileEncryptionDecryptionFunc
{
    internal class ColumnCryptoghrapherHelper
    {
        public static void encryptColumnsParquetFile(string inputBlobName, string encryptedBlobName, string saConnectionString, string blobContainerName, ProtectedDataEncryptionKey encryptionKey)
        {
            try
            {
                #region Read Input Blob
                BlobClient downloadBlobClient = new BlobServiceClient(saConnectionString).GetBlobContainerClient(blobContainerName).GetBlobClient(inputBlobName);
                //Read the blob
                MemoryStream msInputBlob = new MemoryStream();
                downloadBlobClient.DownloadTo(msInputBlob);
                msInputBlob.Position = 0;
                #endregion

                // Create reader
                using ParquetFileReader reader = new ParquetFileReader(msInputBlob);

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

                using (var encryptedMemoryStream = new FileStream(@"temp.parquet", FileMode.OpenOrCreate, FileAccess.ReadWrite))
                {
                    // Create and pass the target settings to the writer
                    using ParquetFileWriter writer = new ParquetFileWriter(encryptedMemoryStream, writerSettings);

                    // Process the file - Transformation
                    ColumnarCryptographer cryptographer = new ColumnarCryptographer(reader, writer);
                    cryptographer.Transform();
                }
                //Write Encrypted Data to Blob
                BlobClient uploadBlobClient = new BlobServiceClient(saConnectionString).GetBlobContainerClient(blobContainerName).GetBlobClient(encryptedBlobName);
                uploadBlobClient.Upload("temp.parquet", true);
                msInputBlob.Dispose();
            }
            catch (Exception exp)
            {
                throw new Exception("Exception occured in encryptColumnsParquetFile. Message- " + exp.Message);
            }
        }
        public static void decryptColumnsParquetFile(string encryptedBlobName, string decryptedBlobName, string saConnectionString, string blobContainerName, ProtectedDataEncryptionKey encryptionKey)
        {
            try
            {
                Stream encryptedFileInput = File.OpenRead(@"C:\Users\Administrator\Desktop\temp\Encrptd_userdata1.parquet");
                Stream decryptedFile = File.OpenWrite(@"C:\Users\Administrator\Desktop\temp\Decrptd22_userdata1.parquet");
                // Create reader
                using ParquetFileReader reader = new ParquetFileReader(encryptedFileInput, GetEncryptionKeyStoreProvider());

                // Copy source settings as target settings
                List<FileEncryptionSettings> writerSettings = reader.FileEncryptionSettings.Select(s => Copy(s)).ToList();

                // Modify a few column settings
                writerSettings[4] = new FileEncryptionSettings<string>(encryptionKey, EncryptionType.Plaintext, StandardSerializerFactory.Default.GetDefaultSerializer<string?>());

                // Create and pass the target settings to the writer
                using ParquetFileWriter writer = new ParquetFileWriter(decryptedFile, writerSettings);
                // Process the file - Transformation
                ColumnarCryptographer decryptCryptographer = new ColumnarCryptographer(reader, writer);

                decryptCryptographer.Transform();
            }
            catch (Exception exp)
            {
                throw new Exception("Exception occured in decryptColumnsParquetFile. Message- " + exp.Message);
            }
        }
        //public static FileEncryptionSettings Copy(FileEncryptionSettings encryptionSettings)
        //{
        //    Type genericType = encryptionSettings.GetType().GenericTypeArguments[0];
        //    Type settingsType = typeof(FileEncryptionSettings<>).MakeGenericType(genericType);
        //    return (FileEncryptionSettings)Activator.CreateInstance(
        //        settingsType,
        //        new object[] {
        //            encryptionSettings.DataEncryptionKey,
        //                encryptionSettings.EncryptionType,
        //                encryptionSettings.GetSerializer ()
        //        }
        //    );
        //}


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

    }
}

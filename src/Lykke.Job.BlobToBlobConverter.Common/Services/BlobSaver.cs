using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace Lykke.Job.BlobToBlobConverter.Common.Services
{
    [PublicAPI]
    public class BlobSaver : IBlobSaver
    {
        private const string _lastBlobFile = "lastblob.txt";
        private const string _tablesStructureFileName = "TableStructure.str2";
        private const int _maxBlockSize = 100 * 1024 * 1024; // 100Mb

        private readonly Encoding _blobEncoding = Encoding.UTF8;
        private readonly CloudBlobContainer _blobContainer;
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5),
            MaximumExecutionTime = TimeSpan.FromMinutes(60),
            ServerTimeout = TimeSpan.FromMinutes(60)
        };
        private readonly ILog _log;
        private readonly ConcurrentDictionary<string, List<string>> _blobDict = new ConcurrentDictionary<string, List<string>>();

        public BlobSaver(
            string blobConnectionString,
            string rootContainer,
            ILog log)
        {
            var blobClient = CloudStorageAccount.Parse(blobConnectionString).CreateCloudBlobClient();
            _blobContainer = blobClient.GetContainerReference(rootContainer);
            if (!_blobContainer.ExistsAsync().GetAwaiter().GetResult())
                _blobContainer.CreateAsync(BlobContainerPublicAccessType.Off, null, null).GetAwaiter().GetResult();
            _log = log;
        }

        public async Task<string> GetLastSavedBlobAsync()
        {
            var blob = _blobContainer.GetBlobReference(_lastBlobFile);
            bool exists = await blob.ExistsAsync();
            if (!exists)
                return null;

            byte[] bytes;
            using (var ms = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(ms);
                bytes = ms.ToArray();
            }
            string lastBlob = _blobEncoding.GetString(bytes);
            return lastBlob.Trim();
        }

        public void StartBlobProcessing()
        {
            _blobDict.Clear();
        }

        public async Task FinishBlobProcessingAsync(string blobName)
        {
            foreach (var pair in _blobDict)
            {
                var blob = _blobContainer.GetBlockBlobReference(pair.Key);
                await blob.PutBlockListAsync(pair.Value);
                await SetContentTypeAsync(blob);
            }

            var lastBlob = _blobContainer.GetBlockBlobReference(_lastBlobFile);
            await lastBlob.DeleteIfExistsAsync(DeleteSnapshotsOption.IncludeSnapshots, null, _blobRequestOptions, null);
            await lastBlob.UploadTextAsync(blobName, null, _blobRequestOptions, null);
            await SetContentTypeAsync(lastBlob);
        }

        public async Task<bool> CreateOrUpdateTablesStructureAsync(TablesStructure tablesStructure)
        {
            string newStructureJson = tablesStructure.ToJson();

            var blob = _blobContainer.GetBlockBlobReference(_tablesStructureFileName);
            bool exists = await blob.ExistsAsync();
            if (exists)
            {
                string structureJson = await blob.DownloadTextAsync(null, _blobRequestOptions, null);
                if (!string.IsNullOrWhiteSpace(structureJson))
                {
                    var structure = structureJson.DeserializeJson<TablesStructure>();
                    if (CompareStructures(tablesStructure, structure))
                        return false;
                }

                _log.WriteWarning(nameof(CreateOrUpdateTablesStructureAsync), "Table structure change", $"Table structure is changed from {structureJson} to {newStructureJson}");
                await blob.DeleteIfExistsAsync();
            }
            await blob.UploadTextAsync(newStructureJson, null, _blobRequestOptions, null);
            await SetContentTypeAsync(blob);
            return true;
        }

        public async Task<TablesStructure> ReadTablesStructureAsync()
        {
            var blob = _blobContainer.GetBlockBlobReference(_tablesStructureFileName);
            if (!await blob.ExistsAsync())
                return null;

            string structureStr = await blob.DownloadTextAsync(null, _blobRequestOptions, null);

            return structureStr.DeserializeJson<TablesStructure>();
        }

        public async Task SaveToBlobAsync(IEnumerable<string> blocks, string directory, string storagePath)
        {
            string path = string.IsNullOrWhiteSpace(directory)
                ? storagePath
                : Path.Combine(directory.ToLower(), storagePath);
            var blob = _blobContainer.GetBlockBlobReference(path);
            if (!_blobDict.ContainsKey(path))
            {
                await blob.DeleteIfExistsAsync();
                _blobDict.TryAdd(path, new List<string>());
            }
            List<string> blockIds = _blobDict[path];

            using (var stream = new MemoryStream())
            {
                using (var writer = new StreamWriter(stream))
                {
                    foreach (var block in blocks)
                    {
                        if (stream.Length + block.Length * 2 >= _maxBlockSize)
                        {
                            await UploadBlockAsync(blob, blockIds, stream);
                            stream.Position = 0;
                            stream.SetLength(0);
                        }

                        writer.WriteLine(block);
                        writer.Flush();
                    }

                    if (stream.Length > 0)
                        await UploadBlockAsync(blob, blockIds, stream);
                }
            }
        }

        private async Task UploadBlockAsync(CloudBlockBlob blob, List<string> blockIds, Stream stream)
        {
            string blockId = Convert.ToBase64String(Encoding.Default.GetBytes(blockIds.Count.ToString("d6")));
            stream.Position = 0;
            await blob.PutBlockAsync(blockId, stream, null, null, _blobRequestOptions, null);
            blockIds.Add(blockId);
        }

        private async Task SetContentTypeAsync(CloudBlockBlob blob)
        {
            try
            {
                blob.Properties.ContentType = "text/plain";
                blob.Properties.ContentEncoding = _blobEncoding.WebName;
                await blob.SetPropertiesAsync(null, _blobRequestOptions, null);
            }
            catch (StorageException)
            {
            }
        }

        private bool CompareStructures(TablesStructure newVersion, TablesStructure oldVersion)
        {
            if (oldVersion?.Tables == null)
                return false;

            if (oldVersion.Tables.Count != newVersion.Tables.Count)
                return false;

            foreach (var table in newVersion.Tables)
            {
                var oldTable = oldVersion.Tables.FirstOrDefault(t =>
                    t.AzureBlobFolder == table.AzureBlobFolder || t.TableName == table.TableName);
                if (oldTable == null)
                    return false;

                if (table.Colums.Count != oldTable.Colums.Count)
                    return false;

                for (int i = 0; i < table.Colums.Count; ++i)
                {
                    if (table.Colums[i].ColumnName != oldTable.Colums[i].ColumnName
                        || table.Colums[i].ColumnType != oldTable.Colums[i].ColumnType)
                        return false;
                }
            }

            return true;
        }
    }
}

using Lykke.Job.BlobToBlobConverter.Common.Services;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class BlobSaver : IBlobSaver
    {
        private const string _lastBlobFile = "lastblob.txt";
        private const string _structureSuffix = ".str";
        private const int _maxBlockSize = 100 * 1024 * 1024; // 100Mb

        private readonly Encoding _blobEncoding = Encoding.UTF8;
        private readonly CloudBlobContainer _blobContainer;
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5),
            MaximumExecutionTime = TimeSpan.FromMinutes(60),
            ServerTimeout = TimeSpan.FromMinutes(60)
        };
        private readonly ConcurrentDictionary<string, List<string>> _blobDict = new ConcurrentDictionary<string, List<string>>();

        public BlobSaver(string blobConnectionString, string rootContainer)
        {
            var blobClient = CloudStorageAccount.Parse(blobConnectionString).CreateCloudBlobClient();
            _blobContainer = blobClient.GetContainerReference(rootContainer);
            if (!_blobContainer.ExistsAsync().GetAwaiter().GetResult())
                _blobContainer.CreateAsync(BlobContainerPublicAccessType.Off, null, null).GetAwaiter().GetResult();
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
            await lastBlob.DeleteIfExistsAsync();
            await lastBlob.UploadTextAsync(blobName, null, _blobRequestOptions, null);
            await SetContentTypeAsync(lastBlob);
        }

        public async Task CreateOrUpdateMappingStructureAsync(Dictionary<string, string> mappingStructure)
        {
            foreach (var pair in mappingStructure)
            {
                var fileName = $"{pair.Key}{_structureSuffix}";
                var blob = _blobContainer.GetBlockBlobReference(fileName);
                bool exists = await blob.ExistsAsync();
                if (exists)
                {
                    string structure = await blob.DownloadTextAsync(null, _blobRequestOptions, null);
                    if (structure == pair.Value)
                        continue;
                    await blob.DeleteAsync();
                }
                await blob.UploadTextAsync(pair.Value, null, _blobRequestOptions, null);
                await SetContentTypeAsync(blob);
            }
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
    }
}

using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using System;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Services
{
    public class BlobReader : IBlobReader
    {
        private const int _blobBlockSize = 4 * 1024 * 1024; // 4 Mb
        private const string _compressedKey = "compressed";

        private readonly string _container;
        private readonly CloudBlobContainer _blobContainer;
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5),
            MaximumExecutionTime = TimeSpan.FromMinutes(60),
            ServerTimeout = TimeSpan.FromMinutes(60)
        };
        private readonly byte[] _eolBytes = Encoding.UTF8.GetBytes("\r\n\r\n");

        public BlobReader(string container, string blobConnectionString)
        {
            _container = container.ToLower();
            var blobClient = CloudStorageAccount.Parse(blobConnectionString).CreateCloudBlobClient();
            _blobContainer = blobClient.GetContainerReference(_container);
            bool containerExists = _blobContainer.ExistsAsync().GetAwaiter().GetResult();
            if (!containerExists)
                throw new InvalidOperationException($"Container {_container} doesn't exist!");
        }

        public async Task<List<string>> GetBlobsForConversionAsync(string lastBlob)
        {
            List<string> blobs = new List<string>();
            BlobContinuationToken continuationToken = null;
            do
            {
                var response = await _blobContainer.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                blobs.AddRange(
                    response.Results
                        .Select(x => x.Uri.LocalPath.Substring(_container.Length + 2))
                        .Where(i => string.IsNullOrWhiteSpace(lastBlob) || i.CompareTo(lastBlob) > 0));
            } while (continuationToken != null);

            var result = blobs.OrderBy(i => i).Take(blobs.Count - 1).ToList();
            return result;
        }

        public async Task ReadAndProcessBlobAsync(string blobName, IMessageProcessor messageProcessor)
        {
            var blob = _blobContainer.GetAppendBlobReference(blobName);
            await blob.FetchAttributesAsync();
            bool isBlobCompressed = false;
            if (blob.Metadata.ContainsKey(_compressedKey) && bool.TryParse(blob.Metadata[_compressedKey], out bool blobCompressed))
                isBlobCompressed = blobCompressed;
            long blobSize = blob.Properties.Length;
            long blobPosition = 0;
            int arrayLength = _blobBlockSize;
            var buffer = new byte[arrayLength];
            int writeStart = 0;
            do
            {
                int readCount = await blob.DownloadRangeToByteArrayAsync(
                    buffer,
                    writeStart,
                    blobPosition,
                    arrayLength - writeStart,
                    null,
                    _blobRequestOptions,
                    null);
                if (readCount == 0)
                {
                    if (writeStart > 0)
                        throw new InvalidOperationException($"Couldn't properly deserialize end part of {blobName}");
                    break;
                }

                int lastEolIndex = -1;
                int filledCount = Math.Min(arrayLength, writeStart + readCount);
                for (int i = _eolBytes.Length - 1; i < filledCount; ++i)
                {
                    if (!FindEolPattern(buffer, i))
                        continue;

                    int chunkSize = i - lastEolIndex - _eolBytes.Length;
                    if (chunkSize == 0)
                        continue;

                    var chunk = new byte[chunkSize];
                    Array.Copy(buffer, lastEolIndex + 1, chunk, 0, chunkSize);
                    if (isBlobCompressed)
                    {
                        using (MemoryStream comp = new MemoryStream(chunk))
                        {
                            using (MemoryStream decomp = new MemoryStream())
                            {
                                using (GZipStream gzip = new GZipStream(comp, CompressionMode.Decompress))
                                {
                                    gzip.CopyTo(decomp);
                                }
                                chunk = decomp.ToArray();
                            }
                        }
                    }
                    bool isCorrectChunk = await messageProcessor.TryProcessMessageAsync(chunk);
                    if (!isCorrectChunk)
                        continue;

                    lastEolIndex = i;
                }

                if (lastEolIndex == -1)
                {
                    writeStart = arrayLength;
                    arrayLength *= 2;
                    Array.Resize(ref buffer, arrayLength);
                }
                else
                {
                    int chunkSize = filledCount - lastEolIndex - 1;
                    Array.Copy(buffer, lastEolIndex + 1, buffer, 0, chunkSize);
                    writeStart = chunkSize;
                }

                blobPosition += readCount;
            }
            while (blobPosition < blobSize);
        }

        private bool FindEolPattern(byte[] buffer, int pos)
        {
            int startPos = pos - _eolBytes.Length + 1;
            for (int i = 0; i < _eolBytes.Length; ++i)
            {
                if (buffer[startPos + i] != _eolBytes[i])
                    return false;
            }
            return true;
        }
    }
}

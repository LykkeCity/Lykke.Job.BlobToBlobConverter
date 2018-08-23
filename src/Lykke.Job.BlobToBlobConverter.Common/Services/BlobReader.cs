using Common.Log;
using JetBrains.Annotations;
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
    [PublicAPI]
    public class BlobReader : IBlobReader
    {
        private const int _maxUnprocessedPatternsCount = 50;
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
        private readonly ILog _log;
        private readonly byte[] _separationPatternBytes = Encoding.UTF8.GetBytes("\r\n\r\n");

        public BlobReader(
            string container,
            string blobConnectionString,
            ILog log)
        {
            _container = container.ToLower();
            var blobClient = CloudStorageAccount.Parse(blobConnectionString).CreateCloudBlobClient();
            _blobContainer = blobClient.GetContainerReference(_container);
            bool containerExists = _blobContainer.ExistsAsync().GetAwaiter().GetResult();
            if (!containerExists)
                throw new InvalidOperationException($"Container {_container} doesn't exist!");
            _log = log;
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

            var result = blobs.OrderBy(i => i)
                .Take(blobs.Count - 1)
                .ToList();
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

                int filledCount = Math.Min(arrayLength, writeStart + readCount);

                int lastReadIndex = await ProcessBufferAsync(
                    buffer,
                    filledCount,
                    isBlobCompressed,
                    messageProcessor);

                if (lastReadIndex == -1)
                {
                    writeStart = arrayLength;
                    arrayLength *= 2;
                    Array.Resize(ref buffer, arrayLength);
                }
                else
                {
                    int chunkSize = filledCount - lastReadIndex - 1;
                    Array.Copy(buffer, lastReadIndex + 1, buffer, 0, chunkSize);
                    writeStart = chunkSize;
                }

                blobPosition += readCount;
            }
            while (blobPosition < blobSize);
        }

        private async Task<int> ProcessBufferAsync(
            byte[] buffer,
            int filledCount,
            bool isBlobCompressed,
            IMessageProcessor messageProcessor)
        {
            var eolIndexes = new List<int> {-1};
            for (int i = _separationPatternBytes.Length - 1; i < filledCount; ++i)
            {
                (bool eolFound, int seekStep) = FindEolPattern(buffer, i);
                if (!eolFound)
                {
                    if (seekStep > 1)
                        i += seekStep - 1;
                    continue;
                }

                bool foundCorrectChunk = false;
                for (int j = 0; j < eolIndexes.Count; ++j)
                {
                    int eolIndex = eolIndexes[j];
                    int chunkSize = i - eolIndex - _separationPatternBytes.Length;
                    if (chunkSize == 0)
                        continue;

                    var chunk = new byte[chunkSize];
                    Array.Copy(buffer, eolIndex + 1, chunk, 0, chunkSize);
                    if (isBlobCompressed)
                    {
                        chunk = UnpackMessage(chunk);
                        if (chunk == null)
                            continue;
                    }

                    foundCorrectChunk = await messageProcessor.TryProcessMessageAsync(chunk);
                    if (foundCorrectChunk)
                    {
                        if (j > 0)
                            _log.WriteWarning(
                                nameof(ProcessBufferAsync),
                                null,
                                $"Couldn't process message(s). Skipped {eolIndex - eolIndexes[0]} bytes with {j} patterns.");
                        break;
                    }
                }

                if (foundCorrectChunk)
                    eolIndexes.Clear();
                eolIndexes.Add(i);
                if (eolIndexes.Count >= _maxUnprocessedPatternsCount)
                    throw new InvalidOperationException($"Couldn't properly process blob - {eolIndexes.Count} unprocessed patterns.");
            }

            return eolIndexes[0];
        }

        private byte[] UnpackMessage(byte[] chunk)
        {
            try
            {
                using (MemoryStream comp = new MemoryStream(chunk))
                {
                    using (MemoryStream decomp = new MemoryStream())
                    {
                        using (GZipStream gzip = new GZipStream(comp, CompressionMode.Decompress))
                        {
                            gzip.CopyTo(decomp);
                        }
                        return decomp.ToArray();
                    }
                }
            }
            catch (InvalidDataException ex)
            {
                _log.WriteInfo(nameof(ReadAndProcessBlobAsync), null, ex.Message);
                return null;
            }
        }

        private (bool, int) FindEolPattern(byte[] buffer, int pos)
        {
            int startPos = pos - _separationPatternBytes.Length + 1;
            for (int i = _separationPatternBytes.Length - 1; i >= 0; --i)
            {
                if (buffer[startPos + i] != _separationPatternBytes[i])
                {
                    int nextStep = 1;
                    for (int j = 1; j < _separationPatternBytes.Length; ++j)
                    {
                        if (buffer[startPos + j] != _separationPatternBytes[0])
                            ++nextStep;
                        else
                            return (false, nextStep);
                    }
                    return (false, nextStep);
                }
            }
            return (true, 0);
        }
    }
}

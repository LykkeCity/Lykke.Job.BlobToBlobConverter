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
            bool facedErrorOnUnpack = false;
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
                for (int i = _separationPatternBytes.Length - 1; i < filledCount; ++i)
                {
                    (bool eolFound, int seekStep) = FindEolPattern(buffer, i);
                    if (!eolFound)
                    {
                        if (seekStep > 1)
                            i += seekStep - 1;
                        continue;
                    }

                    int chunkSize = i - lastEolIndex - _separationPatternBytes.Length;
                    if (chunkSize == 0)
                        continue;

                    var chunk = new byte[chunkSize];
                    Array.Copy(buffer, lastEolIndex + 1, chunk, 0, chunkSize);
                    if (isBlobCompressed)
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
                                    chunk = decomp.ToArray();
                                }
                            }
                            if (facedErrorOnUnpack)
                                facedErrorOnUnpack = false;
                        }
                        catch (InvalidDataException ex)
                        {
                            if (facedErrorOnUnpack)
                                throw;
                            _log.WriteError(nameof(ReadAndProcessBlobAsync), null, ex);
                            chunk = null;
                            facedErrorOnUnpack = true;
                        }
                    }
                    if (chunk != null)
                    {
                        bool isCorrectChunk = await messageProcessor.TryProcessMessageAsync(chunk);
                        if (!isCorrectChunk)
                            continue;
                    }

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

using System;
using System.Linq;
using System.Text;
using System.IO;
using System.IO.Compression;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Lykke.Job.BlobToBlobConverter.Common.Helpers;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.RetryPolicies;

namespace Lykke.Job.BlobToBlobConverter.Common.Services
{
    [PublicAPI]
    public class BlobReader : IBlobReader
    {
        private const int _maxUnprocessedPatternsCount = 50;
        private const int _blobBlockSize = 4 * 1024 * 1024; // 4 Mb
        private const int _maxBlobBlockSize = 100 * 1024 * 1024; // 100 Mb
        private const int _maxAllowedSkippedBytesCount = 10;
        private const string _compressedKey = "compressed";
        private const string _newFormatKey = "NewFormat";

        private readonly string _container;
        private readonly CloudBlobContainer _blobContainer;
        private readonly BlobRequestOptions _blobRequestOptions = new BlobRequestOptions
        {
            RetryPolicy = new ExponentialRetry(TimeSpan.FromSeconds(5), 5),
            MaximumExecutionTime = TimeSpan.FromMinutes(60),
            ServerTimeout = TimeSpan.FromMinutes(60)
        };

        private readonly IMessageProcessor _messageProcessor;
        private readonly ILog _log;
        private readonly byte[] _delimiterBytes = Encoding.UTF8.GetBytes("\r\n\r\n");
        private readonly IMessageTypeResolver _messageTypeResolver;

        private Type _messageType;
        private SerializationFormat? _deserializeFormat;
        private bool? _isNewFormat;

        public BlobReader(
            string container,
            string blobConnectionString,
            IMessageTypeResolver messageTypeResolver,
            IMessageProcessor messageProcessor,
            ILog log)
        {
            _container = container.ToLower();
            var blobClient = CloudStorageAccount.Parse(blobConnectionString).CreateCloudBlobClient();
            _blobContainer = blobClient.GetContainerReference(_container);
            bool containerExists = _blobContainer.ExistsAsync().GetAwaiter().GetResult();
            if (!containerExists)
                throw new InvalidOperationException($"Container {_container} doesn't exist!");
            _messageTypeResolver = messageTypeResolver;
            _messageProcessor = messageProcessor;
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

        public async Task ReadAndProcessBlobAsync(string blobName)
        {
            if (_messageType == null)
                _messageType = await _messageTypeResolver.ResolveMessageTypeAsync();

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
                    blob);

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
            CloudAppendBlob blob)
        {
            var delimiterEndIndexes = new List<int> {-1};
            while(true)
            {
                int newDelimiterEndIndex = await GetNextDelimiterEndIndexAsync(
                    buffer,
                    filledCount,
                    delimiterEndIndexes.Last(),
                    blob);
                if (newDelimiterEndIndex == -1)
                    break;

                bool foundCorrectChunk = false;
                for (int j = 0; j < delimiterEndIndexes.Count; ++j)
                {
                    int delimiterEndIndex = delimiterEndIndexes[j];
                    int chunkSize = newDelimiterEndIndex - delimiterEndIndex - _delimiterBytes.Length;
                    if (chunkSize == 0)
                        continue;

                    int chunkStart = delimiterEndIndex + 1;
                    if (_isNewFormat.HasValue && _isNewFormat.Value)
                    {
                        chunkSize -= 8;
                        chunkStart += 4;
                    }
                    var chunk = new byte[chunkSize];
                    Array.Copy(buffer, chunkStart, chunk, 0, chunkSize);
                    if (isBlobCompressed)
                    {
                        chunk = UnpackMessage(chunk);
                        if (chunk == null)
                            continue;
                    }

                    foundCorrectChunk = TryDeserialize(chunk, out var obj);
                    if (foundCorrectChunk)
                    {
                        int skippedBytesCount = delimiterEndIndex - delimiterEndIndexes[0];
                        if (j > 1 || j == 1 && skippedBytesCount > _maxAllowedSkippedBytesCount)
                            throw new InvalidOperationException(
                                $"Couldn't process message(s). Skipped {skippedBytesCount} bytes with {j} delimiters.");
                        await _messageProcessor.ProcessMessageAsync(obj);
                        break;
                    }
                }

                if (foundCorrectChunk)
                    delimiterEndIndexes.Clear();
                delimiterEndIndexes.Add(newDelimiterEndIndex);
                if (delimiterEndIndexes.Count >= _maxUnprocessedPatternsCount)
                    throw new InvalidOperationException($"Couldn't properly process blob - {delimiterEndIndexes.Count} unprocessed patterns.");
            }

            return delimiterEndIndexes[0];
        }

        private async Task<int> GetNextDelimiterEndIndexAsync(
            byte[] buffer,
            int filledCount,
            int prevDelimiterIndex,
            CloudAppendBlob blob)
        {
            if (!_isNewFormat.HasValue)
                _isNewFormat = await DetectNewFormatAsync(
                    buffer,
                    filledCount,
                    prevDelimiterIndex,
                    blob);

            if (_isNewFormat.Value)
                return GetNextDelimiterEndIndexNew(
                    buffer,
                    filledCount,
                    prevDelimiterIndex);
            return GetNextDelimiterEndIndexOld(
                buffer,
                filledCount,
                prevDelimiterIndex);
        }

        private async Task<bool> DetectNewFormatAsync(
            byte[] buffer,
            int filledCount,
            int prevDelimiterIndex,
            CloudAppendBlob blob)
        {
            if (blob.Metadata == null)
                await blob.FetchAttributesAsync();
            if (blob.Metadata.ContainsKey(_newFormatKey) && bool.TryParse(blob.Metadata[_newFormatKey], out bool isNewFormat))
                return isNewFormat;
            return false;
        }

        private int GetNextDelimiterEndIndexNew(
            byte[] buffer,
            int filledCount,
            int prevDelimiterIndex)
        {
            int startIndex = prevDelimiterIndex + 1;
            if (startIndex + 4 >= filledCount)
                return -1;

            try
            {
                int messageLength = BitConverter.ToInt32(buffer, startIndex);
                int delimiterEndIndex = prevDelimiterIndex + 8 + messageLength + _delimiterBytes.Length;
                if (delimiterEndIndex >= filledCount)
                    return -1;

                int messageLength2 = BitConverter.ToInt32(buffer, startIndex + 4 + messageLength);
                if (messageLength != messageLength2)
                {
                    _isNewFormat = false;
                    _log.WriteWarning(nameof(GetNextDelimiterEndIndexNew), "NewDeserialization", $"Message length don't match - startIndex: {startIndex}, messageLength: {messageLength}");
                    return -1;
                }

                var (isDelimiterEnd, _) = IsDelimiterEnd(buffer, delimiterEndIndex);
                if (isDelimiterEnd)
                    return delimiterEndIndex;

                _isNewFormat = false;
                _log.WriteWarning(nameof(GetNextDelimiterEndIndexNew), "NewDeserialization", $"Not found end delimiter - startIndex: {startIndex}, messageLength: {messageLength}");
                return -1;
            }
            catch (Exception e)
            {
                _isNewFormat = false;
                _log.WriteWarning(nameof(GetNextDelimiterEndIndexNew), "NewDeserialization", $"Error from startIndex {startIndex}: {e.Message}");
                return -1;
            }
        }

        private int GetNextDelimiterEndIndexOld(
            byte[] buffer,
            int filledCount,
            int prevDelimiterIndex)
        {
            for (int i = prevDelimiterIndex + _delimiterBytes.Length; i < filledCount; ++i)
            {
                (bool eolFound, int seekStep) = IsDelimiterEnd(buffer, i);
                if (!eolFound)
                {
                    if (seekStep > 1)
                        i += seekStep - 1;
                    continue;
                }

                return i;
            }

            return -1;
        }

        private (bool, int) IsDelimiterEnd(byte[] buffer, int pos)
        {
            int startPos = pos - _delimiterBytes.Length + 1;
            for (int i = _delimiterBytes.Length - 1; i >= 0; --i)
            {
                if (buffer[startPos + i] == _delimiterBytes[i])
                    continue;

                int nextStep = 1;
                for (int j = 1; j < _delimiterBytes.Length; ++j)
                {
                    if (buffer[startPos + j] == _delimiterBytes[0])
                        return (false, nextStep);

                    ++nextStep;
                }
                return (false, nextStep);
            }
            return (true, 0);
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

        private bool TryDeserialize(byte[] data, out object result)
        {
            if (_deserializeFormat.HasValue)
            {
                bool shouldLogError = _isNewFormat ?? false;
                switch (_deserializeFormat.Value)
                {
                    case SerializationFormat.Json:
                        return JsonDeserializer.TryDeserialize(
                            data,
                            _messageType,
                            shouldLogError ? _log : null,
                            out result);
                    case SerializationFormat.MessagePack:
                        return MessagePackDeserializer.TryDeserialize(
                            data,
                            _messageType,
                            shouldLogError ? _log : null,
                            out result);
                    case SerializationFormat.Protobuf:
                        return ProtobufDeserializer.TryDeserialize(
                            data,
                            _messageType,
                            shouldLogError ? _log : null,
                            out result);
                    default:
                        throw new NotSupportedException($"Serialization format {_deserializeFormat.Value} is not supported");
                }
            }
            bool success = JsonDeserializer.TryDeserialize(
                data,
                _messageType,
                null,
                out result);
            if (success)
            {
                _deserializeFormat = SerializationFormat.Json;
                return true;
            }
            success = MessagePackDeserializer.TryDeserialize(
                data,
                _messageType,
                null,
                out result);
            if (success)
            {
                _deserializeFormat = SerializationFormat.MessagePack;
                return true;
            }
            success = ProtobufDeserializer.TryDeserialize(
                data,
                _messageType,
                null,
                out result);
            if (!success)
                _log.WriteWarning(nameof(TryDeserialize), null, $"Couldn't deserialize message with length {data.Length}");
            return success;
        }
    }
}

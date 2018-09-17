using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;

namespace Lykke.Job.BlobToBlobConverter.Common.Services
{
    [PublicAPI]
    public class BlobProcessor : IBlobProcessor
    {
        private readonly IBlobReader _blobReader;
        private readonly IBlobSaver _blobSaver;
        private readonly IStructureBuilder _structureBuilder;
        private readonly IMessageProcessor _messageConverter;
        private readonly ILog _log;
        private readonly string _instanceTag;

        private bool _allBlobsReprocessingRequired;
        private string _lastBlob;

        public BlobProcessor(
            IBlobReader blobReader,
            IBlobSaver blobSaver,
            IMessageProcessor messageConverter,
            IStructureBuilder structureBuilder,
            ILog log,
            string instanceTag = null,
            string lastBlob = null)
        {
            _blobReader = blobReader;
            _blobSaver = blobSaver;
            _structureBuilder = structureBuilder;
            _messageConverter = messageConverter;
            _log = log;
            _instanceTag = instanceTag;
            _lastBlob = lastBlob;

            if (_structureBuilder.IsDynamicStructure)
            {
                var tablesStructure = _blobSaver.ReadTablesStructureAsync().GetAwaiter().GetResult();
                _allBlobsReprocessingRequired = _structureBuilder.IsAllBlobsReprocessingRequired(tablesStructure);
            }
            else
            {
                var tablesStructure = _structureBuilder.GetTablesStructure();
                _allBlobsReprocessingRequired = _blobSaver.CreateOrUpdateTablesStructureAsync(tablesStructure).GetAwaiter().GetResult();
            }
        }

        public async Task ProcessAsync()
        {
            List<string> blobs;
            if (_allBlobsReprocessingRequired)
            {
                _log.WriteInfo("BlobProcessor.ProcessAsync", _instanceTag, $"All blobs will be reprocessed");
                blobs = await _blobReader.GetBlobsForConversionAsync(null);
                _allBlobsReprocessingRequired = false;
            }
            else
            {
                string lastBlob;
                if (!string.IsNullOrWhiteSpace(_lastBlob))
                {
                    lastBlob = _lastBlob;
                    _lastBlob = null;
                }
                else
                {
                    lastBlob = await _blobSaver.GetLastSavedBlobAsync();
                }
                blobs = await _blobReader.GetBlobsForConversionAsync(lastBlob);
            }

            foreach (var blob in blobs)
            {
                try
                {
                    _log.WriteInfo("BlobProcessor.ProcessAsync", _instanceTag, $"Processing {blob}");

                    _blobSaver.StartBlobProcessing();
                    _messageConverter.StartBlobProcessing((directory, messages) => _blobSaver.SaveToBlobAsync(messages, directory, blob));

                    await _blobReader.ReadAndProcessBlobAsync(blob);

                    await _messageConverter.FinishBlobProcessingAsync();
                    await _blobSaver.FinishBlobProcessingAsync(blob);

                    _log.WriteInfo("BlobProcessor.ProcessAsync", _instanceTag, $"Processed {blob}");
                }
                catch (Exception ex)
                {
                    _log.WriteError("BlobProcessor.ProcessAsync", string.IsNullOrWhiteSpace(_instanceTag) ? blob : $"{_instanceTag}:{blob}", ex);
                    throw;
                }
            }
            if (blobs.Count > 0)
                _log.WriteInfo("BlobProcessor.ProcessAsync", _instanceTag, $"Processed {blobs.Count} blobs");
        }
    }
}

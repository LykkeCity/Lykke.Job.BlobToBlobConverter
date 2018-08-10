﻿using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Services
{
    [PublicAPI]
    public class BlobProcessor : IBlobProcessor
    {
        private readonly IBlobReader _blobReader;
        private readonly IBlobSaver _blobSaver;
        private readonly IStructureBuilder _structureBuilder;
        private readonly IMessageProcessor _messageConverter;
        private readonly string _instanceTag;
        private readonly ILog _log;

        private bool _structureUpdated;

        public BlobProcessor(
            IBlobReader blobReader,
            IBlobSaver blobSaver,
            IMessageProcessor messageConverter,
            IStructureBuilder structureBuilder,
            ILog log)
            : this(
                blobReader,
                blobSaver,
                messageConverter,
                structureBuilder,
                null,
                log)
        {
        }

        public BlobProcessor(
            IBlobReader blobReader,
            IBlobSaver blobSaver,
            IMessageProcessor messageConverter,
            IStructureBuilder structureBuilder,
            string instanceTag,
            ILog log)
        {
            _blobReader = blobReader;
            _blobSaver = blobSaver;
            _structureBuilder = structureBuilder;
            _messageConverter = messageConverter;
            _instanceTag = instanceTag;
            _log = log;

            var messagesStructure = _structureBuilder.GetMappingStructure();
            _structureUpdated = _blobSaver.CreateOrUpdateMappingStructureAsync(messagesStructure).GetAwaiter().GetResult();

            var tablesStructure = _structureBuilder.GetTablesStructure();
            _structureUpdated = _structureUpdated || _blobSaver.CreateOrUpdateTablesStructureAsync(tablesStructure).GetAwaiter().GetResult();
        }

        public async Task ProcessAsync()
        {
            List<string> blobs;
            if (_structureUpdated)
            {
                blobs = await _blobReader.GetBlobsForConversionAsync(null);
            }
            else
            {
                var lastBlob = await _blobSaver.GetLastSavedBlobAsync();
                blobs = await _blobReader.GetBlobsForConversionAsync(lastBlob);
            }
            foreach (var blob in blobs)
            {
                try
                {
                    _log.WriteInfo(nameof(BlobProcessor), nameof(ProcessAsync), $"Processing {blob}");

                    _blobSaver.StartBlobProcessing();
                    _messageConverter.StartBlobProcessing((container, messages) => _blobSaver.SaveToBlobAsync(messages, container, blob));

                    await _blobReader.ReadAndProcessBlobAsync(blob, _messageConverter);

                    await _messageConverter.FinishBlobProcessingAsync();
                    await _blobSaver.FinishBlobProcessingAsync(blob);

                    _log.WriteInfo(nameof(BlobProcessor), nameof(ProcessAsync), $"Processed {blob}");
                }
                catch (Exception ex)
                {
                    _log.WriteError("BlobProcessor.ProcessAsync", string.IsNullOrEmpty(_instanceTag) ? blob : $"{_instanceTag}:{blob}", ex);
                    throw;
                }
            }
            if (blobs.Count > 0)
                _log.WriteInfo(nameof(BlobProcessor), nameof(ProcessAsync), $"Processed {blobs.Count} blobs");
        }
    }
}

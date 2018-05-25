using Common.Log;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using System;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Services
{
    public class BlobProcessor : IBlobProcessor
    {
        private readonly IBlobReader _blobReader;
        private readonly IBlobSaver _blobSaver;
        private readonly IStructureBuilder _structureBuilder;
        private readonly IMessageProcessor _messageConverter;
        private readonly string _instanceTag;
        private readonly ILog _log;

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

            //var messagesStructure = _structureBuilder.GetMappingStructure();
            //_blobSaver.CreateOrUpdateMappingStructureAsync(messagesStructure).GetAwaiter().GetResult();

            var tablesStructure = _structureBuilder.GetTablesStructure();
            _blobSaver.CreateOrUpdateTablesStructureAsync(tablesStructure).GetAwaiter().GetResult();
        }

        public async Task ProcessAsync()
        {
            var lastBlob = await _blobSaver.GetLastSavedBlobAsync();
            var blobs = await _blobReader.GetBlobsForConversionAsync(lastBlob);
            foreach (var blob in blobs)
            {
                try
                {
                    await _log.WriteInfoAsync(nameof(BlobProcessor), nameof(ProcessAsync), $"Processing {blob}");

                    _blobSaver.StartBlobProcessing();
                    _messageConverter.StartBlobProcessing((container, messages) => _blobSaver.SaveToBlobAsync(messages, container, blob));

                    await _blobReader.ReadAndProcessBlobAsync(blob, _messageConverter);

                    await _messageConverter.FinishBlobProcessingAsync();
                    await _blobSaver.FinishBlobProcessingAsync(blob);

                    await _log.WriteInfoAsync(nameof(BlobProcessor), nameof(ProcessAsync), $"Processed {blob}");
                }
                catch (Exception ex)
                {
                    _log.WriteError("BlobProcessor.ProcessAsync", string.IsNullOrEmpty(_instanceTag) ? blob : $"{_instanceTag}:{blob}", ex);
                    throw;
                }
            }
            _log.WriteInfo(nameof(BlobProcessor), nameof(ProcessAsync), $"Processed {blobs.Count} blobs");
        }
    }
}

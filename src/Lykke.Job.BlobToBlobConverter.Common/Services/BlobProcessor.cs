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
        private readonly IMessageProcessor _messageConverter;
        private readonly ILog _log;

        public BlobProcessor(
            IBlobReader blobReader,
            IBlobSaver blobSaver,
            IMessageProcessor messageConverter,
            ILog log)
        {
            _blobReader = blobReader;
            _blobSaver = blobSaver;
            _messageConverter = messageConverter;
            _log = log;

            var messagesStructure = _messageConverter.GetMappingStructure();
            _blobSaver.CreateOrUpdateMappingStructureAsync(messagesStructure).GetAwaiter().GetResult();
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
                    await _log.WriteFatalErrorAsync("BlobProcessor.ProcessAsync", blob, ex);
                    throw;
                }
            }
            await _log.WriteInfoAsync(nameof(BlobProcessor), nameof(ProcessAsync), $"Processed {blobs.Count} blobs");
        }
    }
}

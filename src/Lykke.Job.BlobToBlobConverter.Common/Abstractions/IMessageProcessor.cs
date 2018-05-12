using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IMessageProcessor
    {
        void StartBlobProcessing();

        bool TryProcessMessage(byte[] data);

        Task FinishBlobProcessingAsync(Func<string, List<string>, Task> messagesHandler);

        Dictionary<string, string> GetMappingStructure();
    }
}

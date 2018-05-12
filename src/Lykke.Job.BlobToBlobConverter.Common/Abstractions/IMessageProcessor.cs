using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IMessageProcessor
    {
        Dictionary<string, string> GetMappingStructure();

        void StartBlobProcessing(Func<string, List<string>, Task> messagesHandler);

        Task FinishBlobProcessingAsync();

        bool TryProcessMessage(byte[] data);
    }
}

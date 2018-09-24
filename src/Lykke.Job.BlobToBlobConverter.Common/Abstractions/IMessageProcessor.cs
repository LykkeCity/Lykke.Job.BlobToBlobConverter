using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IMessageProcessor
    {
        void StartBlobProcessing(Func<string, List<string>, Task> messagesHandler);

        Task FinishBlobProcessingAsync();

        Task ProcessMessageAsync(object obj);
    }
}

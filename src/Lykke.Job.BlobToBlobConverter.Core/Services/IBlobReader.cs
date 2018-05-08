using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Core.Services
{
    public interface IBlobReader
    {
        Task<List<string>> GetBlobsForConversionAsync(string lastBlob);

        Task ReadAndProcessBlobAsync(string blobName, IMessageProcessor messageProcessor);
    }
}

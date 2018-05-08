using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Core.Services
{
    public interface IBlobSaver
    {
        void StartBlobProcessing();

        Task FinishBlobProcessingAsync(string blobName);

        Task SaveToBlobAsync(
            IEnumerable<string> blocks,
            string directory,
            string storagePath);

        Task<string> GetLastSavedBlobAsync();

        Task CreateOrUpdateMappingStructureAsync(Dictionary<string, string> mappingStructure);
    }
}

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
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

        Task<bool> CreateOrUpdateTablesStructureAsync(TablesStructure tablesStructure);

        Task<TablesStructure> ReadTablesStructureAsync();
    }
}

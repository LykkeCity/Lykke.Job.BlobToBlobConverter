using System.Collections.Generic;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IStructureBuilder
    {
        Dictionary<string, string> GetMappingStructure();

        TablesStructure GetTablesStructure();
    }
}

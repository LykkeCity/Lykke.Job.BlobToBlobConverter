namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IStructureBuilder
    {
        bool IsDynamicStructure { get; }

        bool IsAllBlobsReprocessingRequired(TablesStructure currentStructure);

        TablesStructure GetTablesStructure();
    }
}

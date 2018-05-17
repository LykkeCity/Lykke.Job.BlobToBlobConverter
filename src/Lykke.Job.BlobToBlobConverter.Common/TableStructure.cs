using System.Collections.Generic;

namespace Lykke.Job.BlobToBlobConverter.Common
{
    public class TableStructure
    {
        public string TableName { get; set; }

        public string AzureBlobFolder { get; set; }

        public List<ColumnInfo> Colums { get; set; }
    }
}

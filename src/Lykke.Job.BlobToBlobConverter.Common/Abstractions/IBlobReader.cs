﻿using System.Collections.Generic;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IBlobReader
    {
        Task<List<string>> GetBlobsForConversionAsync(string lastBlob);

        Task ReadAndProcessBlobAsync(string blobName);
    }
}

using System;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Core.Services
{
    public interface IProcessingTypeResolver
    {
        Task<Type> ResolveProcessingTypeAsync();
    }
}

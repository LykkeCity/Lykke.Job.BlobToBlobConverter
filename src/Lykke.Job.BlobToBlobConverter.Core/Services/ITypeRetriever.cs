using System;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Core.Services
{
    public interface ITypeRetriever
    {
        Task<Type> RetrieveTypeAsync(string typeName, string nugetPackageName);
    }
}

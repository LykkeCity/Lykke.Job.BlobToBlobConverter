using System;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IMessageTypeResolver
    {
        Task<Type> ResolveMessageTypeAsync();
    }
}

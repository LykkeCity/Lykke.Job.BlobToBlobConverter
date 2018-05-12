using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Abstractions
{
    public interface IBlobProcessor
    {
        Task ProcessAsync();
    }
}

using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Common.Services
{
    public interface IBlobProcessor
    {
        Task ProcessAsync();
    }
}

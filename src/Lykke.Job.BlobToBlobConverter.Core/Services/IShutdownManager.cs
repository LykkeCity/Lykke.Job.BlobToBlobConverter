using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Core.Services
{
    public interface IShutdownManager
    {
        Task StopAsync();
    }
}

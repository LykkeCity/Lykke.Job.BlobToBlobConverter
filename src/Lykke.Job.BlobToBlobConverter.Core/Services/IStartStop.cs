using Autofac;
using Common;

namespace Lykke.Job.BlobToBlobConverter.Core.Services
{
    public interface IStartStop : IStartable, IStopable
    {
    }
}

using Common.Log;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class StartupManager : IStartupManager
    {
        private readonly ILog _log;
        private readonly IMainHandler _mainHandler;

        public StartupManager(ILog log, IMainHandler mainHandler)
        {
            _log = log;
            _mainHandler = mainHandler;
        }

        public Task StartAsync()
        {
            _mainHandler.Start();

            return Task.CompletedTask;
        }
    }
}

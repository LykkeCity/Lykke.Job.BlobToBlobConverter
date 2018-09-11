using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.BlobToBlobConverter.Core.Services;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class ShutdownManager : IShutdownManager
    {
        private readonly ILog _log;
        private readonly List<IStopable> _items = new List<IStopable>();

        public ShutdownManager(ILog log, IEnumerable<IStartStop> items)
        {
            _log = log;
            _items.AddRange(items);
        }

        public Task StopAsync()
        {
            Parallel.ForEach(_items, i =>
            {
                try
                {
                    i.Stop();
                }
                catch (Exception ex)
                {
                    _log.WriteWarning(nameof(StopAsync), null, $"Unable to stop {i.GetType().Name}", ex);
                }
            });

            return Task.CompletedTask;
        }
    }
}

using System;
using System.Threading.Tasks;
using Common;
using Common.Log;
using JetBrains.Annotations;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Lykke.Job.BlobToBlobConverter.Core.Services;

namespace Lykke.Job.BlobToBlobConverter.PeriodicalHandlers
{
    [UsedImplicitly]
    public class PeriodicalHandler : TimerPeriod, IStartStop
    {
        private readonly IBlobProcessor _blobProcessor;

        public PeriodicalHandler(
            IBlobProcessor blobProcessor,
            ILog log,
            TimeSpan processTimeout)
            : base((int)processTimeout.TotalMilliseconds, log)
        {
            _blobProcessor = blobProcessor;
        }

        public override async Task Execute()
        {
            await _blobProcessor.ProcessAsync();
        }
    }
}

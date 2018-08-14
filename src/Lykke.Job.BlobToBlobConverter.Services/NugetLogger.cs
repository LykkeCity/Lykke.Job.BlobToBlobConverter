using Common.Log;
using NuGet.Common;
using System;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    internal class NugetLogger : ILogger
    {
        private readonly ILog _log;

        internal NugetLogger(ILog log)
        {
            _log = log;
        }

        public void LogDebug(string data)
        {
            _log.WriteInfo(nameof(NugetLogger), "", data);
        }

        public void LogError(string data)
        {
            _log.WriteError(nameof(NugetLogger), "", new InvalidOperationException(data));
        }

        public void LogErrorSummary(string data)
        {
            _log.WriteError(nameof(NugetLogger), "", new InvalidOperationException(data));
        }

        public void LogInformation(string data)
        {
            _log.WriteInfo(nameof(NugetLogger), "", data);
        }

        public void LogInformationSummary(string data)
        {
            _log.WriteInfo(nameof(NugetLogger), "", data);
        }

        public void LogMinimal(string data)
        {
            _log.WriteInfo(nameof(NugetLogger), "", data);
        }

        public void LogVerbose(string data)
        {
            _log.WriteInfo(nameof(NugetLogger), "", data);
        }

        public void LogWarning(string data)
        {
            _log.WriteWarning(nameof(NugetLogger), "", data);
        }
    }
}

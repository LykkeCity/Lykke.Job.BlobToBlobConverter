using Common.Log;
using NuGet.Common;
using System;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public partial class TypeRetriever
    {
        internal class NugetLogger : ILogger
        {
            private readonly ILog _log;

            internal NugetLogger(ILog log)
            {
                _log = log;
            }

            public void Log(LogLevel level, string data)
            {
                switch (level)
                {
                    case LogLevel.Warning:
                        _log.WriteWarning(nameof(NugetLogger), "", data);
                        break;
                    case LogLevel.Error:
                        _log.WriteError(nameof(NugetLogger), "", new InvalidOperationException(data));
                        break;
                    default:
                        _log.WriteInfo(nameof(NugetLogger), "", data);
                        break;
                }
            }

            public Task LogAsync(LogLevel level, string data)
            {
                switch (level)
                {
                    case LogLevel.Warning:
                        _log.WriteWarning(nameof(NugetLogger), "", data);
                        break;
                    case LogLevel.Error:
                        _log.WriteError(nameof(NugetLogger), "", new InvalidOperationException(data));
                        break;
                    default:
                        _log.WriteInfo(nameof(NugetLogger), "", data);
                        break;
                }
                return Task.CompletedTask;
            }
            /*
            public void Log(ILogMessage message)
            {
                switch (message.Level)
                {
                    case LogLevel.Warning:
                        _log.WriteWarning(nameof(NugetLogger), "", message.Message);
                        break;
                    case LogLevel.Error:
                        _log.WriteError(nameof(NugetLogger), "", new InvalidOperationException(message.Message));
                        break;
                    default:
                        _log.WriteInfo(nameof(NugetLogger), "", message.Message);
                        break;
                }
            }

            public Task LogAsync(ILogMessage message)
            {
                switch (message.Level)
                {
                    case LogLevel.Warning:
                        _log.WriteWarning(nameof(NugetLogger), "", message.Message);
                        break;
                    case LogLevel.Error:
                        _log.WriteError(nameof(NugetLogger), "", new InvalidOperationException(message.Message));
                        break;
                    default:
                        _log.WriteInfo(nameof(NugetLogger), "", message.Message);
                        break;
                }
                return Task.CompletedTask;
            }
            */
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
}

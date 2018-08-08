using Autofac;
using Common.Log;
using Lykke.Common;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Lykke.Job.BlobToBlobConverter.Common.Services;
using Lykke.Job.BlobToBlobConverter.Core;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using Lykke.Job.BlobToBlobConverter.Services;
using Lykke.Job.BlobToBlobConverter.Settings;
using Lykke.Job.BlobToBlobConverter.PeriodicalHandlers;
using System;

namespace Lykke.Job.BlobToBlobConverter.Modules
{
    public class JobModule : Module
    {
        private readonly BlobToBlobConverterSettings _settings;
        private readonly ILog _log;
        private readonly string _instanceTag;

        public JobModule(BlobToBlobConverterSettings settings, ILog log, string instanceTag)
        {
            _settings = settings;
            _log = log;
            _instanceTag = instanceTag;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>()
                .SingleInstance();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>()
                .SingleInstance();

            builder.RegisterResourcesMonitoring(_log);

            builder.RegisterType<BlobReader>()
                .As<IBlobReader>()
                .SingleInstance()
                .WithParameter("container", _settings.InputContainer)
                .WithParameter("blobConnectionString", _settings.InputBlobConnString);

            builder.RegisterType<BlobSaver>()
                .As<IBlobSaver>()
                .SingleInstance()
                .WithParameter("blobConnectionString", _settings.OutputBlobConnString)
                .WithParameter("rootContainer", _settings.InputContainer);

            builder.RegisterType<TypeRetriever>()
                .As<ITypeRetriever>()
                .SingleInstance();

            builder.RegisterType<StructureBuilder>()
                .As<ITypeInfo>()
                .As<IStructureBuilder>()
                .SingleInstance()
                .WithParameter("processingType", _settings.ProcessingType)
                .WithParameter("nugetPackageName", _settings.NugetPackage)
                .WithParameter("instanceTag", _instanceTag)
                .WithParameter("excludedPropertiesMap", _settings.ExcludedPropertiesMap)
                .WithParameter("idPropertiesMap", _settings.IdPropertiesMap)
                .WithParameter("relationPropertiesMap", _settings.RelationPropertiesMap);

            var messageMode = (MessageMode)Enum.Parse(typeof(MessageMode), _settings.MessageMode);

            builder.RegisterType<MessageProcessor>()
                .As<IMessageProcessor>()
                .SingleInstance()
                .WithParameter("processingType", _settings.ProcessingType)
                .WithParameter("nugetPackageName", _settings.NugetPackage)
                .WithParameter("skipCorrupted", _settings.SkipCorrupted ?? false)
                .WithParameter("messageMode", messageMode);

            builder.RegisterType<BlobProcessor>()
                .As<IBlobProcessor>()
                .SingleInstance()
                .WithParameter("instanceTag", _instanceTag);

            builder.RegisterType<PeriodicalHandler>()
                .As<IMainHandler>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.BlobScanPeriod));
        }
    }
}

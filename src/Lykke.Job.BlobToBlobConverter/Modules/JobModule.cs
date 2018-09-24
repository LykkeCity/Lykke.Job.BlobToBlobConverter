using System;
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
                .AutoActivate()
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

            var messageMode = (MessageMode)Enum.Parse(typeof(MessageMode), _settings.MessageMode);
            builder.RegisterType<TypeRetriever>()
                .As<IMessageTypeResolver>()
                .As<IProcessingTypeResolver>()
                .SingleInstance()
                .WithParameter("processingTypeName", _settings.ProcessingType)
                .WithParameter("nugetPackageName", _settings.NugetPackage)
                .WithParameter("messageMode", messageMode);

            builder.RegisterType<StructureBuilder>()
                .As<ITypeInfo>()
                .As<IStructureBuilder>()
                .SingleInstance()
                .WithParameter("instanceTag", _instanceTag)
                .WithParameter("excludedPropertiesMap", _settings.ExcludedPropertiesMap)
                .WithParameter("idPropertiesMap", _settings.IdPropertiesMap)
                .WithParameter("relationPropertiesMap", _settings.RelationPropertiesMap);

            builder.RegisterType<MessageProcessor>()
                .As<IMessageProcessor>()
                .SingleInstance()
                .WithParameter("skipCorrupted", _settings.SkipCorrupted ?? false);

            builder.RegisterType<BlobProcessor>()
                .As<IBlobProcessor>()
                .SingleInstance()
                .WithParameter("instanceTag", _instanceTag)
                .WithParameter("lastBlob", _settings.LastBlob);

            builder.RegisterType<PeriodicalHandler>()
                .As<IStartStop>()
                .SingleInstance()
                .WithParameter(TypedParameter.From(_settings.BlobScanPeriod));
        }
    }
}

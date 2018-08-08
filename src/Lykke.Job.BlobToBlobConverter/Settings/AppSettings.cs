using Lykke.SettingsReader.Attributes;
using System;
using System.Collections.Generic;

namespace Lykke.Job.BlobToBlobConverter.Settings
{
    public class AppSettings
    {
        public BlobToBlobConverterSettings BlobToBlobConverterJob { get; set; }

        public SlackNotificationsSettings SlackNotifications { get; set; }

        [Optional]
        public MonitoringServiceClientSettings MonitoringServiceClient { get; set; }
    }

    public class SlackNotificationsSettings
    {
        public AzureQueuePublicationSettings AzureQueue { get; set; }
    }

    public class AzureQueuePublicationSettings
    {
        public string ConnectionString { get; set; }

        public string QueueName { get; set; }
    }

    public class MonitoringServiceClientSettings
    {
        [HttpCheck("api/isalive", false)]
        public string MonitoringServiceUrl { get; set; }
    }

    public class BlobToBlobConverterSettings
    {
        [AzureTableCheck]
        public string LogsConnString { get; set; }

        [AzureBlobCheck]
        public string InputBlobConnString { get; set; }

        public string InputContainer { get; set; }

        [AzureBlobCheck]
        public string OutputBlobConnString { get; set; }

        public TimeSpan BlobScanPeriod { get; set; }

        public string NugetPackage { get; set; }

        public string ProcessingType { get; set; }

        public string MessageMode { get; set; }

        [Optional]
        public bool? SkipCorrupted { get; set; }

        [Optional]
        public Dictionary<string, List<string>> ExcludedPropertiesMap { get; set; }

        [Optional]
        public Dictionary<string, string> IdPropertiesMap { get; set; }

        [Optional]
        public Dictionary<string, string> RelationPropertiesMap { get; set; }
    }
}

﻿using System;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using AzureStorage.Tables;
using Common.Log;
using Lykke.Common.ApiLibrary.Middleware;
using Lykke.Common.ApiLibrary.Swagger;
using Lykke.Common.Api.Contract.Responses;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using Lykke.Job.BlobToBlobConverter.Settings;
using Lykke.Job.BlobToBlobConverter.Modules;
using Lykke.Logs;
using Lykke.Logs.Slack;
using Lykke.SettingsReader;
using Lykke.MonitoringServiceApiCaller;
using Lykke.SlackNotification.AzureQueue;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Lykke.Job.BlobToBlobConverter
{
    public class Startup
    {
        private string _monitoringServiceUrl;

        public IHostingEnvironment Environment { get; }
        public IContainer ApplicationContainer { get; private set; }
        public IConfigurationRoot Configuration { get; }
        public ILog Log { get; private set; }

        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddEnvironmentVariables();

            Configuration = builder.Build();
            Environment = env;
        }

        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
            try
            {
                services.AddMvc()
                    .AddJsonOptions(options =>
                    {
                        options.SerializerSettings.ContractResolver =
                            new Newtonsoft.Json.Serialization.DefaultContractResolver();
                    });

                services.AddSwaggerGen(options =>
                {
                    options.DefaultLykkeConfiguration("v1", "BlobToBlobConverter API");
                });

                var builder = new ContainerBuilder();
                var appSettings = Configuration.LoadSettings<AppSettings>();
                if (appSettings.CurrentValue.MonitoringServiceClient != null)
                    _monitoringServiceUrl = appSettings.CurrentValue.MonitoringServiceClient.MonitoringServiceUrl;

                Log = CreateLogWithSlack(services, appSettings);

                string instanceTag = Configuration["InstanceTag"];

                builder.RegisterModule(new JobModule(appSettings.CurrentValue.BlobToBlobConverterJob, Log, instanceTag));

                builder.Populate(services);

                ApplicationContainer = builder.Build();

                return new AutofacServiceProvider(ApplicationContainer);
            }
            catch (Exception ex)
            {
                Log?.WriteFatalError(nameof(Startup), nameof(ConfigureServices), ex);
                throw;
            }
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IApplicationLifetime appLifetime)
        {
            try
            {
                if (env.IsDevelopment())
                {
                    app.UseDeveloperExceptionPage();
                }

                app.UseLykkeForwardedHeaders();
                app.UseLykkeMiddleware("BlobToBlobConverter", ex => new ErrorResponse {ErrorMessage = "Technical problem"});

                app.UseMvc();
                app.UseSwagger(c =>
                {
                    c.PreSerializeFilters.Add((swagger, httpReq) => swagger.Host = httpReq.Host.Value);
                });
                app.UseSwaggerUI(x =>
                {
                    x.RoutePrefix = "swagger/ui";
                    x.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
                });
                app.UseStaticFiles();

                appLifetime.ApplicationStarted.Register(() => StartApplication().GetAwaiter().GetResult());
                appLifetime.ApplicationStopping.Register(() => StopApplication().GetAwaiter().GetResult());
                appLifetime.ApplicationStopped.Register(() => CleanUp().GetAwaiter().GetResult());
            }
            catch (Exception ex)
            {
                Log?.WriteFatalError(nameof(Startup), nameof(Configure), ex);
                throw;
            }
        }

        private async Task StartApplication()
        {
            try
            {
                // NOTE: Job not yet recieve and process IsAlive requests here

                await ApplicationContainer.Resolve<IStartupManager>().StartAsync();
                await Log.WriteMonitorAsync("", Program.EnvInfo, "Started");

#if (!DEBUG)
                await AutoRegistrationInMonitoring.RegisterAsync(Configuration, _monitoringServiceUrl, Log);
#endif
            }
            catch (Exception ex)
            {
                await Log.WriteFatalErrorAsync(nameof(Startup), nameof(StartApplication), "", ex);
                throw;
            }
        }

        private async Task StopApplication()
        {
            try
            {
                // NOTE: Job still can recieve and process IsAlive requests here, so take care about it if you add logic here.

                await ApplicationContainer.Resolve<IShutdownManager>().StopAsync();
            }
            catch (Exception ex)
            {
                if (Log != null)
                {
                    await Log.WriteFatalErrorAsync(nameof(Startup), nameof(StopApplication), "", ex);
                }
                throw;
            }
        }

        private async Task CleanUp()
        {
            try
            {
                // NOTE: Job can't recieve and process IsAlive requests here, so you can destroy all resources
                
                if (Log != null)
                {
                    await Log.WriteMonitorAsync("", Program.EnvInfo, "Terminating");
                }
                
                ApplicationContainer.Dispose();
            }
            catch (Exception ex)
            {
                if (Log != null)
                {
                    await Log.WriteFatalErrorAsync(nameof(Startup), nameof(CleanUp), "", ex);
                    (Log as IDisposable)?.Dispose();
                }
                throw;
            }
        }

        private static ILog CreateLogWithSlack(IServiceCollection services, IReloadingManager<AppSettings> settings)
        {
            var consoleLogger = new LogToConsole();
            var aggregateLogger = new AggregateLogger();

            aggregateLogger.AddLog(consoleLogger);

            var dbLogConnectionStringManager = settings.Nested(x => x.BlobToBlobConverterJob.LogsConnString);
            var dbLogConnectionString = dbLogConnectionStringManager.CurrentValue;

            if (string.IsNullOrEmpty(dbLogConnectionString))
            {
                consoleLogger.WriteWarningAsync(nameof(Startup), nameof(CreateLogWithSlack), "Table loggger is not inited").Wait();
                return aggregateLogger;
            }

            if (dbLogConnectionString.StartsWith("${") && dbLogConnectionString.EndsWith("}"))
                throw new InvalidOperationException($"LogsConnString {dbLogConnectionString} is not filled in settings");

            var persistenceManager = new LykkeLogToAzureStoragePersistenceManager(
                AzureTableStorage<LogEntity>.Create(dbLogConnectionStringManager, "BlobToBlobConverterLog", consoleLogger),
                consoleLogger);

            // Creating slack notification service, which logs own azure queue processing messages to aggregate log
            var slackService = services.UseSlackNotificationsSenderViaAzureQueue(new AzureQueueIntegration.AzureQueueSettings
            {
                ConnectionString = settings.CurrentValue.SlackNotifications.AzureQueue.ConnectionString,
                QueueName = settings.CurrentValue.SlackNotifications.AzureQueue.QueueName
            }, aggregateLogger);

            var slackNotificationsManager = new LykkeLogToAzureSlackNotificationsManager(slackService, consoleLogger);

            // Creating azure storage logger, which logs own messages to concole log
            var azureStorageLogger = new LykkeLogToAzureStorage(
                persistenceManager,
                slackNotificationsManager,
                consoleLogger);
            azureStorageLogger.Start();
            aggregateLogger.AddLog(azureStorageLogger);

            var logToSlack = LykkeLogToSlack.Create(slackService, "Bridges", LogLevel.Error | LogLevel.FatalError | LogLevel.Warning);
            aggregateLogger.AddLog(logToSlack);

            return aggregateLogger;
        }
    }
}

﻿using Lykke.Job.BlobToBlobConverter.Core.Domain.Health;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using System.Collections.Generic;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    // NOTE: See https://lykkex.atlassian.net/wiki/spaces/LKEWALLET/pages/35755585/Add+your+app+to+Monitoring
    public class HealthService : IHealthService
    {
        // TODO: Feel free to add properties, which contains your helath metrics, and use it in monitoring layer or in IsAlive API endpoint

        public string GetHealthViolationMessage()
        {
            // TODO: Check gathered health statistics, and return appropriate health violation message, or NULL if job hasn't critical errors
            return null;
        }

        public IEnumerable<HealthIssue> GetHealthIssues()
        {
            // TODO: Check gathered health statistics, and add appropriate health issues message to issues

            return new List<HealthIssue>();
        }

        // TODO: Place health tracing methods here
    }
}

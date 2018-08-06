using JetBrains.Annotations;
using System;

namespace Lykke.Job.BlobToBlobConverter.Common.Helpers
{
    [PublicAPI]
    public static class DateTimeConverter
    {
        private const string _format = "yyyy-MM-dd HH:mm:ss.fff";

        public static string Convert(DateTime dateTime)
        {
            return dateTime.ToString(_format);
        }
    }
}

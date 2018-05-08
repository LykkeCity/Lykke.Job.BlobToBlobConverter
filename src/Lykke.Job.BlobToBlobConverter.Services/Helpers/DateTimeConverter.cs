using System;

namespace Lykke.Job.BlobToBlobConverter.Services.Helpers
{
    internal static class DateTimeConverter
    {
        private const string _format = "yyyy-MM-dd HH:mm:ss.fff";

        internal static string Convert(DateTime dateTime)
        {
            return dateTime.ToString(_format);
        }
    }
}

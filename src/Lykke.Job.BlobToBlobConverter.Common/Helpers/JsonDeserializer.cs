using System;
using System.IO;
using Common.Log;
using JetBrains.Annotations;
using Newtonsoft.Json;

namespace Lykke.Job.BlobToBlobConverter.Common.Helpers
{
    [PublicAPI]
    public static class JsonDeserializer
    {
        private static readonly JsonSerializer _serializer = JsonSerializer.Create(
            new JsonSerializerSettings
            {
                DateTimeZoneHandling = DateTimeZoneHandling.Utc
            });

        public static bool TryDeserialize<T>(
            byte[] data,
            ILog log,
            out T result)
        {
            try
            {
                using (var stream = new MemoryStream(data))
                using (var reader = new StreamReader(stream, true))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    result = _serializer.Deserialize<T>(jsonReader);
                    return true;
                }
            }
            catch (Exception e)
            {
                log.WriteWarning(nameof(JsonDeserializer), nameof(TryDeserialize), e.Message);
                result = default(T);
                return false;
            }
        }

        public static bool TryDeserialize(
            byte[] data,
            Type type,
            ILog log,
            out object result)
        {
            try
            {
                using (var stream = new MemoryStream(data))
                using (var reader = new StreamReader(stream, true))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    result = _serializer.Deserialize(jsonReader, type);
                    return true;
                }
            }
            catch (Exception e)
            {
                log.WriteWarning(nameof(JsonDeserializer), nameof(TryDeserialize), e.Message);
                result = null;
                return false;
            }
        }
    }
}

using JetBrains.Annotations;
using Newtonsoft.Json;
using System;
using System.IO;

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

        public static bool TryDeserialize<T>(byte[] data, out T result)
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
            catch (Exception)
            {
                result = default(T);
                return false;
            }
        }

        public static bool TryDeserialize(byte[] data, Type type, out object result)
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
            catch (Exception)
            {
                result = null;
                return false;
            }
        }
    }
}

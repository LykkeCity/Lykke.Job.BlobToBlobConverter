using System;
using System.IO;
using Common.Log;
using JetBrains.Annotations;
using ProtoBuf;

namespace Lykke.Job.BlobToBlobConverter.Common.Helpers
{
    [PublicAPI]
    public static class ProtobufDeserializer
    {
        public static bool TryDeserialize<T>(
            byte[] data,
            ILog log,
            out T result)
        {
            try
            {
                using (var memStream = new MemoryStream(data))
                {
                    result = Serializer.Deserialize<T>(memStream);
                    return true;
                }
            }
            catch (Exception e)
            {
                log.WriteWarning(nameof(ProtobufDeserializer), nameof(TryDeserialize), e.Message);
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
                using (var memStream = new MemoryStream(data))
                {
                    result = Serializer.Deserialize(type, memStream);
                    return true;
                }
            }
            catch (Exception e)
            {
                log.WriteWarning(nameof(ProtobufDeserializer), nameof(TryDeserialize), e.Message);
                result = null;
                return false;
            }
        }
    }
}

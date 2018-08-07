using JetBrains.Annotations;
using ProtoBuf;
using System;
using System.IO;

namespace Lykke.Job.BlobToBlobConverter.Common.Helpers
{
    [PublicAPI]
    public static class ProtobufDeserializer
    {
        public static bool TryDeserialize<T>(byte[] data, out T result)
        {
            try
            {
                using (var memStream = new MemoryStream(data))
                {
                    result = Serializer.Deserialize<T>(memStream);
                    return true;
                }
            }
            catch
            {
                result = default(T);
                return false;
            }
        }

        public static bool TryDeserialize(byte[] data, Type type, out object result)
        {
            try
            {
                using (var memStream = new MemoryStream(data))
                {
                    result = Serializer.Deserialize(type, memStream);
                    return true;
                }
            }
            catch
            {
                result = null;
                return false;
            }
        }
    }
}

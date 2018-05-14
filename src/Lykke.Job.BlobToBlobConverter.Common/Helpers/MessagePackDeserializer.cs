using MessagePack;
using System;

namespace Lykke.Job.BlobToBlobConverter.Common.Helpers
{
    public static class MessagePackDeserializer
    {
        public static bool TryDeserialize<T>(byte[] data, out T result)
        {
            try
            {
                result = MessagePackSerializer.Deserialize<T>(data);
                return true;
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
                result = MessagePackSerializer.NonGeneric.Deserialize(type, data);
                return true;
            }
            catch
            {
                result = null;
                return false;
            }
        }
    }
}

using System;
using Common.Log;
using JetBrains.Annotations;
using MessagePack;

namespace Lykke.Job.BlobToBlobConverter.Common.Helpers
{
    [PublicAPI]
    public static class MessagePackDeserializer
    {
        public static bool TryDeserialize<T>(
            byte[] data,
            ILog log,
            out T result)
        {
            try
            {
                result = MessagePackSerializer.Deserialize<T>(data);
                return true;
            }
            catch (Exception e)
            {
                log.WriteWarning(nameof(MessagePackDeserializer), nameof(TryDeserialize), e.Message);
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
                result = MessagePackSerializer.NonGeneric.Deserialize(type, data);
                return true;
            }
            catch (Exception e)
            {
                log.WriteWarning(nameof(MessagePackDeserializer), nameof(TryDeserialize), e.Message);
                result = null;
                return false;
            }
        }
    }
}

using MessagePack;

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
    }
}

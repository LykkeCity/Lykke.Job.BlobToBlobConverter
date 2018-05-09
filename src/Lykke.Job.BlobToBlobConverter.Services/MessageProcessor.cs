using Common;
using Common.Log;
using Lykke.Job.BlobToBlobConverter.Core;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using Lykke.Job.BlobToBlobConverter.Services.Helpers;
using MessagePack;
using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class MessageProcessor : IMessageProcessor
    {
        private const string _idPropertyName = "Id";

        private readonly Type _type;
        private readonly Type _messageType;
        private readonly ILog _log;
        private readonly Dictionary<string, List<string>> _excludedPropertiesMap;
        private readonly Dictionary<string, string> _idPropertiesMap;
        private readonly JsonSerializer _serializer = JsonSerializer.Create(
            new JsonSerializerSettings
            {
                DateTimeZoneHandling = DateTimeZoneHandling.Utc
            });
        private readonly Dictionary<Type, (List<PropertyInfo>, List<PropertyInfo>)> _propertiesMap = new Dictionary<Type, (List<PropertyInfo>, List<PropertyInfo>)>();
        private readonly MethodInfo _isValidMethod;

        private bool? _deserializeMethod;
        private Dictionary<string, List<string>> _objectData;

        public MessageProcessor(
            ITypeRetriever typeRetriever,
            string processingType,
            string nugetPackageName,
            MessageMode messageMode,
            Dictionary<string, List<string>> excludedPropertiesMap,
            Dictionary<string, string> idPropertiesMap,
            ILog log)
        {
            _type = typeRetriever.RetrieveTypeAsync(processingType, nugetPackageName).GetAwaiter().GetResult();
            _log = log;
            _excludedPropertiesMap = excludedPropertiesMap;
            _idPropertiesMap = idPropertiesMap;
            _isValidMethod = _type.GetMethod("IsValid");
            switch(messageMode)
            {
                case MessageMode.Single:
                    _messageType = _type;
                    break;
                case MessageMode.List:
                    _messageType = typeof(List<>).MakeGenericType(_type);
                    break;
                case MessageMode.Array:
                    _messageType = _type.MakeArrayType();
                    break;
            }
        }

        public Dictionary<string, string> GetMappingStructure()
        {
            var result = new Dictionary<string, string>();

            AddStructureLevel(
                _type,
                null,
                false,
                null,
                result);

            return result;
        }

        public void StartBlobProcessing()
        {
            _objectData = new Dictionary<string, List<string>>();
        }

        public bool TryProcessMessage(byte[] data)
        {
            object obj;
            var result = TryDeserialize(data, out obj);
            if (!result)
                return false;

            var items = obj as IEnumerable;
            if (items != null)
            {
                foreach (var item in items)
                {
                    ProcessTypeItem(item);
                }
            }
            else
            {
                ProcessTypeItem(obj);
            }

            return true;
        }

        public async Task FinishBlobProcessingAsync(Func<string, List<string>, Task> messagesHandler)
        {
            foreach (var convertedPair in _objectData)
            {
                if (convertedPair.Value.Count > 0)
                    await messagesHandler(convertedPair.Key, convertedPair.Value);
            }
        }

        private void ProcessTypeItem(object obj)
        {
            if (_isValidMethod != null)
            {
                bool isValid = (bool)_isValidMethod.Invoke(obj, null);
                if (!isValid)
                    _log.WriteWarning(nameof(MessageProcessor), nameof(TryProcessMessage), $"{_type.FullName} {obj.ToJson()} is invalid!");
            }

            AddValueLevel(
                obj,
                null,
                null,
                null);
        }

        private bool TryDeserialize(byte[] data, out object result)
        {
            if (_deserializeMethod.HasValue)
            {
                if (_deserializeMethod.Value)
                    return TryJsonDeserialize(data, out result);
                else
                    return TryMsgPackDeserialize(data, out result);
            }
            bool success = TryJsonDeserialize(data, out result);
            if (success)
            {
                _deserializeMethod = true;
                return true;
            }
            success = TryMsgPackDeserialize(data, out result);
            if (success)
                _deserializeMethod = false;
            return success;
        }

        private bool TryJsonDeserialize(byte[] data, out object result)
        {
            try
            {
                using (var stream = new MemoryStream(data))
                using (var reader = new StreamReader(stream, true))
                using (var jsonReader = new JsonTextReader(reader))
                {
                    result = _serializer.Deserialize(jsonReader, _messageType);
                    return true;
                }
            }
            catch (Exception)
            {
                result = null;
                return false;
            }
        }

        private bool TryMsgPackDeserialize(byte[] data, out object result)
        {
            try
            {
                result = MessagePackSerializer.NonGeneric.Deserialize(_messageType, data);
                return true;
            }
            catch
            {
                result = null;
                return false;
            }
        }

        private void AddValueLevel(
            object obj,
            string parentLevel,
            string parentId,
            string parentTypeName)
        {
            if (obj == null)
                return;

            Type type = obj.GetType();

            string level = _objectData.ContainsKey(type.Name)
                ? $"{parentLevel}-{type.Name}"
                : type.Name;

            StringBuilder sb = new StringBuilder();

            string id = null;
            var idPropertyName = _idPropertiesMap.ContainsKey(type.Name) ? _idPropertiesMap[type.Name] : null;
            (var valueProperties, var childrenEntityProperties) = _propertiesMap[type];

            if (parentId != null && parentTypeName != null)
            {
                var parentIdPropertyName = $"{parentTypeName}{_idPropertyName}";
                var parentIdProperty = valueProperties.Find(v => v.Name == parentIdPropertyName);
                if (parentIdProperty == null)
                {
                    sb.Append(parentId);
                    sb.Append(',');
                }
            }

            for (int i = 0; i < valueProperties.Count; ++i)
            {
                var valueProperty = valueProperties[i];
                if (i > 0)
                    sb.Append(',');
                object value = valueProperty.GetValue(obj);
                if (valueProperty.Name == _idPropertyName || valueProperty.Name == idPropertyName)
                    id = value.ToString();
                string strValue = string.Empty;
                if (value != null)
                {
                    if (valueProperty.PropertyType == typeof(DateTime))
                        strValue = DateTimeConverter.Convert((DateTime)value);
                    else if (valueProperty.PropertyType == typeof(DateTime?))
                        strValue = DateTimeConverter.Convert(((DateTime?)value).Value);
                    else
                        strValue = value.ToString();
                }
                sb.Append(strValue);
            }

            if (_objectData.ContainsKey(level))
                _objectData[level].Add(sb.ToString());
            else
                _objectData.Add(level, new List<string> { sb.ToString() });

            foreach (var childrenEntityProperty in childrenEntityProperties)
            {
                object value = childrenEntityProperty.GetValue(obj);
                var items = value as IEnumerable;
                if (items != null)
                {
                    foreach (var item in items)
                    {
                        AddValueLevel(
                            value,
                            level,
                            id,
                            type.Name);
                    }
                }
                else if (value != null)
                {
                    AddValueLevel(
                        value,
                        level,
                        id,
                        type.Name);
                }
            }
        }

        private void AddStructureLevel(
            Type type,
            string parentLevel,
            bool parentHasId,
            string parentTypeName,
            Dictionary<string, string> dictionary)
        {
            string level = dictionary.ContainsKey(type.Name)
                ? $"{parentTypeName}-{type.Name}"
                : type.Name;

            StringBuilder sb = new StringBuilder();

            if (parentHasId && parentTypeName != null)
            {
                var parentIdPropertyName = $"{parentTypeName}{_idPropertyName}";
                var parentIdProperty = type.GetProperty(parentIdPropertyName);
                if (parentIdProperty == null)
                    sb.Append(parentIdPropertyName);
            }

            var excludedProperties = _excludedPropertiesMap.ContainsKey(type.Name) ? _excludedPropertiesMap[type.Name] : new List<string>(0);
            var idPropertyName = _idPropertiesMap.ContainsKey(type.Name) ? _idPropertiesMap[type.Name] : null;
            bool hasId = false;
            var valueProperties = new List<PropertyInfo>();
            var childrenEntityProperties = new List<PropertyInfo>();
            var notSimpleProperties = new List<Type>();
            var topLevelProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var property in topLevelProperties)
            {
                if (excludedProperties.Contains(property.Name))
                    continue;

                if (!hasId && (property.Name == _idPropertyName || property.Name == idPropertyName))
                    hasId = true;
                if (property.PropertyType.IsClass
                    && property.PropertyType != typeof(string))
                {
                    if (property.PropertyType.IsArray)
                    {
                        notSimpleProperties.Add(property.PropertyType.GetElementType());
                    }
                    else if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(List<>))
                    {
                        notSimpleProperties.Add(property.PropertyType.GetGenericArguments()[0]);
                    }
                    else
                    {
                        notSimpleProperties.Add(property.PropertyType);
                    }
                    childrenEntityProperties.Add(property);
                }
                else
                {
                    if (sb.Length > 0)
                        sb.Append(',');
                    sb.Append(property.Name);
                    valueProperties.Add(property);
                }
            }

            if (!_propertiesMap.ContainsKey(type))
                _propertiesMap.Add(type, (valueProperties, childrenEntityProperties));

            dictionary.Add(level, sb.ToString());

            foreach (var notSimpleType in notSimpleProperties)
            {
                AddStructureLevel(
                    notSimpleType,
                    level,
                    hasId,
                    type.Name,
                    dictionary);
            }
        }
    }
}

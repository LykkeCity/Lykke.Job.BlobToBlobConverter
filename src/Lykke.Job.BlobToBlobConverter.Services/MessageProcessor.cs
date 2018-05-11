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
using System.Linq;
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
        private readonly Dictionary<Type, (List<PropertyInfo>, List<PropertyInfo>, List<PropertyInfo>)> _propertiesMap
            = new Dictionary<Type, (List<PropertyInfo>, List<PropertyInfo>, List<PropertyInfo>)>();
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

            if (_isValidMethod != null)
            {
                bool isValid = (bool)_isValidMethod.Invoke(obj, null);
                if (!isValid)
                    _log.WriteWarning(nameof(MessageProcessor), nameof(TryProcessMessage), $"{_type.FullName} {obj.ToJson()} is invalid!");
            }

            ProcessTypeItem(obj, null, null);

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

        private string ProcessTypeItem(
            object obj,
            string parentId,
            string parentTypeName)
        {
            string result = null;
            var items = obj as IEnumerable;
            if (items != null)
            {
                foreach (var item in items)
                {
                    result = AddValueLevel(
                        item,
                        parentId,
                        parentTypeName);
                }
            }
            else
            {
                result = AddValueLevel(
                    obj,
                    parentId,
                    parentTypeName);
            }
            return result;
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

        private string AddValueLevel(
            object obj,
            string parentId,
            string parentTypeName)
        {
            if (obj == null)
                return null;

            Type type = obj.GetType();
            string typeName = type.Name;

            var idPropertyName = _idPropertiesMap.ContainsKey(typeName) ? _idPropertiesMap[typeName] : null;
            string parentIdPropertyName = parentTypeName != null ? $"{parentTypeName}{_idPropertyName}" : null;
            (var valueProperties, var oneToOneChildrenProperties, var oneToManyChildrenProperties) = _propertiesMap[type];

            string id = null;
            if (valueProperties.Count > 0)
            {
                StringBuilder sb = new StringBuilder();
                bool hasParentIdProperty = false;
                for (int i = 0; i < valueProperties.Count; ++i)
                {
                    var valueProperty = valueProperties[i];
                    object value = valueProperty.GetValue(obj);
                    if (valueProperty.Name == _idPropertyName || valueProperty.Name == idPropertyName)
                    {
                        if (value == null)
                            throw new InvalidOperationException($"'{valueProperty.Name}' property can't be null");
                        id = value.ToString();
                    }
                    else
                    {
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
                        if (parentIdPropertyName != null && valueProperty.Name == parentIdPropertyName)
                            hasParentIdProperty = true;
                        if (sb.Length > 0 || i > 0 && (i != 1 || id == null))
                            sb.Append(',');
                        sb.Append(strValue);
                    }
                }

                if (parentId != null && !hasParentIdProperty)
                    sb.Insert(0, $"{parentId},");
                if (id != null)
                    sb.Insert(0, $"{id},");

                if (_objectData.ContainsKey(typeName))
                    _objectData[typeName].Add(sb.ToString());
                else
                    _objectData.Add(typeName, new List<string> { sb.ToString() });
            }
            else
            {
                id = parentId;
                typeName = parentTypeName;
            }

            string childId = null;
            foreach (var childrenEntityProperty in oneToOneChildrenProperties)
            {
                object value = childrenEntityProperty.GetValue(obj);
                if (value == null)
                    continue;

                childId = ProcessTypeItem(
                    value,
                    id,
                    typeName);
            }

            if (oneToOneChildrenProperties.Count == 1 && childId != null)
                id = childId;

            foreach (var childrenEntityProperty in oneToManyChildrenProperties)
            {
                object value = childrenEntityProperty.GetValue(obj);
                if (value == null)
                    continue;

                var items = value as IEnumerable;
                if (items == null)
                    throw new InvalidOperationException($"Couldn't cast value '{value}' of property {childrenEntityProperty.Name} from {typeName} to IEnumerable");
                foreach (var item in items)
                {
                    ProcessTypeItem(
                        item,
                        id,
                        typeName);
                }
            }

            return id;
        }

        private bool AddStructureLevel(
            Type type,
            bool parentHasId,
            string parentTypeName,
            Dictionary<string, string> dictionary)
        {
            string typeName = type.Name;

            StringBuilder sb = new StringBuilder();

            var excludedProperties = _excludedPropertiesMap.ContainsKey(typeName) ? _excludedPropertiesMap[typeName] : new List<string>(0);
            var idPropertyName = _idPropertiesMap.ContainsKey(typeName) ? _idPropertiesMap[typeName] : null;
            bool hasId = false;
            var valueProperties = new List<PropertyInfo>();
            var oneToOneChildrenProperties = new List<(PropertyInfo, Type)>();
            var oneToManyChildrenProperties = new List<(PropertyInfo, Type)>();
            var topLevelProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var property in topLevelProperties)
            {
                if (excludedProperties.Contains(property.Name))
                    continue;

                if (property.PropertyType.IsClass && property.PropertyType != typeof(string))
                {
                    if (property.PropertyType.IsArray)
                        oneToManyChildrenProperties.Add((property, property.PropertyType.GetElementType()));
                    else if (property.PropertyType.IsGenericType && property.PropertyType.GetGenericTypeDefinition() == typeof(List<>))
                        oneToManyChildrenProperties.Add((property, property.PropertyType.GetGenericArguments()[0]));
                    else
                        oneToOneChildrenProperties.Add((property, property.PropertyType));
                }
                else
                {
                    if (!hasId && (property.Name == _idPropertyName || property.Name == idPropertyName))
                    {
                        hasId = true;
                    }
                    else
                    {
                        if (sb.Length > 0)
                            sb.Append(',');
                        sb.Append(property.Name);
                    }
                    valueProperties.Add(property);
                }
            }

            if (_propertiesMap.ContainsKey(type))
                throw new InvalidOperationException($"Type {typeName} is found more than once");

            _propertiesMap.Add(
                type,
                (
                    valueProperties,
                    oneToOneChildrenProperties.Select(i => i.Item1).ToList(),
                    oneToManyChildrenProperties.Select(i => i.Item1).ToList()
                ));

            if (sb.Length > 0)
            {
                if (parentHasId && parentTypeName != null)
                {
                    var parentIdPropertyName = $"{parentTypeName}{_idPropertyName}";
                    var parentIdProperty = type.GetProperty(parentIdPropertyName);
                    if (parentIdProperty == null)
                        sb.Insert(0, $"{parentIdPropertyName},");
                }
                if (hasId)
                    sb.Insert(0, $"{_idPropertyName},");
                dictionary.Add(typeName, sb.ToString());
            }

            if (valueProperties.Count == 0)
            {
                hasId = parentHasId;
                typeName = parentTypeName;
            }

            bool childHasId = false;
            foreach (var childTypePair in oneToOneChildrenProperties)
            {
                childHasId = AddStructureLevel(
                    childTypePair.Item2,
                    hasId,
                    typeName,
                    dictionary);
            }

            if (oneToOneChildrenProperties.Count == 1 && childHasId)
                typeName = oneToOneChildrenProperties[0].Item2.Name;
            else
                childHasId = hasId;
            foreach (var childTypePair in oneToManyChildrenProperties)
            {
                AddStructureLevel(
                    childTypePair.Item2,
                    childHasId,
                    typeName,
                    dictionary);
            }

            return hasId;
        }
    }
}

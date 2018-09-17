using Common;
using Common.Log;
using Lykke.Job.BlobToBlobConverter.Core;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using Lykke.Job.BlobToBlobConverter.Common.Helpers;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class MessageProcessor : IMessageProcessor
    {
        private const int _maxBatchCount = 1000000;

        private readonly Type _type;
        private readonly Type _messageType;
        private readonly bool _skipCorrupted;
        private readonly ILog _log;
        private readonly ITypeInfo _typeInfo;
        private readonly MethodInfo _isValidMethod;

        private SerializationFormat? _deserializeFormat;
        private Dictionary<string, List<string>> _objectsData;
        private Func<string, List<string>, Task> _messagesHandler;

        public MessageProcessor(
            string processingType,
            string nugetPackageName,
            MessageMode messageMode,
            bool skipCorrupted,
            ITypeRetriever typeRetriever,
            ITypeInfo typeInfo,
            ILog log)
        {
            _type = typeRetriever.RetrieveTypeAsync(processingType, nugetPackageName).GetAwaiter().GetResult();
            _log = log;
            _typeInfo = typeInfo;
            _skipCorrupted = skipCorrupted;
            _isValidMethod = _type.GetMethod("IsValid", new Type[0]);
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

        public void StartBlobProcessing(Func<string, List<string>, Task> messagesHandler)
        {
            _messagesHandler = messagesHandler;
            _objectsData = new Dictionary<string, List<string>>();
        }

        public async Task FinishBlobProcessingAsync()
        {
            await SaveObjectsDataAsync();
        }

        public async Task<bool> TryProcessMessageAsync(byte[] data)
        {
            var result = TryDeserialize(data, out var obj);
            if (!result)
                return false;

            try
            {
                if (_isValidMethod != null)
                {
                    bool isValid = (bool)_isValidMethod.Invoke(obj, null);
                    if (!isValid)
                        _log.WriteWarning(nameof(MessageProcessor), nameof(TryProcessMessageAsync), $"{_type.FullName} {obj.ToJson()} is invalid!");
                }

                ProcessTypeItem(
                    obj,
                    null,
                    null);

                if (_objectsData.Values.Any(v => v.Count >= _maxBatchCount))
                {
                    await SaveObjectsDataAsync();
                    _objectsData.Clear();
                }
            }
            catch (Exception ex)
            {
                _log.WriteError(nameof(TryProcessMessageAsync), obj, ex);
                if (_skipCorrupted)
                    _log.WriteWarning(nameof(TryProcessMessageAsync), obj, "Skipped corrupted message");
                else
                    throw;
            }

            return true;
        }

        private async Task SaveObjectsDataAsync()
        {
            foreach (var convertedPair in _objectsData)
            {
                if (convertedPair.Value.Count > 0)
                    await _messagesHandler(convertedPair.Key, convertedPair.Value);
            }
        }

        private void ProcessTypeItem(
            object obj,
            Type parentType,
            string parentId)
        {
            if (obj is IEnumerable items)
            {
                foreach (var item in items)
                {
                    AddValueLevel(
                        item,
                        parentType,
                        parentId);
                }
            }
            else
            {
                AddValueLevel(
                    obj,
                    parentType,
                    parentId);
            }
        }

        private bool TryDeserialize(byte[] data, out object result)
        {
            if (_deserializeFormat.HasValue)
            {
                switch (_deserializeFormat.Value)
                {
                    case SerializationFormat.Json:
                        return JsonDeserializer.TryDeserialize(
                            data,
                            _messageType,
                            _log,
                            out result);
                    case SerializationFormat.MessagePack:
                        return MessagePackDeserializer.TryDeserialize(data, _messageType, out result);
                    case SerializationFormat.Protobuf:
                        return ProtobufDeserializer.TryDeserialize(data, _messageType, out result);
                    default:
                        throw new NotSupportedException($"Serialization format {_deserializeFormat.Value} is not supported");
                }
            }
            bool success = JsonDeserializer.TryDeserialize(
                data,
                _messageType,
                _log,
                out result);
            if (success)
            {
                _deserializeFormat = SerializationFormat.Json;
                return true;
            }
            success = MessagePackDeserializer.TryDeserialize(data, _messageType, out result);
            if (success)
            {
                _deserializeFormat = SerializationFormat.MessagePack;
                return true;
            }
            success = ProtobufDeserializer.TryDeserialize(data, _messageType, out result);
            if (!success)
                _log.WriteWarning(nameof(TryDeserialize), null, $"Couldn't deserialize message with length {data.Length}");
            return success;
        }

        private void AddValueLevel(
            object obj,
            Type parentType,
            string parentId)
        {
            if (obj == null)
                return;

            Type type = obj.GetType();
            string typeName = type.Name;

            var idPropertyName = _typeInfo.GetIdPropertyName(typeName);
            var typeData = _typeInfo.PropertiesMap[type];

            string id = null;
            if (typeData.ValueProperties.Count > 0)
            {
                StringBuilder sb = new StringBuilder();
                bool hasParentIdProperty = false;
                for (int i = 0; i < typeData.ValueProperties.Count; ++i)
                {
                    var valueProperty = typeData.ValueProperties[i];
                    object value = valueProperty.GetValue(obj);
                    if (valueProperty.Name == _typeInfo.IdPropertyName || valueProperty.Name == idPropertyName)
                    {
                        if (value == null)
                            _log.WriteWarning(nameof(AddValueLevel), obj, $"Id property {valueProperty.Name} of {typeName} is null");
                        id = value?.ToString() ?? string.Empty;
                    }
                    else
                    {
                        string strValue = string.Empty;
                        if (value != null)
                        {
                            if (valueProperty.PropertyType == typeof(DateTime))
                            {
                                strValue = DateTimeConverter.Convert((DateTime)value);
                            }
                            else if (valueProperty.PropertyType == typeof(DateTime?))
                            {
                                strValue = DateTimeConverter.Convert(((DateTime?)value).Value);
                            }
                            else if (StructureBuilder.GenericCollectionTypes.Any(t => t == valueProperty.PropertyType))
                            {
                                var strValues = new List<string>();
                                var enumerable = (IEnumerable) value;
                                foreach(var item in enumerable)
                                {
                                    strValues.Add(item.ToString() ?? string.Empty);
                                }
                                strValue = string.Join(';', strValues);
                            }
                            else
                            {
                                strValue = value.ToString();
                            }
                        }
                        if (typeData.ParentIdPropertyName != null && valueProperty.Name == typeData.ParentIdPropertyName)
                            hasParentIdProperty = true;
                        if (sb.Length > 0 || i > 0 && (i != 1 || id == null))
                            sb.Append(',');
                        sb.Append(strValue);
                    }
                }

                if (parentId != null && !hasParentIdProperty)
                {
                    sb.Insert(0, $"{parentId},");
                }
                else if (sb.Length > 0 && parentType != null && _typeInfo.PropertiesMap[parentType].ValueProperties.Count > 0)
                {
                    _log.WriteWarning(
                        nameof(AddValueLevel),
                        obj,
                        $"Message of type {parentType.Name} doesn't have any identificators that can be used to make relations to its children");
                    sb.Insert(0, ",");
                }

                if (id != null)
                    sb.Insert(0, $"{id},");

                if (_objectsData.ContainsKey(typeName))
                    _objectsData[typeName].Add(sb.ToString());
                else
                    _objectsData.Add(typeName, new List<string> { sb.ToString() });
            }

            if (typeData.OneChildrenProperties.Count == 0 && typeData.ManyChildrenProperties.Count == 0)
                return;

            if (id == null)
                id = GetIdFromChildren(obj, typeData);
            if (id == null)
                id = parentId;
            if (id == null && typeData.RelationProperty != null)
                id = typeData.RelationProperty.GetValue(obj)?.ToString();

            foreach (var childProperty in typeData.OneChildrenProperties)
            {
                object value = childProperty.GetValue(obj);
                if (value == null)
                    continue;

                ProcessTypeItem(
                    value,
                    type,
                    childProperty == typeData.ChildWithIdProperty ? null : id);
            }

            foreach (var childrenProperty in typeData.ManyChildrenProperties)
            {
                object value = childrenProperty.GetValue(obj);
                if (value == null)
                    continue;

                var items = value as IEnumerable;
                if (items == null)
                    throw new InvalidOperationException($"Couldn't cast value '{value}' of property {childrenProperty.Name} from {typeName} to IEnumerable");
                foreach (var item in items)
                {
                    ProcessTypeItem(
                        item,
                        type,
                        childrenProperty == typeData.ChildWithIdProperty ? null : id);
                }
            }
        }

        private string GetIdFromChildren(object obj, TypeData typeData)
        {
            if (typeData.ChildWithIdProperty == null)
                return null;

            var child = typeData.ChildWithIdProperty.GetValue(obj);
            if (child == null)
                throw new InvalidOperationException($"Property {typeData.ChildWithIdProperty.Name} with Id can't be null in {obj.ToJson()}");

            return typeData.IdPropertyInChild.GetValue(child)?.ToString();
        }
    }
}

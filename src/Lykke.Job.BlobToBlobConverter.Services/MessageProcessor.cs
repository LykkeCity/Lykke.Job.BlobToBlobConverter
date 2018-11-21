using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Log;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Lykke.Job.BlobToBlobConverter.Common.Helpers;
using Lykke.Job.BlobToBlobConverter.Core;
using Lykke.Job.BlobToBlobConverter.Core.Services;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class MessageProcessor : IMessageProcessor
    {
        private const int _maxBatchCount = 100000;

        private readonly ILog _log;
        private readonly ITypeInfo _typeInfo;
        private readonly IProcessingTypeResolver _processingTypeResolver;

        private Type _type;
        private MethodInfo _isValidMethod;
        private Dictionary<string, List<string>> _objectsData;
        private Func<string, List<string>, Task> _messagesHandler;

        public MessageProcessor(
            IProcessingTypeResolver processingTypeResolver,
            ITypeInfo typeInfo,
            ILog log)
        {
            _processingTypeResolver = processingTypeResolver;
            _typeInfo = typeInfo;
            _log = log;
        }

        public void StartBlobProcessing(Func<string, List<string>, Task> messagesHandler)
        {
            _messagesHandler = messagesHandler;
            _objectsData = new Dictionary<string, List<string>>();

            if (_type == null)
            {
                _type = _processingTypeResolver.ResolveProcessingTypeAsync().GetAwaiter().GetResult();
                _isValidMethod = _type.GetMethod("IsValid", new Type[0]);
            }
        }

        public async Task FinishBlobProcessingAsync()
        {
            await SaveObjectsDataAsync();
        }

        public async Task ProcessMessageAsync(object obj)
        {
            try
            {
                if (_isValidMethod != null)
                {
                    bool isValid = (bool)_isValidMethod.Invoke(obj, null);
                    if (!isValid)
                        _log.WriteWarning(nameof(MessageProcessor), nameof(ProcessMessageAsync), $"{_type.FullName} {obj.ToJson()} is invalid!");
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
                _log.WriteError(nameof(ProcessMessageAsync), obj, ex);
                throw;
            }
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

using Lykke.Job.BlobToBlobConverter.Common;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using Lykke.Job.BlobToBlobConverter.Core;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class StructureBuilder : IStructureBuilder, ITypeInfo
    {
        private readonly Type _type;
        private readonly Dictionary<string, List<string>> _excludedPropertiesMap;
        private readonly Dictionary<string, string> _idPropertiesMap;
        private readonly Dictionary<string, string> _relationPropertiesMap;
        private readonly string _instanceTag;

        internal static Type[] GenericCollectionTypes { get; } = {
            typeof(List<>), typeof(IList<>), typeof(IReadOnlyList<>),
            typeof(ICollection<>), typeof(IReadOnlyCollection<>),
        };

        public string IdPropertyName => "Id";

        public Dictionary<Type, TypeData> PropertiesMap { get; }

        public StructureBuilder(
            ITypeRetriever typeRetriever,
            string processingType,
            string nugetPackageName,
            string instanceTag,
            Dictionary<string, List<string>> excludedPropertiesMap,
            Dictionary<string, string> idPropertiesMap,
            Dictionary<string, string> relationPropertiesMap)
        {
            _type = typeRetriever.RetrieveTypeAsync(processingType, nugetPackageName).GetAwaiter().GetResult();
            _excludedPropertiesMap = excludedPropertiesMap ?? new Dictionary<string, List<string>>(0);
            _idPropertiesMap = idPropertiesMap ?? new Dictionary<string, string>(0);
            _relationPropertiesMap = relationPropertiesMap ?? new Dictionary<string, string>();
            _instanceTag = instanceTag;
            PropertiesMap = new Dictionary<Type, TypeData>();
        }

        public string GetIdPropertyName(string typeName)
        {
            return _idPropertiesMap.ContainsKey(typeName) ? _idPropertiesMap[typeName] : null;
        }

        public Dictionary<string, string> GetMappingStructure()
        {
            var result = new Dictionary<string, string>();

            AddStructureLevel(
                _type,
                null,
                null,
                null,
                (t, pt) => new StringBuilder(),
                (sb, n, t) =>
                {
                    if (sb.Length > 0)
                        sb.Append(',');
                    sb.Append(n);
                },
                (sb, n, t) => sb.Insert(0, $"{n},"),
                sb => sb.Length > 0,
                (t, sb) => result.Add(t, sb.ToString()));

            return result;
        }

        public TablesStructure GetTablesStructure()
        {
            var result = new TablesStructure { Tables = new List<TableStructure>() };

            AddStructureLevel(
                _type,
                null,
                null,
                null,
                (t, pt) => new TableStructure
                {
                    TableName = string.IsNullOrWhiteSpace(pt)
                        ? (string.IsNullOrWhiteSpace(_instanceTag) ? t : $"{t}_{_instanceTag}")
                        : (string.IsNullOrWhiteSpace(_instanceTag) ? $"{pt}{t}" : $"{pt}{t}_{_instanceTag}"),
                    AzureBlobFolder = t.ToLower(),
                    Colums = new List<ColumnInfo>(),
                },
                (ts, n, t) => ts.Colums.Add(new ColumnInfo { ColumnName = n, ColumnType = t }),
                (ts, n, t) => ts.Colums.Insert(0, new ColumnInfo { ColumnName = n, ColumnType = t }),
                ts => ts.Colums.Count > 0,
                (t, ts) => result.Tables.Add(ts));

            return result;
        }

        private void AddStructureLevel<TCollector>(
            Type type,
            Type parentType,
            string parentIdPropertyName,
            string parentIdPropertyTypeName,
            Func<string, string, TCollector> initCollector,
            Action<TCollector, string, string> addPropertyInfo,
            Action<TCollector, string, string> insertPropertyInfoToStart,
            Func<TCollector, bool> isCollectorNotEmpty,
            Action<string, TCollector> submitDataFromCollector)
        {
            var processingresult = ProcessProperties(
                type,
                parentType,
                parentIdPropertyName,
                parentIdPropertyTypeName,
                initCollector,
                addPropertyInfo,
                insertPropertyInfoToStart,
                isCollectorNotEmpty,
                submitDataFromCollector);

            if (processingresult.OneChildrenProperties.Count == 0 && processingresult.ManyChildrenProperties.Count == 0)
                return;

            Type typeForChildren = type;
            string idPropertyName = processingresult.IdProperty?.Name;
            string idPropertyTypeName = processingresult.IdProperty?.PropertyType.Name;

            if (parentType != null && processingresult.ValueProperties.Count == 0)
                typeForChildren = parentType;

            if (idPropertyName == null)
            {
                bool idWasFoundInChildren = false;
                Type childWithIdType = null;
                PropertyInfo childWithIdProperty = null;
                PropertyInfo idPropertyInChild = null;
                foreach (var childTypePair in processingresult.OneChildrenProperties)
                {
                    var (childType, childIdProperty) = GetIdPropertyTypeName(childTypePair.Item2);
                    if (childIdProperty == null)
                        continue;

                    if (idWasFoundInChildren)
                    {
                        childWithIdType = null;
                        idPropertyInChild = null;
                        childWithIdProperty = null;
                    }
                    else
                    {
                        childWithIdType = childType;
                        idPropertyInChild = childIdProperty;
                        childWithIdProperty = childTypePair.Item1;
                    }

                    idWasFoundInChildren = true;
                }

                if (idPropertyInChild != null)
                {
                    idPropertyName = idPropertyInChild.Name;
                    idPropertyTypeName = idPropertyInChild.PropertyType.Name;
                    if (idPropertyInChild.Name == IdPropertyName)
                    {
                        typeForChildren = childWithIdType;
                        idPropertyName = $"{childWithIdType.Name}{IdPropertyName}";
                    }
                    var typeData = PropertiesMap[type];
                    typeData.ChildWithIdProperty = childWithIdProperty;
                    typeData.IdPropertyInChild = idPropertyInChild;
                }
            }
            else
            {
                idPropertyName = $"{typeForChildren.Name}{IdPropertyName}";
            }

            if (idPropertyName == null)
            {
                idPropertyName = parentIdPropertyName;
                idPropertyTypeName = parentIdPropertyTypeName;
            }
            if (idPropertyName == null && processingresult.RelationProperty != null)
            {
                idPropertyName = processingresult.RelationProperty.Name;
                idPropertyTypeName = processingresult.RelationProperty.PropertyType.Name;
                PropertiesMap[type].RelationProperty = processingresult.RelationProperty;
            }

            foreach (var childTypePair in processingresult.OneChildrenProperties)
            {
                AddStructureLevel(
                    childTypePair.Item2,
                    typeForChildren,
                    idPropertyName,
                    idPropertyTypeName,
                    initCollector,
                    addPropertyInfo,
                    insertPropertyInfoToStart,
                    isCollectorNotEmpty,
                    submitDataFromCollector);
            }

            foreach (var childType in processingresult.ManyChildrenProperties)
            {
                AddStructureLevel(
                    childType,
                    typeForChildren,
                    idPropertyName,
                    idPropertyTypeName,
                    initCollector,
                    addPropertyInfo,
                    insertPropertyInfoToStart,
                    isCollectorNotEmpty,
                    submitDataFromCollector);
            }
        }

        private PropertiesProcessingResult ProcessProperties<TCollector>(
            Type type,
            Type parentType,
            string parentIdPropertyName,
            string parentIdPropertyTypeName,
            Func<string, string, TCollector> initCollector,
            Action<TCollector, string, string> addPropertyInfo,
            Action<TCollector, string, string> insertPropertyInfoToStart,
            Func<TCollector, bool> isCollectorNotEmpty,
            Action<string, TCollector> submitDataFromCollector)
        {
            string typeName = type.Name;
            _excludedPropertiesMap.TryGetValue(typeName, out var excludedProperties);
            _idPropertiesMap.TryGetValue(typeName, out var idPropertyName);
            _relationPropertiesMap.TryGetValue(typeName, out var relationPropertyName);
            var valueProperties = new List<PropertyInfo>();
            var oneChildrenProperties = new List<(PropertyInfo, Type)>();
            var manyChildrenProperties = new List<(PropertyInfo, Type)>();

            var collector = initCollector(typeName, parentType?.Name);
            var topLevelProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            PropertyInfo idProperty = null;
            PropertyInfo relationProperty = null;
            foreach (var property in topLevelProperties)
            {
                if (excludedProperties != null && excludedProperties.Contains(property.Name))
                    continue;

                var propertyType = property.PropertyType;

                if ((propertyType.IsClass || propertyType.IsInterface) && propertyType != typeof(string))
                {
                    if (propertyType.IsArray)
                    {
                        manyChildrenProperties.Add((property, propertyType.GetElementType()));
                    }
                    else if (propertyType.IsGenericType && GenericCollectionTypes.Any(t => t == propertyType.GetGenericTypeDefinition()))
                    {
                        var genericType = propertyType.GetGenericArguments()[0];
                        if (genericType.IsClass && genericType != typeof(string))
                        {
                            manyChildrenProperties.Add((property, propertyType.GetGenericArguments()[0]));
                        }
                        else
                        {
                            string genericTypeName = genericType.IsEnum ? typeof(string).Name : genericType.Name;
                            int ind = propertyType.Name.IndexOf('`');
                            var propertyTypeName = $"{propertyType.Name.Substring(0, ind + 1)}{genericTypeName}";
                            addPropertyInfo(collector, property.Name, propertyTypeName);
                            valueProperties.Add(property);
                        }
                    }
                    else
                    {
                        oneChildrenProperties.Add((property, propertyType));
                    }
                }
                else
                {
                    if (idProperty == null && (property.Name == IdPropertyName || property.Name == idPropertyName))
                    {
                        idProperty = property;
                    }
                    else
                    {
                        string propertyTypeName = propertyType.Name;
                        if (propertyType.IsGenericType)
                        {
                            var genericType = propertyType.GetGenericArguments()[0];
                            string genericTypeName = genericType.IsEnum ? typeof(string).Name : genericType.Name;
                            int ind = propertyTypeName.IndexOf('`');
                            propertyTypeName = $"{propertyTypeName.Substring(0, ind + 1)}{genericTypeName}";
                        }
                        else if (propertyType.IsEnum)
                        {
                            propertyTypeName = typeof(string).Name;
                        }
                        if (property.Name == relationPropertyName)
                            relationProperty = property;
                        addPropertyInfo(collector, property.Name, propertyTypeName);
                    }
                    valueProperties.Add(property);
                }
            }

            if (isCollectorNotEmpty(collector))
            {
                if (parentIdPropertyName != null && parentType != type)
                {
                    var parentIdPropertyInChild = type.GetProperty(parentIdPropertyName);
                    if (parentIdPropertyInChild == null)
                        insertPropertyInfoToStart(collector, parentIdPropertyName, parentIdPropertyTypeName);
                }
                else if (valueProperties.Count > 0 && parentType != null && parentType != type
                    && PropertiesMap[parentType].ValueProperties.Count > 0)
                    throw new InvalidOperationException(
                        $"Type {typeName} must have any identificators that can be used to make relations to its parent");

                if (idProperty != null)
                {
                    idPropertyName = parentType != null && parentType != type && PropertiesMap[parentType].OneChildrenProperties.Count > 1
                        ? idProperty.Name
                        : IdPropertyName;
                    insertPropertyInfoToStart(collector, idPropertyName, idProperty.PropertyType.Name);
                }
                submitDataFromCollector(typeName, collector);
            }

            if (!PropertiesMap.ContainsKey(type))
                PropertiesMap.Add(
                    type,
                    new TypeData
                    {
                        ValueProperties = valueProperties,
                        OneChildrenProperties = oneChildrenProperties.Select(i => i.Item1).ToList(),
                        ManyChildrenProperties = manyChildrenProperties.Select(i => i.Item1).ToList(),
                        ParentIdPropertyName = parentIdPropertyName,
                    });

            return new PropertiesProcessingResult
            {
                IdProperty = idProperty,
                RelationProperty = relationProperty,
                ValueProperties = valueProperties,
                OneChildrenProperties = oneChildrenProperties,
                ManyChildrenProperties = manyChildrenProperties.Select(i => i.Item2).ToList(),
            };
        }

        private (Type, PropertyInfo) GetIdPropertyTypeName(Type type)
        {
            string typeName = type.Name;
            _idPropertiesMap.TryGetValue(typeName, out var idPropertyName);
            var topLevelProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var property in topLevelProperties)
            {
                if ((property.Name == IdPropertyName || property.Name == idPropertyName))
                    return (type, property);
            }
            return (null, null);
        }
    }
}

using Lykke.Job.BlobToBlobConverter.Common;
using Lykke.Job.BlobToBlobConverter.Common.Abstractions;
using Lykke.Job.BlobToBlobConverter.Core.Services;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    public class StructureBuilder : IStructureBuilder, ITypeInfo
    {
        private readonly Type _type;
        private readonly Dictionary<string, List<string>> _excludedPropertiesMap;
        private readonly Dictionary<string, string> _idPropertiesMap;

        public string IdPropertyName => "Id";

        public Dictionary<Type, (List<PropertyInfo>, List<PropertyInfo>, List<PropertyInfo>)> PropertiesMap { get; private set; }

        public StructureBuilder(
            ITypeRetriever typeRetriever,
            string processingType,
            string nugetPackageName,
            Dictionary<string, List<string>> excludedPropertiesMap,
            Dictionary<string, string> idPropertiesMap)
        {
            _type = typeRetriever.RetrieveTypeAsync(processingType, nugetPackageName).GetAwaiter().GetResult();
            _excludedPropertiesMap = excludedPropertiesMap;
            _idPropertiesMap = idPropertiesMap;
            PropertiesMap = new Dictionary<Type, (List<PropertyInfo>, List<PropertyInfo>, List<PropertyInfo>)>();
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
                false,
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
                false,
                null,
                null,
                (t, pt) => new TableStructure
                {
                    TableName = string.IsNullOrWhiteSpace(pt) ? t : $"{pt}{t}",
                    AzureBlobFolder = t.ToLower(),
                    Colums = new List<ColumnInfo>(),
                },
                (ts, n, t) => ts.Colums.Add(new ColumnInfo { ColumnName = n, ColumnType = t }),
                (ts, n, t) => ts.Colums.Insert(0, new ColumnInfo { ColumnName = n, ColumnType = t }),
                ts => ts.Colums.Count > 0,
                (t, ts) => result.Tables.Add(ts));

            return result;
        }

        private (bool, string) AddStructureLevel<TCollector>(
            Type type,
            bool parentHasId,
            string parentTypeName,
            string parentIdTypeName,
            Func<string, string, TCollector> initCollector,
            Action<TCollector, string, string> addPropertyInfo,
            Action<TCollector, string, string> insertPropertyInfoToStart,
            Func<TCollector, bool> isCollectorNotEmpty,
            Action<string, TCollector> submitDataFromCollector)
        {
            string typeName = type.Name;

            var excludedProperties = _excludedPropertiesMap.ContainsKey(typeName) ? _excludedPropertiesMap[typeName] : new List<string>(0);
            var idPropertyName = _idPropertiesMap.ContainsKey(typeName) ? _idPropertiesMap[typeName] : null;
            bool hasId = false;
            var valueProperties = new List<PropertyInfo>();
            var oneToOneChildrenProperties = new List<(PropertyInfo, Type)>();
            var oneToManyChildrenProperties = new List<(PropertyInfo, Type)>();

            var collector = initCollector(typeName, parentTypeName);
            var topLevelProperties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            string idTypeName = null;
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
                    if (!hasId && (property.Name == IdPropertyName || property.Name == idPropertyName))
                    {
                        hasId = true;
                        idTypeName = property.PropertyType.Name;
                    }
                    else
                    {
                        string propertyTypeName = property.PropertyType.Name;
                        if (property.PropertyType.IsGenericType)
                        {
                            string genericType = property.PropertyType.GetGenericArguments()[0].Name;
                            int ind = propertyTypeName.IndexOf('`');
                            propertyTypeName = $"{propertyTypeName.Substring(0, ind + 1)}{genericType}";
                        }
                        addPropertyInfo(collector, property.Name, propertyTypeName);
                    }
                    valueProperties.Add(property);
                }
            }

            if (PropertiesMap.ContainsKey(type))
                throw new InvalidOperationException($"Type {typeName} is found more than once");

            PropertiesMap.Add(
                type,
                (
                    valueProperties,
                    oneToOneChildrenProperties.Select(i => i.Item1).ToList(),
                    oneToManyChildrenProperties.Select(i => i.Item1).ToList()
                ));

            if (isCollectorNotEmpty(collector))
            {
                if (parentHasId && parentTypeName != null)
                {
                    var parentIdPropertyName = $"{parentTypeName}{IdPropertyName}";
                    var parentIdProperty = type.GetProperty(parentIdPropertyName);
                    if (parentIdProperty == null)
                    {
                        if (parentIdTypeName == null)
                            throw new InvalidOperationException($"Parent id field '{parentIdPropertyName}' type is unknown for type {typeName}");
                        insertPropertyInfoToStart(collector, parentIdPropertyName, parentIdTypeName);
                    }
                }
                if (hasId)
                {
                    if (idTypeName == null)
                        throw new InvalidOperationException($"Id field '{idTypeName}' type is unknown for type {typeName}");
                    insertPropertyInfoToStart(collector, IdPropertyName, idTypeName);
                }
                submitDataFromCollector(typeName, collector);
            }

            if (valueProperties.Count == 0)
            {
                hasId = parentHasId;
                typeName = parentTypeName;
                idTypeName = parentIdTypeName;
            }

            bool childHasId = false;
            string childIdTypeName = null;
            foreach (var childTypePair in oneToOneChildrenProperties)
            {
                (childHasId, childIdTypeName) = AddStructureLevel(
                    childTypePair.Item2,
                    hasId,
                    typeName,
                    idTypeName,
                    initCollector,
                    addPropertyInfo,
                    insertPropertyInfoToStart,
                    isCollectorNotEmpty,
                    submitDataFromCollector);
            }

            if (oneToOneChildrenProperties.Count == 1 && childHasId)
            {
                typeName = oneToOneChildrenProperties[0].Item2.Name;
                idTypeName = childIdTypeName;
                hasId = childHasId;
            }
            foreach (var childTypePair in oneToManyChildrenProperties)
            {
                AddStructureLevel(
                    childTypePair.Item2,
                    hasId,
                    typeName,
                    idTypeName,
                    initCollector,
                    addPropertyInfo,
                    insertPropertyInfoToStart,
                    isCollectorNotEmpty,
                    submitDataFromCollector);
            }

            return (hasId, idTypeName);
        }
    }
}

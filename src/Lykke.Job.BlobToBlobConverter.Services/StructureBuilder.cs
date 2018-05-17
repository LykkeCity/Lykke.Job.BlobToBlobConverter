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
                result);

            return result;
        }

        public TablesStructure GetTablesStructure()
        {
            throw new NotImplementedException();
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
                    if (!hasId && (property.Name == IdPropertyName || property.Name == idPropertyName))
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

            if (PropertiesMap.ContainsKey(type))
                throw new InvalidOperationException($"Type {typeName} is found more than once");

            PropertiesMap.Add(
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
                    var parentIdPropertyName = $"{parentTypeName}{IdPropertyName}";
                    var parentIdProperty = type.GetProperty(parentIdPropertyName);
                    if (parentIdProperty == null)
                        sb.Insert(0, $"{parentIdPropertyName},");
                }
                if (hasId)
                    sb.Insert(0, $"{IdPropertyName},");
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

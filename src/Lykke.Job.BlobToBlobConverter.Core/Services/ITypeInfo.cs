using System;
using System.Collections.Generic;
using System.Reflection;

namespace Lykke.Job.BlobToBlobConverter.Core.Services
{
    public interface ITypeInfo
    {
        string IdPropertyName { get; }

        Dictionary<Type, (List<PropertyInfo>, List<PropertyInfo>, List<PropertyInfo>)> PropertiesMap { get; }

        string GetIdPropertyName(string typeName);
    }
}

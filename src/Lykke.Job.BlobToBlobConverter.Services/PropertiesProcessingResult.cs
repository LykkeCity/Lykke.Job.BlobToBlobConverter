using System;
using System.Collections.Generic;
using System.Reflection;

namespace Lykke.Job.BlobToBlobConverter.Services
{
    internal class PropertiesProcessingResult
    {
        internal PropertyInfo IdProperty { get; set; }
        internal PropertyInfo RelationProperty { get; set; }
        internal List<PropertyInfo> ValueProperties { get; set; }
        internal List<(PropertyInfo, Type)> OneChildrenProperties { get; set; }
        internal List<Type> ManyChildrenProperties { get; set; }
    }
}

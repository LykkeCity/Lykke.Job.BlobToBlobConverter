using System.Collections.Generic;
using System.Reflection;

namespace Lykke.Job.BlobToBlobConverter.Core
{
    public class TypeData
    {
        public List<PropertyInfo> ValueProperties { get; set; }
        public List<PropertyInfo> OneChildrenProperties { get; set; }
        public List<PropertyInfo> ManyChildrenProperties { get; set; }
        public string ParentIdPropertyName { get; set; }
        public PropertyInfo ChildWithIdProperty { get; set; }
        public PropertyInfo IdPropertyInChild { get; set; }
    }
}

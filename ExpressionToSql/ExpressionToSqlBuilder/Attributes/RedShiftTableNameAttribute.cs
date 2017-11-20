using System;

namespace ExpressionToSqlBuilder.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class RedShiftTableNameAttribute : Attribute
    {
        public string Name;

        public RedShiftTableNameAttribute(string name)
        {
            this.Name = name;
        }
    }
}

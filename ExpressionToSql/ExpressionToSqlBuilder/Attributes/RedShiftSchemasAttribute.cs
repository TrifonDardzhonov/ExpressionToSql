using ExpressionToSqlBuilder.Enums;
using System;

namespace ExpressionToSqlBuilder.Attributes
{
    [AttributeUsage(AttributeTargets.Property, AllowMultiple = false)]
    public class RedShiftSchemasAttribute : Attribute
    {
        public RedShiftSchemasEnum[] Schemas { get; set; }

        public RedShiftSchemasAttribute(params RedShiftSchemasEnum[] schemas)
        {
            this.Schemas = schemas;
        }
    }
}

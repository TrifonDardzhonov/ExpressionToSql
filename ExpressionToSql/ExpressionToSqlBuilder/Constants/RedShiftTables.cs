using ExpressionToSqlBuilder.Attributes;
using ExpressionToSqlBuilder.Enums;
using ExpressionToSqlBuilder.Interfaces;
using System;
using System.Linq;
using System.Reflection;

namespace ExpressionToSqlBuilder.Constants
{
    public sealed partial class RedShiftTables : IAmazonRedshiftTableTranslator
    {
        [RedShiftSchemas(RedShiftSchemasEnum.schema1, RedShiftSchemasEnum.schema2)]
        [RedShiftTableName("footable")]
        public string FooTable { get; set; }

        [RedShiftSchemas(RedShiftSchemasEnum.schema3)]
        [RedShiftTableName("gootable")]
        public string GooTable { get; set; }

        public string GetTableNameWithSchema(string table, RedShiftSchemasEnum schema)
        {
            PropertyInfo tablePropertyInfo = this.GetType().GetProperty(table);
            if (tablePropertyInfo == null)
            {
                throw new ArgumentException("Invalid table");
            }

            bool schemaExist = tablePropertyInfo.GetCustomAttribute<RedShiftSchemasAttribute>().Schemas.Contains(schema);

            if (schemaExist)
                return schema.ToString() + '.' + tablePropertyInfo.GetCustomAttribute<RedShiftTableNameAttribute>().Name.ToString();
            else
                throw new ArgumentException("Redshift schema don't exist");
        }
    }
}

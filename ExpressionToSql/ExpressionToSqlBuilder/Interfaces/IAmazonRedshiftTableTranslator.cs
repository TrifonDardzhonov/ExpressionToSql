using ExpressionToSqlBuilder.Enums;

namespace ExpressionToSqlBuilder.Interfaces
{
    public interface IAmazonRedshiftTableTranslator
    {
        string GetTableNameWithSchema(string table, RedShiftSchemasEnum schema);
    }
}

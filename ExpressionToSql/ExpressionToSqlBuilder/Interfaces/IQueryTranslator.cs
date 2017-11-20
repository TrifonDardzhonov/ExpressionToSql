using System.Linq.Expressions;

namespace ExpressionToSqlBuilder.Interfaces
{
    public interface IQueryTranslator
    {
        string Translate(Expression expression, bool useExpressionSelectorAsAlias);
    }
}

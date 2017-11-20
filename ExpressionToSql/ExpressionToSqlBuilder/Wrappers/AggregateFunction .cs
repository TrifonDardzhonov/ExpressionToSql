using ExpressionToSqlBuilder.Interfaces;
using ExpressionToSqlBuilder.Translator;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace ExpressionToSqlBuilder.Wrappers
{
    /// <summary>
    /// This static class have multiple methods for translating lambda expressions to SQL aggregate functions
    /// </summary>
    /// <typeparam name="TEntity">Entity(Database) Model</typeparam>
    public static class AggregateFunction<TEntity> where TEntity : class
    {
        private static Dictionary<int, string> TranslatedExpressions { get; set; }

        static AggregateFunction()
        {
            TranslatedExpressions = new Dictionary<int, string>();
        }

        internal static string GetResultAndReset()
        {
            string translatedResult = TranslatedExpressions[System.Threading.Thread.CurrentThread.ManagedThreadId];

            TranslatedExpressions.Remove(System.Threading.Thread.CurrentThread.ManagedThreadId);

            return translatedResult;
        }

        public static string ConstantValue(object ConstantValue)
        {
            string constantExpressionAsString = ConstantValue.ToString();

            SaveTextResult(" '" + constantExpressionAsString + "' ");
            return constantExpressionAsString;
        }

        #region aggregate functions

        #region COUNT
        public static TResult Count<TResult>(Expression<Func<TEntity, object>> expression)
            where TResult : struct
        {
            return Count<TResult>(expression: expression, overLambdaExpression: null, distinct: false, useExpressionSelectorAsAlias: false);
        }

        public static TResult Count<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            if (expression == null && overLambdaExpression == null)
            {
                return SaveTextResultAndReturnGenericResult<TResult>(" COUNT(*)");
            }
            else
            {
                return AppendFunction<TResult>(function: "COUNT", lambdaExpression: expression, overLambdaExpression: overLambdaExpression, distinct: distinct, caseStatemenResultLambdaExpression: null, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
            }
        }

        public static TResult Count<TResult>(Expression<Func<TEntity, bool>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, Expression<Func<TEntity, object>> caseStatemenResultLambdaExpression, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendFunction<TResult>(function: "COUNT", lambdaExpression: Cast<TEntity, bool, object>(expression), overLambdaExpression: overLambdaExpression, distinct: distinct, caseStatemenResultLambdaExpression: caseStatemenResultLambdaExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
        }

        public static TResult Count<TResult>()
            where TResult : struct
        {
            return Count<TResult>(expression: null, overLambdaExpression: null, distinct: false, useExpressionSelectorAsAlias: false);
        }
        #endregion COUNT

        #region SUM
        public static TResult Sum<TResult>(Expression<Func<TEntity, object>> expression)
             where TResult : struct
        {
            return Sum<TResult>(expression: expression, overLambdaExpression: null, distinct: false, useExpressionSelectorAsAlias: false);
        }

        public static TResult Sum<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendFunction<TResult>(function: "SUM", lambdaExpression: expression, overLambdaExpression: overLambdaExpression, distinct: distinct, caseStatemenResultLambdaExpression: null, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
        }
        #endregion SUM

        #region MIN
        public static TResult Min<TResult>(Expression<Func<TEntity, object>> expression)
            where TResult : struct
        {
            return Min<TResult>(expression: expression, overLambdaExpression: null, distinct: false, useExpressionSelectorAsAlias: false);
        }

        public static TResult Min<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendFunction<TResult>(function: "MIN", lambdaExpression: expression, overLambdaExpression: overLambdaExpression, distinct: distinct, caseStatemenResultLambdaExpression: null, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
        }
        #endregion MIN

        #region MAX
        public static TResult Max<TResult>(Expression<Func<TEntity, object>> expression)
            where TResult : struct
        {
            return Max<TResult>(expression: expression, overLambdaExpression: null, distinct: false, useExpressionSelectorAsAlias: false);
        }

        public static TResult Max<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendFunction<TResult>(function: "MAX", lambdaExpression: expression, overLambdaExpression: overLambdaExpression, distinct: distinct, caseStatemenResultLambdaExpression: null, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
        }
        #endregion MAX

        #region AVG
        public static TResult Avg<TResult>(Expression<Func<TEntity, object>> expression)
            where TResult : struct
        {
            return Avg<TResult>(expression: expression, overLambdaExpression: null, distinct: false, useExpressionSelectorAsAlias: false);
        }

        public static TResult Avg<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendFunction<TResult>(function: "AVG", lambdaExpression: expression, overLambdaExpression: overLambdaExpression, distinct: distinct, caseStatemenResultLambdaExpression: null, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
        }
        #endregion AVG

        #region MEDIAN
        public static TResult Median<TResult>(Expression<Func<TEntity, object>> expression)
            where TResult : struct
        {
            return Median<TResult>(expression: expression, overLambdaExpression: null, distinct: false, useExpressionSelectorAsAlias: false);
        }

        public static TResult Median<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendFunction<TResult>(function: "MEDIAN", lambdaExpression: expression, overLambdaExpression: overLambdaExpression, distinct: distinct, caseStatemenResultLambdaExpression: null, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
        }
        #endregion MEDIAN

        #endregion

        #region Quartiles
        public static TResult FirstQuartile<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendPercentileFunction<TResult>("PERCENTILE_CONT(0.25)", expression, overLambdaExpression, useExpressionSelectorAsAlias);
        }

        public static TResult ThirdQuartile<TResult>(Expression<Func<TEntity, object>> expression, Expression<Func<TEntity, object>> overLambdaExpression, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            return AppendPercentileFunction<TResult>("PERCENTILE_CONT(0.75)", expression, overLambdaExpression, useExpressionSelectorAsAlias);
        }
        #endregion

        #region private methods
        private static TResult AppendFunction<TResult>(string function, Expression<Func<TEntity, object>> lambdaExpression, Expression<Func<TEntity, object>> overLambdaExpression, bool distinct, Expression<Func<TEntity, object>> caseStatemenResultLambdaExpression, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            IQueryTranslator queryTranslator = GetQueryTranslator();
            string translatedExpression = queryTranslator.Translate(lambdaExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
            string result = string.Empty;

            if (string.IsNullOrEmpty(translatedExpression))
            {
                throw new InvalidOperationException("Invalid property");
            }
            else
            {
                string overExpression = queryTranslator.Translate(overLambdaExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
                string caseStatemenResultExpression = (caseStatemenResultLambdaExpression != null) ? queryTranslator.Translate(caseStatemenResultLambdaExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias) : string.Empty;

                if (GetExpressionOperandType(lambdaExpression) != typeof(Boolean))
                    result = string.Format(" {0}({1}{2})", function, ((distinct == true) ? "distinct " : string.Empty), translatedExpression);
                else
                    result = string.Format(" {0}({1}CASE WHEN {2} THEN {3} END)", function, ((distinct == true) ? "distinct " : string.Empty), translatedExpression, (string.IsNullOrEmpty(caseStatemenResultExpression) ? "1" : caseStatemenResultExpression));

                if (!string.IsNullOrEmpty(overExpression))
                    result += string.Format(" OVER(PARTITION BY {0})", overExpression);

                return SaveTextResultAndReturnGenericResult<TResult>(result);
            }
        }

        private static TResult AppendPercentileFunction<TResult>(string function, Expression<Func<TEntity, object>> lambdaExpression, Expression<Func<TEntity, object>> overLambdaExpression, bool useExpressionSelectorAsAlias)
            where TResult : struct
        {
            IQueryTranslator queryTranslator = GetQueryTranslator();
            string translatedExpression = queryTranslator.Translate(lambdaExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
            string overExpression = queryTranslator.Translate(overLambdaExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);
            string result = string.Empty;

            if (string.IsNullOrEmpty(translatedExpression) || string.IsNullOrEmpty(overExpression))
            {
                throw new InvalidOperationException("Invalid property");
            }
            else
            {
                result = string.Format(" {0} WITHIN GROUP(ORDER BY {1}) OVER(PARTITION BY {2})", function, translatedExpression, overExpression);
            }

            return SaveTextResultAndReturnGenericResult<TResult>(result);
        }

        private static Type GetExpressionOperandType(Expression<Func<TEntity, object>> expression)
        {
            UnaryExpression unExp = (expression.Body as UnaryExpression);
            if (unExp == null)
            {
                return expression.Body.Type;
            }
            else
            {
                return unExp.Operand.Type;
            }
        }

        private static IQueryTranslator GetQueryTranslator()
        {
            return new QueryTranslator();
        }

        private static TResult SaveTextResultAndReturnGenericResult<TResult>(string result)
            where TResult : struct
        {
            SaveTextResult(result);
            return (TResult)Activator.CreateInstance(typeof(TResult));
        }

        private static void SaveTextResult(string result)
        {
            TranslatedExpressions.Add(System.Threading.Thread.CurrentThread.ManagedThreadId, result);
        }

        public static Expression<Func<TModel, TToProperty>> Cast<TModel, TFromProperty, TToProperty>(Expression<Func<TModel, TFromProperty>> expression)
        {
            Expression converted = Expression.Convert(expression.Body, typeof(TToProperty));

            return Expression.Lambda<Func<TModel, TToProperty>>(converted, expression.Parameters);
        }
        #endregion
    }
}

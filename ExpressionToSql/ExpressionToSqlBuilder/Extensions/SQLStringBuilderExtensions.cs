using ExpressionToSqlBuilder.Enums;
using ExpressionToSqlBuilder.Interfaces;
using ExpressionToSqlBuilder.Translator;
using ExpressionToSqlBuilder.Wrappers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace ExpressionToSqlBuilder.Extensions
{
    public static class SQLStringBuilderExtensions
    {
        #region Select clauses
        /// <summary>
        /// This method is useful when you want to select particular columns with/without alias.
        /// You can select only entity models with this method.
        /// You should use overload method if you want to use aggregate functions like (sum,min,max etc.).
        /// </summary>
        /// <typeparam name="TEntity">Entity/View (result) model - must be class and should be entity model and result model at the same time</typeparam>
        /// <param name="query"></param>
        /// <param name="selector">expression selector (see some example from useges).</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <param name="distinct">if you want to (SELECT DISTINCT).</param>
        /// <returns>Modified query as string builder.</returns>
        public static StringBuilder AddSelectClause<TEntity>(this StringBuilder query, Expression<Func<TEntity, object>> selector, bool useExpressionSelectorAsAlias = false, bool distinct = false)
            where TEntity : class
        {
            string alias = (useExpressionSelectorAsAlias == true) ? GetParameterExpressionNameAsAlias(selector) : string.Empty;

            List<string> members = GetExpressionMembers(selector, "Invalid select clause")
                .Select(member => alias + (!string.IsNullOrEmpty(alias) ? "." : string.Empty) + member.Name.ToLower())
                .ToList<string>();

            AddClause(query, "SELECT ");

            if (distinct)
                query.Append("distinct ");

            return query.Append(string.Join<string>(",", members));
        }

        /// <summary>
        /// This method is useful when you want to select particular columns.
        /// Also you can select specific model (TResult) from entity model (TEntity).
        /// It's helpful when you want to use aggregate functions. You MUST USE SQLQueryWrapper static class for aggregates functions.
        /// </summary>
        /// <typeparam name="TEntity">Entity Model - must be class and must have public constructor.</typeparam>
        /// <typeparam name="TResult">View (result) model - must be class.</typeparam>
        /// <param name="query"></param>
        /// <param name="selector">expression selector (see some example from useges).</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <param name="distinct">if you want to (SELECT DISTINCT).</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddSelectClause<TEntity, TResult>(this StringBuilder query, Expression<Func<TEntity, TResult>> selector, bool useExpressionSelectorAsAlias = false, bool distinct = false)
            where TEntity : class, new()
            where TResult : class
        {
            List<string> clauses = new List<string>();
            string value = string.Empty;
            string alias = string.Empty;
            MemberExpression memberExpression = null;
            bool isAggregateFunction = false;

            AddClause(query, "SELECT ");

            if (distinct)
                query.Append("distinct ");

            MemberInitExpression body = selector.Body as MemberInitExpression;

            foreach (MemberBinding item in body.Bindings)
            {
                MemberAssignment memberAssignment = item as MemberAssignment;
                Expression exp = memberAssignment.Expression;

                //if we invoke a method
                if (exp.NodeType == ExpressionType.Call)
                {
                    //fix when you want to select bool to string (dapper can't map bool value)
                    if ((exp as MethodCallExpression).Method.Name.Equals("ToString"))
                    {
                        //IGNORE .toString() and get property
                        memberExpression = (exp as MethodCallExpression).Object as MemberExpression;
                    }
                    else
                    {
                        isAggregateFunction = true;
                    }
                }
                else
                {
                    //fix when you want to select nullable value
                    if (memberAssignment.Expression.NodeType == ExpressionType.Convert)
                    {
                        //IGNORE convert part and return property
                        memberExpression = (memberAssignment.Expression as UnaryExpression).Operand as MemberExpression;
                    }
                    else
                    {
                        memberExpression = memberAssignment.Expression as MemberExpression;
                    }
                }

                //If SQLQueryWrapper<TEntity> static method was used
                if (isAggregateFunction)
                {
                    Expression.Lambda(exp).Compile().DynamicInvoke();

                    value = AggregateFunction<TEntity>.GetResultAndReset();

                    isAggregateFunction = false;
                }
                else
                {
                    if (useExpressionSelectorAsAlias == true)
                        alias = (memberExpression.Expression as ParameterExpression).Name;

                    value = alias + (!string.IsNullOrEmpty(alias) ? "." : string.Empty) + memberExpression.Member.Name.ToLower();

                    alias = string.Empty;
                }

                clauses.Add(value + " as " + memberAssignment.Member.Name.ToLower());
            }

            return query.Append(string.Join<string>(",", clauses));
        }
        #endregion

        #region From Clauses
        /// <summary>
        /// With this method you can add from clause from existing table.
        /// </summary>
        /// <typeparam name="TSource">Must be class, must implement IAmazonRedshiftTableTranslator inerface and must have public constructor.</typeparam>
        /// <param name="query"></param>
        /// <param name="schema">Schema from RedShiftSchemasEnum.</param>
        /// <param name="selector">You MUST select one table only.</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddFromClause<TSource>(this StringBuilder query, RedShiftSchemasEnum schema, Expression<Func<TSource, object>> selector, bool useExpressionSelectorAsAlias = false)
            where TSource : class, IAmazonRedshiftTableTranslator, new()
        {
            TSource tableTranslator = new TSource();
            string alias = (useExpressionSelectorAsAlias == true) ? GetParameterExpressionNameAsAlias(selector) : string.Empty;
            IEnumerable<MemberInfo> members = GetExpressionMembers(selector, "Invalid FROM clause");

            if (!members.Any() || members.Count() > 1) { throw new ArgumentException("Invalid FROM clause"); }

            return query.AppendFormat(" FROM {0} {1} ",
                tableTranslator.GetTableNameWithSchema(members.First().Name, schema),
                alias);
        }

        /// <summary>
        /// With this method you can add from clause from inner query.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="innerQuery">Inner query.</param>
        /// <param name="alias">table alias.</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddFromClause(this StringBuilder query, StringBuilder innerQuery, string alias)
        {
            return query.AppendFormat(" FROM ( {0} ) {1} ", innerQuery.ToString(), alias);
        }
        #endregion

        #region Where Clauses
        /// <summary>
        /// This method is useful when you want to START new where clause and conditions.
        /// Can be "Operation(LessThan, GreaterThan, Equal, LessThan or Equal, GreaterThan or Equal, Not Equal) or (Contains,StartsWith,EndsWith, Equals) extension methods." 
        /// </summary>
        /// <typeparam name="TEntity">Entity Model</typeparam>
        /// <param name="query"></param>
        /// <param name="conditionExpression">condition expression selector (see some example from useges). Must return boolean.</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder StartCondition<TEntity>(this StringBuilder query, Expression<Func<TEntity, bool>> conditionExpression, bool useExpressionSelectorAsAlias = false) where TEntity : class
        {
            string condition = GetQueryTranslator().Translate(conditionExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);

            if (AreEvenBrackets(condition, '(', ')') == false) { throw new Exception("Invalid query"); }
            else if (!string.IsNullOrEmpty(condition))
            {
                query.Append(" WHERE " + condition);
            }

            return query;
        }

        /// <summary>
        /// This method is useful when you want to ADD conditions in existing where clause.
        /// Can be "Operation(LessThan, GreaterThan, Equal, LessThan or Equal, GreaterThan or Equal, Not Equal) or (Contains,StartsWith,EndsWith, Equals) extension methods".
        /// </summary>
        /// <typeparam name="TEntity">Entity Model</typeparam>
        /// <param name="query"></param>
        /// <param name="conditionExpression">condition expression selector (see some example from useges). Must return boolean.</param>
        /// <param name="isAndOperation">And or Or operation.</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddCondition<TEntity>(this StringBuilder query, Expression<Func<TEntity, bool>> conditionExpression, bool isAndOperation = true, bool useExpressionSelectorAsAlias = false) where TEntity : class
        {
            string condition = GetQueryTranslator().Translate(conditionExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);

            if (AreEvenBrackets(condition, '(', ')') == false) { throw new Exception("Invalid query"); }
            if (isAndOperation)
                return query.Append(" AND " + condition);
            else
                return query.Append(" OR " + condition);
        }

        /// <summary>
        /// Return translated boolean conditions.
        /// </summary>
        /// <typeparam name="TEntity">Entity Model</typeparam>
        /// <param name="conditionExpression">condition expression selector (see some example from useges). Must return boolean.</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>translated expression as string</returns>
        public static string TranslateCondition<TEntity>(Expression<Func<TEntity, bool>> conditionExpression, bool useExpressionSelectorAsAlias = false) where TEntity : class
        {
            string condition = GetQueryTranslator().Translate(conditionExpression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);

            if (AreEvenBrackets(condition, '(', ')') == false) { throw new Exception("Invalid query"); }
            else { return condition; }
        }

        /// <summary>
        /// Return translated conditions.
        /// </summary>
        /// <typeparam name="TModel">Some model</typeparam>
        /// <param name="expression">expression selector (see some example from useges). Must return object.</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>translated expression as string</returns>
        public static string TranslateExpression<TModel>(Expression<Func<TModel, object>> expression, bool useExpressionSelectorAsAlias = false) where TModel : class
        {
            string condition = GetQueryTranslator().Translate(expression, useExpressionSelectorAsAlias: useExpressionSelectorAsAlias);

            if (AreEvenBrackets(condition, '(', ')') == false) { throw new Exception("Invalid query"); }
            else { return condition; }
        }
        #endregion

        #region Group By Clause
        /// <summary>
        /// This method is useful when you want to START new group by clause.
        /// </summary>
        /// <typeparam name="TEntity">Entity Model</typeparam>
        /// <param name="query"></param>
        /// <param name="groupBySelector">expression selector (see some example from useges).</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder StartGroupByClause<TEntity>(this StringBuilder query, Expression<Func<TEntity, object>> groupBySelector, bool useExpressionSelectorAsAlias = false)
        {
            string alias = (useExpressionSelectorAsAlias == true) ? GetParameterExpressionNameAsAlias(groupBySelector) : string.Empty;

            List<string> members = GetExpressionMembers(groupBySelector, "No specified columns for the group by expression.")
                .Select(member => alias + (!string.IsNullOrEmpty(alias) ? "." : string.Empty) + member.Name.ToLower())
                .ToList<string>();

            return query.AppendFormat(" GROUP BY {0} ", string.Join<string>(",", members));
        }

        /// <summary>
        /// This method is useful when you want to ADD columns in existing group by clause.
        /// </summary>
        /// <typeparam name="TEntity">Entity Model</typeparam>
        /// <param name="query"></param>
        /// <param name="groupBySelector">expression selector (see some example from useges).</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddGroupByClause<TEntity>(this StringBuilder query, Expression<Func<TEntity, object>> groupBySelector, bool useExpressionSelectorAsAlias = false)
        {
            string alias = (useExpressionSelectorAsAlias == true) ? GetParameterExpressionNameAsAlias(groupBySelector) : string.Empty;

            List<string> members = GetExpressionMembers(groupBySelector, "No specified columns for the group by expression.")
                .Select(member => alias + (!string.IsNullOrEmpty(alias) ? "." : string.Empty) + member.Name.ToLower())
                .ToList<string>();

            return query.AppendFormat(" {0} {1} ", ',', string.Join<string>(",", members));
        }
        #endregion

        #region Join Clause
        /// <summary>
        /// This method is useful when you want to ADD JOIN clause with existing table. Automatic gets selector parameter expression name as alias.
        /// </summary>
        /// <typeparam name="TSource">Must be class, must implement IAmazonRedshiftTableTranslator inerface and must have public constructor</typeparam>
        /// <typeparam name="T1">First Table (already joined), not joined table</typeparam>
        /// <typeparam name="T2">Second table, which we want to join</typeparam>
        /// <param name="query"></param>
        /// <param name="schema">Schema from RedShiftSchemasEnum.</param>
        /// <param name="selector">Table selector. You MUST select one table only.</param>
        /// <param name="conditionExpression">ON clause.</param>
        /// <param name="leftJoin">left or inner join (default is inner join).</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddJoinClause<TSource, T1, T2>(this StringBuilder query, RedShiftSchemasEnum schema, Expression<Func<TSource, object>> selector,
            Expression<Func<T1, T2, bool>> conditionExpression, bool leftJoin = false)
            where TSource : class, IAmazonRedshiftTableTranslator, new()
            where T1 : class
            where T2 : class
        {
            TSource tableTranslator = new TSource();
            string alias = GetParameterExpressionNameAsAlias(selector);
            IEnumerable<MemberInfo> members = GetExpressionMembers(selector, "Invalid FROM clause");

            if (!members.Any() || members.Count() > 1) { throw new ArgumentException("Invalid FROM clause"); }

            query.AppendFormat(" {0} join ", leftJoin ? "left" : "inner");
            query.AppendFormat(" {0}  {1} ", tableTranslator.GetTableNameWithSchema(members.First().Name, schema), alias);
            query.AppendFormat("ON {0} ", GetQueryTranslator().Translate(conditionExpression, useExpressionSelectorAsAlias: true));

            return query;
        }

        /// <summary>
        /// This method is useful when you want to ADD JOIN clause with inner query. You must explicit specify the alias.
        /// </summary>
        /// <typeparam name="T1">First Table (already joined), not joined table</typeparam>
        /// <typeparam name="T2">Second table, which we want to join</typeparam>
        /// <param name="query"></param>
        /// <param name="innerQuery">inner query.</param>
        /// <param name="conditionExpression">ON clause.</param>
        /// <param name="alias">the alias for joined query.</param>
        /// <param name="leftJoin">left or inner join (default is inner join).</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddJoinClause<T1, T2>(this StringBuilder query, StringBuilder innerQuery,
            Expression<Func<T1, T2, bool>> conditionExpression, string alias, bool leftJoin = false)
            where T1 : class
            where T2 : class
        {
            query.AppendFormat(" {0} join ", leftJoin ? "left" : "inner");
            query.AppendFormat("( {0} ) {1} ", innerQuery.ToString(), alias);
            query.AppendFormat("ON {0} ", GetQueryTranslator().Translate(conditionExpression, useExpressionSelectorAsAlias: true));

            return query;
        }
        #endregion

        #region Order By Clause
        /// <summary>
        /// This method is useful when you want to ADD Order by clause.
        /// </summary>
        /// <typeparam name="TEntity">Entity Model</typeparam>
        /// <param name="query"></param>
        /// <param name="orderBySelector">expression selector (see some example from useges).</param>
        /// <param name="ascending">asc/desc (default is asc).</param>
        /// <param name="useExpressionSelectorAsAlias">(use/don't use) alias - example: alias.property expression will be translated to alias.property or just property.</param>
        /// <returns>modified query as string builder</returns>
        public static StringBuilder AddOrderByClause<TEntity>(this StringBuilder query, Expression<Func<TEntity, object>> orderBySelector, bool ascending = true, bool useExpressionSelectorAsAlias = false)
        {
            string alias = (useExpressionSelectorAsAlias == true) ? GetParameterExpressionNameAsAlias(orderBySelector) : string.Empty;

            List<string> members = GetExpressionMembers(orderBySelector, "No specified columns for the order by expression.")
                .Select(member => alias + (!string.IsNullOrEmpty(alias) ? "." : string.Empty) + member.Name.ToLower())
                .ToList<string>();

            AddClause(query, " ORDER BY ");

            return query.AppendFormat(" {0} {1} ", string.Join<string>(",", members), (ascending ? " asc" : " desc"));
        }
        #endregion

        #region Batch
        public static StringBuilder Top(this StringBuilder query, int pageSize, int page)
        {
            return query.Append(string.Format(" LIMIT {0} OFFSET {1}", pageSize, ((page == 1) ? 0 : ((page - 1) * pageSize))));
        }
        #endregion

        #region Private methods
        /// <summary>
        /// Get members in lambda expression as IEnumerable(MemberInfo)
        /// </summary>
        /// <typeparam name="Texp"></typeparam>
        /// <param name="expression"></param>
        /// <param name="ErrorMessage"></param>
        /// <returns></returns>
        private static IEnumerable<MemberInfo> GetExpressionMembers<Texp>(Expression<Func<Texp, object>> expression, string ErrorMessage)
        {
            List<MemberInfo> members = new List<MemberInfo>();

            if (expression.Body is NewExpression)
            {
                NewExpression newExpression = (NewExpression)expression.Body;
                members = newExpression.Members.ToList();
            }
            else if (expression.Body is MemberExpression)
            {
                MemberExpression memberExpression = (MemberExpression)expression.Body;
                members.Add(memberExpression.Member);
            }
            else
            {
                const string Format = "Expression '{0}' not supported.";
                string message = string.Format(Format, expression.Body.NodeType);
                throw new ArgumentException(message, "Expression");
            }

            if (!members.Any())
            {
                throw new InvalidOperationException(ErrorMessage);
            }

            return members;
        }

        private static string GetParameterExpressionNameAsAlias<Texp>(Expression<Func<Texp, object>> expression)
        {
            List<string> parameterExpressionNames = new List<string>();

            if (expression.Body is NewExpression)
            {
                parameterExpressionNames = ((NewExpression)expression.Body)
                    .Arguments
                    .ToList<Expression>()
                    .Select(arg => arg as MemberExpression)
                    .Select(arg => (arg.Expression as ParameterExpression).Name)
                    .ToList<string>();
            }
            else if (expression.Body is MemberExpression)
            {
                parameterExpressionNames.Add(((expression.Body as MemberExpression).Expression as ParameterExpression).Name);
            }
            else
            {
                const string Format = "Expression '{0}' not supported.";
                string message = string.Format(Format, expression.Body.NodeType);
                throw new ArgumentException(message, "Expression");
            }

            if (!parameterExpressionNames.Any() && parameterExpressionNames.Any(o => o != parameterExpressionNames.First()))
            {
                throw new InvalidOperationException("Invalid expression");
            }

            return parameterExpressionNames.FirstOrDefault();
        }

        private static bool AreEvenBrackets(string query, char openBrancket, char closeBracket)
        {
            return query.Count(ch => ch == openBrancket || ch == closeBracket) % 2 == 0;
        }

        private static void AddClause(StringBuilder query, string clause)
        {
            if (!query.ToString().Contains(clause)) query.Append(clause);
            else query.Append(",");
        }

        private static IQueryTranslator GetQueryTranslator()
        {
            return new QueryTranslator();
        }
        #endregion
    }
}

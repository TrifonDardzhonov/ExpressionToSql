using ExpressionToSqlBuilder.Interfaces;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace ExpressionToSqlBuilder.Translator
{
    internal class QueryTranslator : ExpressionVisitor, IQueryTranslator
    {
        internal StringBuilder sb { get; set; }
        private string propertyPath { get; set; }
        private bool not { get; set; }
        private List<int> inClauseValues { get; set; }
        private bool useExpressionSelectorAsAlias { get; set; }

        public string Translate(Expression expression, bool useExpressionSelectorAsAlias)
        {
            Reset();
            this.useExpressionSelectorAsAlias = useExpressionSelectorAsAlias;
            this.Visit(expression);
            return this.sb.ToString();
        }

        private void Reset()
        {
            this.sb = new StringBuilder();
            this.propertyPath = string.Empty;
            this.not = false;
            this.inClauseValues = new List<int>();
            this.useExpressionSelectorAsAlias = false;
        }

        private static Expression StripQuotes(Expression e)
        {
            while (e.NodeType == ExpressionType.Quote)
            {
                e = ((UnaryExpression)e).Operand;
            }
            return e;
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            if (node.Method.Name.Equals("Contains", StringComparison.InvariantCultureIgnoreCase))
            {
                if (node.Arguments.Count == 1)
                {
                    //This method don't put input_string into '% %' example: '%input_string%'. 
                    //It just build this expression "AND COLUMN_NAME LIKE input_string"
                    //It's very comfortable for parameterized query where your query MUST look like this "AND COLUMN_NAME LIKE ?"
                    if ((node.Method as MethodInfo).ReflectedType == typeof(String))
                    {
                        this.Visit((node.Object as MemberExpression));
                        if (not) { sb.Append(" NOT"); }
                        sb.Append(" ILIKE ");
                        this.Visit(node.Arguments[0]);
                    }
                    else if ((node.Method as MethodInfo).ReflectedType == typeof(List<int>))
                    {
                        this.Visit((node.Object as MemberExpression));
                        if (inClauseValues != null && inClauseValues.Any())
                        {
                            this.Visit(node.Arguments[0]);
                            if (not) { sb.Append(" NOT"); }
                            sb.Append(" IN (");
                            sb.Append(string.Join<int>(",", inClauseValues));
                            sb.Append(") ");

                            inClauseValues.Clear();
                        }
                    }
                    else
                    {
                        throw new NotImplementedException("Invalid Contains Clause");
                    }
                }
                else if (node.Arguments.Count == 2)
                {
                    this.Visit(node.Arguments[0]);
                    if (inClauseValues != null && inClauseValues.Any())
                    {
                        this.Visit((node.Arguments[1] as MemberExpression));
                        if (not) { sb.Append(" NOT"); }
                        sb.Append(" IN (");
                        sb.Append(string.Join<int>(",", inClauseValues));
                        sb.Append(") ");

                        inClauseValues.Clear();
                    }
                    #region legacy code
                    //System.Int32[] values;
                    //// Extract parameters from original expression
                    //MemberExpression contains = (MemberExpression)node.Arguments[0];
                    //var constantSelector = contains.Expression as ConstantExpression;

                    ////If array is object property
                    //if (constantSelector == null)
                    //{
                    //    MemberExpression containsMemberExpression = (contains.Expression as MemberExpression);
                    //    constantSelector = containsMemberExpression.Expression as ConstantExpression;
                    //    //Get object
                    //    object obj = ((FieldInfo)containsMemberExpression.Member).GetValue(constantSelector.Value);
                    //    //Get array with name "contains.Member.Name" from this object
                    //    values = (System.Int32[])obj.GetType().GetProperty(contains.Member.Name).GetValue(obj);
                    //}
                    ////If array is field
                    //else
                    //{
                    //    values = (System.Int32[])((FieldInfo)contains.Member).GetValue(constantSelector.Value);
                    //}

                    //MemberExpression property = (MemberExpression)node.Arguments[1];

                    //if (values.Any())
                    //{
                    //    sb.Append(property.Member.Name.ToLower() + " IN (" + AmazonRedshiftQueryBuilderHelper.ConcatenateArray<int>(values.ToList<int>()) + ") ");
                    //}
                    #endregion
                }
                else
                {
                    throw new NotImplementedException("Invalid Contains Clause");
                }
            }
            else if (node.Method.Name.Equals("StartsWith", StringComparison.InvariantCultureIgnoreCase))
            {
                this.Visit((node.Object as MemberExpression));
                if (not) { sb.Append(" NOT"); }
                sb.Append(" ILIKE '");
                this.Visit(node.Arguments[0]);
                sb.Append("%'");
            }
            else if (node.Method.Name.Equals("EndsWith", StringComparison.InvariantCultureIgnoreCase))
            {
                this.Visit((node.Object as MemberExpression));
                if (not) { sb.Append(" NOT"); }
                sb.Append(" ILIKE '%");
                this.Visit(node.Arguments[0]);
                sb.Append("'");
            }
            else if (node.Method.Name.Equals("Equals", StringComparison.InvariantCultureIgnoreCase))
            {
                this.Visit((node.Object as MemberExpression));
                if (not) { sb.Append(" != '"); }
                else { sb.Append(" = '"); }
                this.Visit(node.Arguments[0]);
                sb.Append("'");
            }
            else if (node.Method.DeclaringType == typeof(Queryable) && node.Method.Name == "Where")
            {
                sb.Append("SELECT * FROM (");
                this.Visit(node.Arguments[0]);
                sb.Append(") AS T WHERE ");
                LambdaExpression lambda = (LambdaExpression)StripQuotes(node.Arguments[1]);
                this.Visit(lambda.Body);
                return node;
            }
            else if (node.Method.Name.Equals("EqualsNullableInteger", StringComparison.InvariantCultureIgnoreCase))
            {
                //(({0} IS NULL AND {1} IS NULL) OR ({0} = {1}))
                MemberExpression firstMemberExpression;
                MemberExpression secondMemberExpression;

                if (node.Arguments[0].NodeType == ExpressionType.Convert)
                {
                    firstMemberExpression = (node.Arguments[0] as UnaryExpression).Operand as MemberExpression;
                }
                else
                {
                    firstMemberExpression = node.Arguments[0] as MemberExpression;
                }

                if (node.Arguments[1].NodeType == ExpressionType.Convert)
                {
                    secondMemberExpression = (node.Arguments[1] as UnaryExpression).Operand as MemberExpression;
                }
                else
                {
                    secondMemberExpression = node.Arguments[1] as MemberExpression;
                }

                sb.Append("((");
                this.Visit(firstMemberExpression);
                sb.Append(" IS NULL AND ");
                this.Visit(secondMemberExpression);
                sb.Append(" IS NULL ) OR ( ");
                this.Visit(firstMemberExpression);
                sb.Append(" = ");
                this.Visit(secondMemberExpression);
                sb.Append("))");
            }
            else
            {
                throw new NotSupportedException(string.Format("The method '{0}' is not supported", node.Method.Name));
            }

            not = false;
            return node;
        }

        protected override Expression VisitUnary(UnaryExpression u)
        {
            switch (u.NodeType)
            {
                case ExpressionType.Not:
                    not = true;
                    this.Visit(u.Operand);
                    break;
                case ExpressionType.Convert:
                    this.Visit(u.Operand);
                    break;
                default:
                    throw new NotSupportedException(string.Format("The unary operator '{0}' is not supported", u.NodeType));
            }
            return u;
        }

        protected override Expression VisitBinary(BinaryExpression b)
        {
            sb.Append("(");
            this.Visit(b.Left);
            switch (b.NodeType)
            {
                case ExpressionType.And:
                case ExpressionType.AndAlso:
                    sb.Append(" AND ");
                    break;
                case ExpressionType.Or:
                case ExpressionType.OrElse:
                    sb.Append(" OR ");
                    break;
                case ExpressionType.Equal:
                    sb.Append(" = ");
                    break;
                case ExpressionType.NotEqual:
                    sb.Append(" <> ");
                    break;
                case ExpressionType.LessThan:
                    sb.Append(" < ");
                    break;
                case ExpressionType.LessThanOrEqual:
                    sb.Append(" <= ");
                    break;
                case ExpressionType.GreaterThan:
                    sb.Append(" > ");
                    break;
                case ExpressionType.GreaterThanOrEqual:
                    sb.Append(" >= ");
                    break;
                case ExpressionType.Add:
                    sb.Append(" + ");
                    break;
                case ExpressionType.Subtract:
                    sb.Append(" - ");
                    break;
                case ExpressionType.Multiply:
                    sb.Append(" * ");
                    break;
                case ExpressionType.Divide:
                    sb.Append(" / ");
                    break;
                default:
                    throw new NotSupportedException(string.Format("The binary operator '{0}' is not supported", b.NodeType));
            }
            this.Visit(b.Right);

            sb.Append(")")
              .Replace("( )", "")
              .Replace("AND )", ")")
              .Replace("OR )", ")")
              .Replace("( AND", "(")
              .Replace("( OR", "(")
              .Replace("= NULL", "IS NULL")
              .Replace("<> NULL", "IS NOT NULL");

            return b;
        }

        protected override Expression VisitConstant(ConstantExpression c)
        {
            IQueryable q = c.Value as IQueryable;
            if (q != null)
            {
                // assume constant nodes w/ IQueryables are table references
                sb.Append("SELECT * FROM ");
                sb.Append(q.ElementType.Name);
            }
            else if (c.Value == null)
            {
                sb.Append("NULL");
            }
            else
            {
                switch (Type.GetTypeCode(c.Value.GetType()))
                {
                    case TypeCode.Boolean:
                        sb.Append(((bool)c.Value) ? 1 : 0);
                        break;
                    case TypeCode.String:
                        if (sb.ToString().EndsWith("'") || sb.ToString().EndsWith("%") || c.Value.Equals("?"))
                        {
                            sb.Append(c.Value);
                        }
                        else
                        {
                            sb.Append("'");
                            sb.Append(c.Value);
                            sb.Append("'");
                        }
                        break;
                    case TypeCode.Single:
                        sb.Append(((float)c.Value).ToString("F"));
                        break;
                    case TypeCode.Double:
                        sb.Append(((double)c.Value).ToString("F"));
                        break;
                    case TypeCode.Object:
                        throw new NotSupportedException(string.Format("The constant for '{0}' is not supported", c.Value));
                    default:
                        sb.Append(c.Value);
                        break;
                }
            }
            return c;
        }

        protected override Expression VisitMember(MemberExpression m)
        {
            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Constant)
            {
                var inner = (ConstantExpression)m.Expression;
                var value = (m.Member as FieldInfo).GetValue(inner.Value);

                if (!string.IsNullOrEmpty(propertyPath))
                {
                    value = GetPropertyValueWithReflection(value, propertyPath);
                    propertyPath = string.Empty;
                }

                if (value != null)
                {
                    switch (value.GetType().FullName)
                    {
                        case "System.Boolean":
                            sb.Append(((bool)value) ? 1 : 0);
                            break;
                        case "System.DateTime":
                            sb.AppendFormat("'{0}'::TIMESTAMP", ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss"));
                            break;
                        case "System.Int32[]":
                            inClauseValues = ((System.Int32[])value).ToList<int>();
                            break;
                        case "System.Int64[]":
                        case "System.Double[]":
                            throw new NotImplementedException();
                        default:
                            if (IsGenericList(value))
                            {
                                IEnumerable e = value as IEnumerable;
                                inClauseValues = e.OfType<int>().ToList<int>();
                            }
                            else
                            {
                                sb.Append(value.ToString());
                            }
                            break;
                    }
                }
                else
                {
                    sb.Append("NULL");
                }

                return m;
            }

            if (m.Expression != null && m.Expression.NodeType == ExpressionType.Parameter)
            {
                if (!this.useExpressionSelectorAsAlias)
                    sb.Append(m.Member.Name.ToLower());
                else
                    sb.Append((m.Expression as ParameterExpression).Name + "." + m.Member.Name.ToLower());

                propertyPath = string.Empty;
                return m;
            }

            if (m.Expression != null && m.Expression.NodeType == ExpressionType.MemberAccess)
            {
                if (propertyPath.Length > 0)
                {
                    propertyPath = propertyPath.Insert(0, ".");
                }
                propertyPath = propertyPath.Insert(0, m.Member.Name);

                this.Visit(m.Expression as MemberExpression);
                return m;
            }

            throw new NotSupportedException(string.Format("The member '{0}' is not supported", m.Member.Name));
        }

        private object GetPropertyValueWithReflection(object target, string propertyStr)
        {
            foreach (string part in propertyStr.Split('.'))
            {
                PropertyInfo prop = target.GetType().GetProperty(part, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (prop == null)
                {
                    target = target.GetType().GetField(part, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(target);
                }
                else
                {
                    target = target.GetType().GetProperty(part, BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic).GetValue(target, null);
                }
            }

            return target;
        }

        private static bool IsGenericList(object o)
        {
            return (o.GetType().IsGenericType && (o.GetType().GetGenericTypeDefinition() == typeof(List<>)));
        }
    }
}

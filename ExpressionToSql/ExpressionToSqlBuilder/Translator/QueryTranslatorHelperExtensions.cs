namespace ExpressionToSqlBuilder.Translator
{
    public static class QueryTranslatorHelperExtensions
    {
        /// <summary>
        /// Query translator will translate this to ((first IS NULL AND second IS NULL) OR (first = second)) which is correct sql syntax
        /// </summary>
        /// <param name="first"></param>
        /// <param name="second"></param>
        /// <returns></returns>
        public static bool EqualsNullableInteger(this int? first, int? second)
        {
            return first == second;
        }
    }
}

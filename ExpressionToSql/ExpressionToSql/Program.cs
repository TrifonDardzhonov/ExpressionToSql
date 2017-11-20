using ExpressionToSqlBuilder.Constants;
using ExpressionToSqlBuilder.Entities;
using ExpressionToSqlBuilder.Enums;
using ExpressionToSqlBuilder.Extensions;
using ExpressionToSqlBuilder.Wrappers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExpressionToSql
{
    class Program
    {
        static void Main(string[] args)
        {
            StringBuilder sb = new StringBuilder();
            IEnumerable<int> randomIds = new List<int>() { 1, 5, 66, 84 };
            DateTime now = DateTime.UtcNow;

            sb.AddSelectClause<FooTableEntity, Result>(fte => new Result()
            {
                Value = fte.Value,
                Count = AggregateFunction<FooTableEntity>.Count<int>(),
                MaxDate = AggregateFunction<FooTableEntity>.Max<DateTime>(fte_agg => fte_agg.CreatedDate),
            })
            .AddFromClause<RedShiftTables>(RedShiftSchemasEnum.schema1, v2rsft => v2rsft.FooTable)
            .StartCondition<FooTableEntity>(fte =>
                randomIds.Contains(fte.ID) &&
                fte.CreatedDate <= now &&
                fte.Value.StartsWith("YOLO"))
            .StartGroupByClause<FooTableEntity>(fte => new
            {
                fte.Value
            });

            Console.WriteLine(sb);
        }

        public class Result
        {
            public string Value { get; set; }

            public int Count { get; set; }

            public DateTime MaxDate { get; set; }
        }
    }
}

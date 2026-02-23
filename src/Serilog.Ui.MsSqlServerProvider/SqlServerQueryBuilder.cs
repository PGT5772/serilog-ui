using Serilog.Ui.Core.Models;
using Serilog.Ui.Core.QueryBuilder.Sql;
using System;
using System.Text;

namespace Serilog.Ui.MsSqlServerProvider;

/// <summary>
/// Provides methods to build SQL queries specifically for SQL Server to fetch and count logs.
/// </summary>
/// <typeparam name="TModel">The type of the log model.</typeparam>
public class SqlServerQueryBuilder<TModel> : SqlQueryBuilder<TModel> where TModel : LogModel
{
    ///<inheritdoc />
    public override string BuildFetchLogsQuery(SinkColumnNames columns, string schema, string tableName, FetchLogsQuery query)
    {
        StringBuilder queryStr = new();

        GenerateSelectClause(queryStr, columns, schema, tableName);

        GenerateWhereClause(queryStr, columns, query.Level, query.SearchCriteria, query.StartDate, query.EndDate);

        queryStr.Append($"{GenerateSortClause(columns, query.SortOn, query.SortBy)} OFFSET @Offset ROWS FETCH NEXT @Count ROWS ONLY");

        return queryStr.ToString();
    }

    /// <summary>
    /// Builds a SQL query to count logs in the specified table.
    /// </summary>
    /// <param name="columns">The column names used in the sink for logging.</param>
    /// <param name="schema">The schema of the table.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="query">The query parameters for counting logs.</param>
    /// <returns>A SQL query string to count logs.</returns>
    public override string BuildCountLogsQuery(SinkColumnNames columns, string schema, string tableName, FetchLogsQuery query)
    {
        StringBuilder queryStr = new();

        queryStr.Append("SELECT COUNT([Id]) ")
                .Append($"FROM [{schema}].[{tableName}] ");

        GenerateWhereClause(queryStr, columns, query.Level, query.SearchCriteria, query.StartDate, query.EndDate);

        return queryStr.ToString();
    }

    protected override string GenerateSortClause(SinkColumnNames columns, SearchOptions.SortProperty sortOn, SearchOptions.SortDirection sortBy)
        => $"ORDER BY [{GetSortColumnName(columns, sortOn)}] {sortBy.ToString().ToUpper()}";

    /// <summary>
    /// Generates the SELECT clause for the SQL query.
    /// </summary>
    /// <param name="queryBuilder">The StringBuilder to append the SELECT clause to.</param>
    /// <param name="columns">The column names used in the sink for logging.</param>
    /// <param name="schema">The schema of the table.</param>
    /// <param name="tableName">The name of the table.</param>
    private static void GenerateSelectClause(StringBuilder queryBuilder, SinkColumnNames columns, string schema, string tableName)
    {
        if (typeof(TModel) != typeof(SqlServerLogModel))
        {
            queryBuilder.Append("SELECT * ");
        }
        else
        {
            queryBuilder.Append("SELECT [Id], ")
                .Append($"[{columns.Message}], ")
                .Append($"[{columns.Level}], ")
                .Append($"[{columns.Timestamp}], ")
                .Append($"[{columns.Exception}], ")
                .Append($"[{columns.LogEventSerialized}] ");
        }

        queryBuilder.Append($"FROM [{schema}].[{tableName}] ");
    }

    protected override string EscapeColumn(string columnName) => $"[{columnName}]";

}
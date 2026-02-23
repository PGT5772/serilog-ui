using Serilog.Ui.Core.Attributes;
using Serilog.Ui.Core.Models;
using System.Reflection;
using static Serilog.Ui.Core.Models.SearchOptions;

namespace Serilog.Ui.Core.QueryBuilder.Sql;

/// <summary>
/// Abstract class that provides methods to build SQL queries for fetching and counting logs.
/// </summary>
public abstract class SqlQueryBuilder<TModel> where TModel : LogModel
{
    /// <summary>
    /// Builds a SQL query to fetch logs from the specified table.
    /// </summary>
    /// <param name="columns">The column names used in the sink for logging.</param>
    /// <param name="schema">The schema of the table.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="query">The query parameters for fetching logs.</param>
    /// <returns>A SQL query string to fetch logs.</returns>
    public abstract string BuildFetchLogsQuery(SinkColumnNames columns, string schema, string tableName, FetchLogsQuery query);

    /// <summary>
    /// Builds a SQL query to count logs in the specified table.
    /// </summary>
    /// <param name="columns">The column names used in the sink for logging.</param>
    /// <param name="schema">The schema of the table.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="query">The query parameters for counting logs.</param>
    /// <returns>A SQL query string to count logs.</returns>
    public abstract string BuildCountLogsQuery(SinkColumnNames columns, string schema, string tableName, FetchLogsQuery query);

    /// <summary>
    /// Generates a SQL sort clause based on the specified sort property and direction.
    /// </summary>
    /// <param name="columns">The column names used in the sink for logging.</param>
    /// <param name="sortOn">The property to sort on.</param>
    /// <param name="sortBy">The direction to sort by.</param>
    /// <returns>A SQL sort clause string.</returns>
    protected abstract string GenerateSortClause(SinkColumnNames columns, SortProperty sortOn, SortDirection sortBy);

    /// <summary>
    /// Generates a SQL sort clause based on the specified sort property and direction.
    /// </summary>
    /// <param name="columns">The column names used in the sink for logging.</param>
    /// <param name="sortOn">The property to sort on.</param>
    /// <returns>A SQL sort clause string.</returns>
    protected static string GetSortColumnName(SinkColumnNames columns, SortProperty sortOn) => sortOn switch
    {
        SortProperty.Timestamp => columns.Timestamp,
        SortProperty.Level => columns.Level,
        SortProperty.Message => columns.Message,
        _ => columns.Timestamp
    };

    /// <summary>
    /// Determines whether to add the exception column to the WHERE clause based on the presence of the RemovedColumnAttribute.
    /// </summary>
    /// <returns>True if the exception column should be added to the WHERE clause; otherwise, false.</returns>
    protected static bool AddExceptionToWhereClause()
    {
        PropertyInfo? exceptionProperty = typeof(TModel).GetProperty("Exception");
        RemovedColumnAttribute? att = exceptionProperty?.GetCustomAttribute<RemovedColumnAttribute>();

        return att is null;
    }

    /// <summary>
    /// Escapes the column name according to the specific SQL dialect.
    /// Can be overridden by specific providers (e.g., to add [] for SQL Server or "" for Postgres).
    /// </summary>
    /// <param name="columnName">The original column name.</param>
    /// <returns>The escaped column name.</returns>
    protected virtual string EscapeColumn(string columnName) => columnName;

    /// <summary>
    /// Generates the WHERE clause for the SQL query. 
    /// This is common logic extracted to the base class to avoid duplication across providers.
    /// </summary>
    protected void GenerateWhereClause(
        StringBuilder queryBuilder,
        SinkColumnNames columns,
        string? level,
        string? searchCriteria,
        DateTime? startDate,
        DateTime? endDate)
    {
        StringBuilder conditions = new();

        if (!string.IsNullOrWhiteSpace(level))
        {
            conditions.Append($"AND {EscapeColumn(columns.Level)} = @Level ");
        }

        if (!string.IsNullOrWhiteSpace(searchCriteria))
        {
            conditions.Append($"AND ({EscapeColumn(columns.Message)} LIKE @Search ");
            conditions.Append(AddExceptionToWhereClause() ? $"OR {EscapeColumn(columns.Exception)} LIKE @Search) " : ") ");
        }

        if (startDate.HasValue)
        {
            conditions.Append($"AND {EscapeColumn(columns.Timestamp)} >= @StartDate ");
        }

        if (endDate.HasValue)
        {
            conditions.Append($"AND {EscapeColumn(columns.Timestamp)} <= @EndDate ");
        }

        if (conditions.Length > 0)
        {
            // Usamos 1 = 1 porque es estándar en SQL y nos permite encadenar los 'AND' limpiamente
            queryBuilder
                .Append("WHERE 1 = 1 ")
                .Append(conditions);
        }
    }
}
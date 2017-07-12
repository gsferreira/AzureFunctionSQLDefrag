#r "System.Data"

using Polly;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;

public class Database : IDisposable
{
    private const string QUERY_TABLES = @"SELECT QUOTENAME(SCHEMA_NAME(o.schema_id)) + '.' + QUOTENAME(o.name) AS [table_name]
                                        FROM sys.objects AS o
                                        WHERE o.is_ms_shipped = 0 AND o.[type] = 'U';";

    private const string QUERY_FRAGMENTED_INDEXS = @"SELECT name, avg_fragmentation_in_percent
                                                FROM sys.dm_db_index_physical_stats (
                                                        DB_ID(N'{0}')
                                                        , OBJECT_ID('{1}')
                                                        , NULL
                                                           , NULL
                                                        , NULL) AS a
                                                JOIN sys.indexes AS b
                                                ON a.object_id = b.object_id AND a.index_id = b.index_id
                                                WHERE avg_fragmentation_in_percent > {2}";

    private const string QUERY_REBUILD_INDEXS = "ALTER INDEX [{0}] ON {1} REBUILD";
    private const string QUERY_REORGANIZE_INDEXS = "ALTER INDEX [{0}] ON {1} REORGANIZE";

    private readonly string _connectionString;
    private readonly Policy _retryPolicy;
    private readonly SqlConnection _connection;

    public Database( string connectionString)
    {
        _connectionString = connectionString;
        _connection = new SqlConnection(_connectionString);

        _retryPolicy = Policy.Handle<TimeoutException>()
            .Or<SqlException>()
            .WaitAndRetry(5, exponentialBackoff);

    }

    public IList<string> GetTables()
    {
        var tables = new List<string>();

        using(var reader = executeReader(
                QUERY_TABLES))
        {
            while (reader.Read())
            {
                if(!reader.IsDBNull(0))
                    tables.Add(reader.GetString(0));
            }
        }

            
        return tables;
    }
    public IList<string> GetFragmentedIndexes(string table, int fragmentationThreshold)
    {
        var indexs = new List<string>();

        var query = string.Format(
                    QUERY_FRAGMENTED_INDEXS,
                    _connection.Database,
                    table,
                    fragmentationThreshold);

        using(var reader = executeReader(query))
        {
            while (reader.Read())
            {
                if(!reader.IsDBNull(0))
                    indexs.Add(reader.GetString(0));
            }
        }

        return indexs;
    }

    public void RebuildIndex(string indexName, string table)
    {

            executeNonQuery(
                string.Format(QUERY_REBUILD_INDEXS,
                    indexName,
                    table));
        
    }

    public void ReorganizeIndex(string indexName, string table)
    {

            executeNonQuery(
                string.Format(QUERY_REORGANIZE_INDEXS,
                    indexName,
                    table));
        
    }

    private void executeNonQuery( string query, SqlParameter[] parameters = null)
    {
        
        using (var command = new SqlCommand(query))
        {
            command.Connection = _connection;
            command.CommandType = CommandType.Text;

            if (parameters != null)
            {
                foreach (var parameter in parameters)
                    command.Parameters.Add(parameter);

            }



            if (command.Connection.State != ConnectionState.Open)
            {
                _retryPolicy.Execute(command.Connection.Open);
            }

            this._retryPolicy.Execute(() =>
            {
                return command.ExecuteNonQuery();
            });

        }


    }

    private IDataReader executeReader( string query, SqlParameter[] parameters = null)
    {

        IDataReader reader;

        using (var command = new SqlCommand(query))
        {
            command.Connection = _connection;
            command.CommandType = CommandType.Text;

            if (parameters != null)
            {
                foreach (var parameter in parameters)
                    command.Parameters.Add(parameter);

            }



            if (command.Connection.State != ConnectionState.Open)
            {
                _retryPolicy.Execute(command.Connection.Open);

            }

            reader = this._retryPolicy.Execute(() =>
            {
                return command.ExecuteReader();
            });

        }
        return reader;

    }

    private TimeSpan exponentialBackoff(int attempt) => TimeSpan.FromSeconds(Math.Pow(2, attempt));

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }
    protected virtual void Dispose(bool disposing)
    {

        if (disposing)
        {
            this._connection.Close();
            this._connection.Dispose();
        }
    }
}

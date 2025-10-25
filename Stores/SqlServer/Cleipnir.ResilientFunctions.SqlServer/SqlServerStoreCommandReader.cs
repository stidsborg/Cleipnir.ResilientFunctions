using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerStoreCommandReader(SqlConnection connection, SqlDataReader reader) : IStoreCommandReader
{
    public int AffectedRows => reader.RecordsAffected;
    public Task<bool> MoveToNextResults() => reader.NextResultAsync();
    public Task<bool> ReadAsync() => reader.ReadAsync();

    public Task<bool> IsDbNullAsync(int ordinal) => reader.IsDBNullAsync(ordinal);
    public bool IsDbNull(int ordinal) => reader.IsDBNull(ordinal);

    public Guid GetGuid(int ordinal) => reader.GetGuid(ordinal);
    public object GetValue(int ordinal) => reader.GetValue(ordinal);
    public bool GetBoolean(int ordinal) => reader.GetBoolean(ordinal);
    public int GetInt32(int ordinal) => reader.GetInt32(ordinal);
    public long GetInt64(int ordinal) => reader.GetInt64(ordinal);
    public string GetString(int ordinal) => reader.GetString(ordinal);

    public async ValueTask DisposeAsync()
    {
        await reader.DisposeAsync();
        await connection.DisposeAsync();
    }
}

using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerCommandExecutor(string connectionString) : IStoreCommandExecutor
{
    public async Task<IStoreCommandReader> Execute(StoreCommands commands)
    {
        var conn = await CreateConnection();
        var batch = commands.Commands.ToSqlBatch().WithConnection(conn);

        var reader = await batch.ExecuteReaderAsync();
        return new SqlServerStoreCommandReader(conn, reader);
    }

    public async Task<int> ExecuteNonQuery(StoreCommands commands)
    {
        await using var conn = await CreateConnection();
        await using var batch = commands.Commands.ToSqlBatch().WithConnection(conn);

        return await batch.ExecuteNonQueryAsync();
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}

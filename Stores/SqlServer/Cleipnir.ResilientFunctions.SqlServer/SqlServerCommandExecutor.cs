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
        return new SqlServerStoreCommandReader(conn, batch, reader);
    }

    public async Task<IStoreCommandReader> Execute(StoreCommand command)
    {
        var conn = await CreateConnection();
        var cmd = command.ToSqlCommand(conn);

        var reader = await cmd.ExecuteReaderAsync();
        return new SqlServerStoreCommandReader(conn, cmd, reader);
    }

    public async Task<int> ExecuteNonQuery(StoreCommands commands)
    {
        await using var conn = await CreateConnection();
        await using var batch = commands.Commands.ToSqlBatch().WithConnection(conn);

        return await batch.ExecuteNonQueryAsync();
    }

    public async Task<int> ExecuteNonQuery(StoreCommand command)
    {
        await using var conn = await CreateConnection();
        await using var batch = command.ToSqlCommand(conn);

        return await batch.ExecuteNonQueryAsync();
    }

    public async Task<object?> ExecuteScalar(StoreCommand command)
    {
        await using var conn = await CreateConnection();
        await using var cmd = command.ToSqlCommand(conn);

        return await cmd.ExecuteScalarAsync();
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}

using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDB.StoreCommand;

public class MariaDbCommandExecutor(string connectionString) : IStoreCommandExecutor
{
    public async Task<IStoreCommandReader> Execute(StoreCommands commands)
    {
        var conn = await CreateConnection();
        var batch = commands.Commands.ToMySqlBatch().WithConnection(conn);

        var reader = await batch.ExecuteReaderAsync();
        return new MariaDbStoreCommandReader(conn, reader);
    }

    public async Task<int> ExecuteNonQuery(StoreCommands commands)
    {
        await using var conn = await CreateConnection();
        await using var batch = commands.Commands.ToMySqlBatch().WithConnection(conn);

        return await batch.ExecuteNonQueryAsync();
    }

    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}

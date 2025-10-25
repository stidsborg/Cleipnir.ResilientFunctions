using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresCommandExecutor(string connectionString) : IStoreCommandExecutor
{
    public async Task<IStoreCommandReader> Execute(StoreCommands commands)
    {
        var conn = await CreateConnection();
        await using var batch = commands.Commands.ToNpgsqlBatch().WithConnection(conn);

        var reader = await batch.ExecuteReaderAsync();
        return new PostgresStoreCommandReader(conn, reader);
    }

    public async Task<int> ExecuteNonQuery(StoreCommands commands)
    {
        var conn = await CreateConnection();
        await using var batch = commands.Commands.ToNpgsqlBatch().WithConnection(conn);

        return await batch.ExecuteNonQueryAsync();
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
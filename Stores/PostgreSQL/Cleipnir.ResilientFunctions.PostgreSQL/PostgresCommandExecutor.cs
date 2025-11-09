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

    public async Task<IStoreCommandReader> Execute(StoreCommand command)
    {
        var conn = await CreateConnection();
        var cmd = command.ToNpgsqlCommand(conn);

        var reader = await cmd.ExecuteReaderAsync();
        return new PostgresStoreCommandReader(conn, reader);
    }

    public async Task<int> ExecuteNonQuery(StoreCommands commands)
    {
        await using var conn = await CreateConnection();
        await using var batch = commands.Commands.ToNpgsqlBatch().WithConnection(conn);

        return await batch.ExecuteNonQueryAsync();
    }
    
    public async Task<int> ExecuteNonQuery(StoreCommand command)
    {
        await using var conn = await CreateConnection();
        await using var batch = command.ToNpgsqlCommand(conn);

        return await batch.ExecuteNonQueryAsync();
    }

    public async Task<object?> ExecuteScalar(StoreCommand command)
    {
        await using var conn = await CreateConnection();
        await using var cmd = command.ToNpgsqlCommand(conn);
        return await cmd.ExecuteScalarAsync();
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
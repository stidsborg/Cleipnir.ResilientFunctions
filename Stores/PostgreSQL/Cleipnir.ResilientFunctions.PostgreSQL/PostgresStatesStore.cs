using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresStatesStore : IStatesStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public PostgresStatesStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}rfunction_states (
                id VARCHAR(450) PRIMARY KEY,
                state TEXT NOT NULL
            );";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {_tablePrefix}rfunction_states";
        var command = new NpgsqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task UpsertState(FunctionId functionId, StoredState storedState)
    {
        var (functionTypeId, functionInstanceId) = functionId;
        
        await using var conn = await CreateConnection();
        var sql = $@"
          INSERT INTO {_tablePrefix}rfunction_states 
              (id, state)
          VALUES
              ($1, $2) 
          ON CONFLICT (id) 
          DO 
            UPDATE SET state = EXCLUDED.state";
        
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(functionTypeId.Value, functionInstanceId.Value, storedState.StateId.Value)},
                new() {Value = storedState.StateJson}
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task<IEnumerable<StoredState>> GetStates(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT id, state
            FROM {_tablePrefix}rfunction_states
            WHERE id LIKE $1";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters =
            {
                new() { Value = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + "%" }
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var functions = new List<StoredState>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var stateId = Escaper.Unescape(id)[2];
            var state = reader.GetString(1);
            functions.Add(new StoredState(stateId, state));
        }

        return functions;
    }

    public async Task RemoveState(FunctionId functionId, StateId stateId)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {_tablePrefix}rfunction_states WHERE id = $1";
        
        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value, stateId.Value);
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { new() {Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task Remove(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {_tablePrefix}rfunction_states WHERE id LIKE $1";
        
        var idPrefix = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + $"{Escaper.Separator}%";
        await using var command = new NpgsqlCommand(sql, conn)
        {
            Parameters = { new() {Value = idPrefix } }
        };

        await command.ExecuteNonQueryAsync();
    }

    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
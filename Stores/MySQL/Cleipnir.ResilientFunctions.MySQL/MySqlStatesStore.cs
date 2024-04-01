using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MySQL;

public class MySqlStatesStore : IStatesStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;

    public MySqlStatesStore(string connectionString, string tablePrefix = "")
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
        var command = new MySqlCommand(sql, conn);
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
              (?, ?)  
           ON DUPLICATE KEY UPDATE
                state = VALUES(state)";
        
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(functionTypeId.Value, functionInstanceId.Value, storedState.StateId.Value)},
                new() {Value = storedState.StateJson},
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
            WHERE id LIKE ?";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters =
            {
                new() {Value = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + "%"},
            }
        };

        await using var reader = await command.ExecuteReaderAsync();

        var states = new List<StoredState>();
        while (await reader.ReadAsync())
        {
            var id = reader.GetString(0);
            var stateId = Escaper.Unescape(id)[2];
            var json = reader.GetString(1);
            states.Add(new StoredState(stateId, json));
        }

        return states;
    }

    public async Task RemoveState(FunctionId functionId, StateId stateId)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {_tablePrefix}rfunction_states WHERE id = ?";
        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value, stateId.Value);
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters = { new() { Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    public async Task Remove(FunctionId functionId)
    {
        await using var conn = await CreateConnection();
        var sql = $"DELETE FROM {_tablePrefix}rfunction_states WHERE id LIKE ?";
        var id = Escaper.Escape(functionId.TypeId.Value, functionId.InstanceId.Value) + $"{Escaper.Separator}%";
        await using var command = new MySqlCommand(sql, conn)
        {
            Parameters = { new() { Value = id } }
        };

        await command.ExecuteNonQueryAsync();
    }

    private async Task<MySqlConnection> CreateConnection()
    {
        var conn = new MySqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
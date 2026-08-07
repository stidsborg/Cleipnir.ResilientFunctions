using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerTypeStore(string connectionString, string tablePrefix = "") : ITypeStore
{
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        var sql = @$"
            CREATE TABLE {tablePrefix}_Types (
                Id BIGINT PRIMARY KEY,
                Type VARBINARY(MAX) NOT NULL
            );";
        var command = new SqlCommand(sql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        var sql = $"TRUNCATE TABLE {tablePrefix}_Types";
        var command = new SqlCommand(sql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task InsertTypes(IReadOnlyDictionary<TypeId, byte[]> types)
    {
        if (types.Count == 0)
            return;

        await using var conn = await CreateConnection();
        var sql = @$"
            INSERT INTO {tablePrefix}_Types
                (Id, Type)
            SELECT Id, Type
            FROM (VALUES {types.Select((_, i) => $"(@Id{i}, @Type{i})").StringJoin(", ")}) AS V(Id, Type)
            WHERE NOT EXISTS (SELECT 1 FROM {tablePrefix}_Types T WHERE T.Id = V.Id);";

        await using var command = new SqlCommand(sql, conn);
        var i = 0;
        foreach (var (id, type) in types)
        {
            command.Parameters.AddWithValue($"@Id{i}", id.Value);
            command.Parameters.AddWithValue($"@Type{i}", type);
            i++;
        }

        try
        {
            await command.ExecuteNonQueryAsync();
        }
        // A concurrent replica inserted the same id between the existence check and the insert - the mapping's
        // content is identical (ids are content-derived), so the row is already what this insert would have written.
        catch (SqlException exception) when (exception.Number == 2627) {}
    }

    public async Task<IReadOnlyDictionary<TypeId, byte[]>> GetAllTypes()
    {
        await using var conn = await CreateConnection();
        var sql = @$"
            SELECT Id, Type
            FROM {tablePrefix}_Types";

        await using var command = new SqlCommand(sql, conn);
        var dict = new Dictionary<TypeId, byte[]>();

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var id = new TypeId(reader.GetInt64(0));
            var type = (byte[]) reader.GetValue(1);
            dict[id] = type;
        }

        return dict;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}

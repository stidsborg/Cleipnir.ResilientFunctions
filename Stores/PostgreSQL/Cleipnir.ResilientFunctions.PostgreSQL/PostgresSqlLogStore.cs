using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresSqlLogStore(string connectionString, string tablePrefix = "") : ILogStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();
        _initializeSql ??= $@"
            CREATE TABLE IF NOT EXISTS {tablePrefix}_logs (
                type INT NOT NULL,
                instance UUID NOT NULL,
                position INT NOT NULL,   
                owner INT NOT NULL,             
                content BYTEA NOT NULL,            
                PRIMARY KEY (type, instance, position)
            );";

        await using var command = new NpgsqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {tablePrefix}_logs;";
        var command = new NpgsqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private string? _updateLogSql;
    public async Task<Position> Update(StoredId id, Position position, byte[] content, Owner owner)
    {
        await using var conn = await CreateConnection();
        
        _updateLogSql ??= @$"
            UPDATE {tablePrefix}_logs
            SET content = $1
            WHERE type = $2 AND instance = $3 AND position = $4;";
        
        await using var command = new NpgsqlCommand(_updateLogSql, conn)
        {
            Parameters =
            {
                new() {Value = content},
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value},
                new() {Value = position.Value.ToInt()},
            }
        };

        await command.ExecuteNonQueryAsync();

        return position;
    }

    public async Task Delete(StoredId id, Position position)
    {
        await using var conn = await CreateConnection();
        
        _updateLogSql ??= @$"
            DELETE FROM {tablePrefix}_logs            
            WHERE type = $1 AND instance = $2 AND position = $3;";
        
        await using var command = new NpgsqlCommand(_updateLogSql, conn)
        {
            Parameters =
            {
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value},
                new() {Value = position.Value.ToInt()},
            }
        };

        await command.ExecuteNonQueryAsync();
    }

    private string? _appendSql;
    public async Task<Position> Append(StoredId id, byte[] content, Owner owner)
    {
        await using var conn = await CreateConnection();
        _appendSql ??= @$"    
                INSERT INTO {tablePrefix}_logs
                    (type, instance, position, owner, content)
                VALUES (
                     $1, $2, 
                     (SELECT COALESCE(MAX(position), -1) + 1 FROM {tablePrefix}_logs WHERE type = $1 AND instance = $2), 
                     $3, $4
                ) RETURNING position;";
        var command = new NpgsqlCommand(_appendSql, conn)
        {
            Parameters =
            {
                new() { Value = id.Type.Value },
                new() { Value = id.Instance.Value },
                new() { Value = owner.Value },
                new() { Value = content },
            }
        };

        var position = await command.ExecuteScalarAsync();
        return new Position(position!.ToString()!);
    }

    private string? _appendsSql;
    public async Task<IReadOnlyList<Position>> Append(IEnumerable<AppendEntry> entries)
    {
        await using var conn = await CreateConnection();
        
        _appendsSql ??= @$"    
                INSERT INTO {tablePrefix}_logs
                    (type, instance, position, owner, content)
                VALUES @VALUES
                RETURNING position;";
        
        var valuesTemplate = $@"( 
                $TYPE, 
                $INSTANCE, 
                (SELECT COALESCE(MAX(position), -1) + @POSITION_OFFSET FROM {tablePrefix}_logs WHERE type = $TYPE AND instance = $INSTANCE),
                $OWNER,
                $CONTENT
            )";

        var command = new NpgsqlCommand();
        var i = 1;
        var valueSqls = new List<string>();
        var positionOffsets = new Dictionary<StoredId, int>();
        foreach (var (storedId, owner, content) in entries)
        {
            if (!positionOffsets.ContainsKey(storedId))
                positionOffsets[storedId] = 1;

            var positionOffset = positionOffsets[storedId]++;
            
            var typeIndex = i++;
            var instanceIndex = i++;
            var ownerIndex = i++;
            var contentIndex = i++;
            
            var replacedTemplate = valuesTemplate
                .Replace("$TYPE", "$" + typeIndex)
                .Replace("$INSTANCE", "$" + instanceIndex)
                .Replace("$OWNER", "$" + ownerIndex)
                .Replace("$CONTENT", "$" + contentIndex);
            valueSqls.Add(replacedTemplate.Replace("@POSITION_OFFSET", positionOffset.ToString()));

            command.Parameters.AddWithValue(storedId.Type.Value);
            command.Parameters.AddWithValue(storedId.Instance.Value);
            command.Parameters.AddWithValue(owner.Value);
            command.Parameters.AddWithValue(content);
        }
        command.Connection = conn;
        var sql = _appendsSql.Replace("@VALUES", string.Join(", ", valueSqls));
        command.CommandText = sql;

        var positions = new List<Position>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var position = reader.GetInt32(0);
            positions.Add(new Position(position.ToString()));
        }
        
        return positions;
    }

    private string? _getEntries;
    public async Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id)
    {
        await using var conn = await CreateConnection();
        _getEntries ??= @$"    
            SELECT position, owner, content
            FROM {tablePrefix}_logs
            WHERE type = $1 AND instance = $2
            ORDER BY position ASC;";
        
        await using var command = new NpgsqlCommand(_getEntries, conn)
        {
            Parameters =
            {
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value}
            }
        };

        return await ReadEntries(command);
    }

    private string? _getEntriesWithOffset;
    public async Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id, Position offset)
    {
        await using var conn = await CreateConnection();
        _getEntriesWithOffset ??= @$"    
            SELECT position, owner, content
            FROM {tablePrefix}_logs
            WHERE type = $1 AND instance = $2 AND position > $3
            ORDER BY position ASC;";
        
        await using var command = new NpgsqlCommand(_getEntriesWithOffset, conn)
        {
            Parameters =
            {
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value},
                new() {Value = offset.Value.ToInt()},
            }
        };

        return await ReadEntries(command);
    }

    private string? _getEntriesWithOffsetAndOwner;
    public async Task<MaxPositionAndEntries> GetEntries(StoredId id, Position offset, Owner owner)
    {
        await using var conn = await CreateConnection();
        _getEntriesWithOffsetAndOwner ??= @$"    
            SELECT position, owner, CASE WHEN owner = $1 THEN content END AS content
            FROM {tablePrefix}_logs
            WHERE type = $2 AND instance = $3 AND position > $4
            ORDER BY position ASC;";
        
        await using var command = new NpgsqlCommand(_getEntriesWithOffsetAndOwner, conn)
        {
            Parameters =
            {
                new() {Value = owner.Value},
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value},
                new() {Value = offset.Value.ToInt()},
            }
        };

        var entries = await ReadEntries(command);
        if (entries.Count == 0)
            return new MaxPositionAndEntries(offset, Entries: []);
        
        var maxPosition = entries[^1].Position;
        return new MaxPositionAndEntries(
            maxPosition,
            entries.Where(e => e.Content != null!).ToList()
        );
    }

    private async Task<IReadOnlyList<StoredLogEntry>> ReadEntries(NpgsqlCommand command)
    {
        var storedMessages = new List<StoredLogEntry>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var position = new Position(reader.GetInt32(0).ToString());
            var owner = new Owner(reader.GetInt32(1));
            var content = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
            storedMessages.Add(new StoredLogEntry(owner, position, content!));
        }

        return storedMessages;
    }
    
    private async Task<NpgsqlConnection> CreateConnection()
    {
        var conn = new NpgsqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
}
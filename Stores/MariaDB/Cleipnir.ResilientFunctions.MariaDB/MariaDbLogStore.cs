using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

public class MariaDbLogStore : ILogStore
{
    private readonly string _connectionString;
    private readonly string _tablePrefix;
    
    public MariaDbLogStore(string connectionString, string tablePrefix = "")
    {
        _connectionString = connectionString;
        _tablePrefix = tablePrefix;
    }

    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _initializeSql ??= @$"
            CREATE TABLE IF NOT EXISTS {_tablePrefix}_logs (
                type INT,
                instance CHAR(32),
                position INT NOT NULL,
                owner INT NOT NULL,
                content LONGBLOB NOT NULL,                         
                PRIMARY KEY (type, instance, position)
            );";
        var command = new MySqlCommand(_initializeSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateTableSql;
    public async Task Truncate()
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);;
        _truncateTableSql ??= $"TRUNCATE TABLE {_tablePrefix}_logs;";
        var command = new MySqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    private string? _truncateSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _truncateSql ??= @$"    
                DELETE FROM {_tablePrefix}_logs
                WHERE type = ? AND instance = ?";
        
        await using var command = new MySqlCommand(_truncateSql, conn);
        command.Parameters.Add(new() { Value = storedId.Type.Value });
        command.Parameters.Add(new() { Value = storedId.Instance.Value.ToString("N") });
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _updateSql;
    public async Task<Position> Update(StoredId id, Position position, byte[] content, Owner owner)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        
        _updateSql ??= @$"    
                UPDATE {_tablePrefix}_logs
                SET content = ?
                WHERE type = ? AND instance = ? AND position = ?";
        await using var command = new MySqlCommand(_updateSql, conn)
        {
            Parameters =
            {
                new() {Value = content},
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value.ToString("N")},
                new() {Value = int.Parse(position.Value)}
            }
        };
        var affectedRows = await command.ExecuteNonQueryAsync();
        if (affectedRows == 0)
            throw new InvalidOperationException("Unable to find position");
        
        return position;
    }

    private string? _deleteSql;
    public async Task Delete(StoredId id, Position position)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        
        _deleteSql ??= @$"    
                DELETE FROM {_tablePrefix}_logs
                WHERE type = ? AND instance = ? AND position = ?";
        await using var command = new MySqlCommand(_deleteSql, conn)
        {
            Parameters =
            {
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value.ToString("N")},
                new() {Value = int.Parse(position.Value)}
            }
        };

        await command.ExecuteNonQueryAsync();
    }
    
    private string? _appendSql;
    public async Task<Position> Append(StoredId id, byte[] content, Owner owner)
    {
        for (var i = 0; i < 10; i++) //retry if deadlock occurs
            try
            {
                await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
                //https://dev.mysql.com/doc/refman/8.0/en/locking-functions.html#function_get-lock
                var lockName = id.ToString().GenerateSHA256Hash();
                _appendSql ??= @$"    
                    SELECT GET_LOCK(?, 10);
                    INSERT INTO {_tablePrefix}_logs
                        (type, instance, position, owner, content)
                    SELECT ?, ?, COALESCE(MAX(position), -1) + 1, ?, ? 
                        FROM {_tablePrefix}_logs
                        WHERE type = ? AND instance = ?
                    RETURNING position;
                    SELECT RELEASE_LOCK(?);";

                /*
                   SELECT epoch, status
                   FROM {_tablePrefix}
                   WHERE type = ? AND instance = ?;*/
                
                await using var command = new MySqlCommand(_appendSql, conn)
                {
                    Parameters =
                    {
                        new() { Value = lockName },
                        new() { Value = id.Type.Value },
                        new() { Value = id.Instance.Value.ToString("N") },
                        new() { Value = owner.Value },
                        new() { Value = content },
                        
                        new() { Value = id.Type.Value },
                        new() { Value = id.Instance.Value.ToString("N") },
                        new() { Value = lockName },
                        //new() { Value = id.Type.Value },
                        //new() { Value = id.Instance.Value.ToString("N") },
                    }
                };
                
                await using var reader = await command.ExecuteReaderAsync();
                await reader.NextResultAsync(); //lock select
                
                await reader.ReadAsync(); //position return
                var position = reader.GetInt32(0);

                return new Position(position.ToString());
                /*
                await reader.NextResultAsync();
                while (await reader.ReadAsync())
                {
                    var epoch = reader.GetInt32(0);
                    var status = (Status)reader.GetInt32(1);
                    return new FunctionStatus(status, epoch);
                }*/
            }
            catch (MySqlException e) when (e.Number == 1213) //deadlock found when trying to get lock; try restarting transaction
            {
                if (i == 9)
                    throw;

                await Task.Delay(Random.Shared.Next(10, 250));
            }

        return null!;
    }

    private string? _appendsSql;
    public async Task<IReadOnlyList<Position>> Append(IEnumerable<AppendEntry> entries)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _appendsSql ??= @$"                       
            INSERT INTO {_tablePrefix}_logs
            (type, instance, position, owner, content)
            @VALUES
            RETURNING position;";
        
        var valuesTemplate = @$"
           ( 
             SELECT 
                ?, 
                ?, 
                COALESCE(MAX(position), -1) + @POSITION_OFFSET,
                ?,
                ?
              FROM {_tablePrefix}_logs
              WHERE type = ? AND instance = ?
           )";
        
        var command = new MySqlCommand();
        var positionOffsets = new Dictionary<StoredId, int>();
        var valueSqls = new List<string>();
        foreach (var (storedId, owner, content) in entries)
        {
            if (!positionOffsets.ContainsKey(storedId))
                positionOffsets[storedId] = 1;

            var positionOffset = positionOffsets[storedId]++;
            valueSqls.Add(valuesTemplate.Replace("@POSITION_OFFSET", positionOffset.ToString()));

            command.Parameters.Add(new MySqlParameter(name: null, storedId.Type.Value));
            command.Parameters.Add(new MySqlParameter(name: null, storedId.Instance.Value.ToString("N")));
            command.Parameters.Add(new MySqlParameter(name: null, owner.Value));
            command.Parameters.Add(new MySqlParameter(name: null, content));
            command.Parameters.Add(new MySqlParameter(name: null, storedId.Type.Value));
            command.Parameters.Add(new MySqlParameter(name: null, storedId.Instance.Value.ToString("N")));
        }
        command.Connection = conn;
        var sql = _appendsSql.Replace("@VALUES", string.Join(" UNION " + Environment.NewLine, valueSqls));
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

    private string? _getEntriesSql;
    public async Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _getEntriesSql ??= @$"    
            SELECT position, owner, content
            FROM {_tablePrefix}_logs
            WHERE type = ? AND instance = ?
            ORDER BY position ASC;";
        await using var command = new MySqlCommand(_getEntriesSql, conn)
        {
            Parameters =
            {
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value.ToString("N")}
            }
        };

        return await ReadEntries(command);
    }

    private string? _getEntriesWithOffset;
    public async Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id, Position offset)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _getEntriesWithOffset ??= @$"    
            SELECT position, owner, content
            FROM {_tablePrefix}_logs
            WHERE type = ? AND instance = ? AND position > ?
            ORDER BY position ASC;";
        await using var command = new MySqlCommand(_getEntriesWithOffset, conn)
        {
            Parameters =
            {
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value.ToString("N")},
                new() {Value = int.Parse(offset.Value)}
            }
        };

        return await ReadEntries(command);
    }

    private string? _getEntriesWithOffsetAndOwner;
    public async Task<MaxPositionAndEntries> GetEntries(StoredId id, Position offset, Owner owner)
    {
        await using var conn = await DatabaseHelper.CreateOpenConnection(_connectionString);
        _getEntriesWithOffsetAndOwner ??= @$"    
            SELECT position, owner, CASE WHEN owner = ? THEN content END AS content
            FROM {_tablePrefix}_logs
            WHERE type = ? AND instance = ? AND position > ?
            ORDER BY position ASC;";
        await using var command = new MySqlCommand(_getEntriesWithOffsetAndOwner, conn)
        {
            Parameters =
            {
                new() {Value = owner.Value},
                new() {Value = id.Type.Value},
                new() {Value = id.Instance.Value.ToString("N")},
                new() {Value = int.Parse(offset.Value)}
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

    private async Task<List<StoredLogEntry>> ReadEntries(MySqlCommand command)
    {
        var entries = new List<StoredLogEntry>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var position = new Position(reader.GetInt32(0).ToString());
            var owner = new Owner(reader.GetInt32(1));
            var content = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
            entries.Add(new StoredLogEntry(owner, position, content!));
        }

        return entries;
    }
}
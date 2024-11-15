using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerLogStore(string connectionString, string tablePrefix = "") : ILogStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {tablePrefix}_Logs (
            Type INT,
            Instance UNIQUEIDENTIFIER,
            Position INT NOT NULL,
            Owner INT NOT NULL,
            Content VARBINARY(MAX) NOT NULL,          
            PRIMARY KEY (Type, Instance, Position)
        );";
        var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }
    
    private string? _truncateSql;
    public async Task Truncate()
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= $"TRUNCATE TABLE {tablePrefix}_Logs";
        await using var command = new SqlCommand(_truncateSql, conn);
        await command.ExecuteNonQueryAsync();
    }
    
    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }
    
    private string? _updateSql;
    public async Task<Position> Update(StoredId id, Position position, byte[] content, Owner owner)
    {
        await using var conn = await CreateConnection();
        
        _updateSql ??= @$"    
            UPDATE {tablePrefix}_Logs
            SET Content = @Content
            WHERE Type = @Type AND Instance = @Instance AND Position = @Position";
        
        await using var command = new SqlCommand(_updateSql, conn);
        command.Parameters.AddWithValue("@Content", content);
        command.Parameters.AddWithValue("@Type", id.Type.Value);
        command.Parameters.AddWithValue("@Instance", id.Instance.Value);
        command.Parameters.AddWithValue("@Position", int.Parse(position.Value));
        
        await command.ExecuteNonQueryAsync();
        return position;
    }

    private string? _deleteSql;
    public async Task Delete(StoredId id, Position position)
    {
        await using var conn = await CreateConnection();
        
        _deleteSql ??= @$"    
            DELETE FROM {tablePrefix}_Logs           
            WHERE Type = @Type AND Instance = @Instance AND Position = @Position";
        
        await using var command = new SqlCommand(_deleteSql, conn);
        command.Parameters.AddWithValue("@Type", id.Type.Value);
        command.Parameters.AddWithValue("@Instance", id.Instance.Value);
        command.Parameters.AddWithValue("@Position", int.Parse(position.Value));
        
        await command.ExecuteNonQueryAsync();
    }

    private string? _appendSql;
    public async Task<Position> Append(StoredId id, byte[] content, Owner owner)
    {
        await using var conn = await CreateConnection();
        
        _appendSql ??= @$"    
            INSERT INTO {tablePrefix}_Logs
                (Type, Instance, Position, Owner, Content)
            OUTPUT INSERTED.Position
            VALUES ( 
                @Type, 
                @Instance, 
                (SELECT COALESCE(MAX(Position), -1) + 1 FROM {tablePrefix}_Logs WHERE Type = @Type AND Instance = @Instance),
                @Owner,
                @Content
            );";
        
        await using var command = new SqlCommand(_appendSql, conn);
        command.Parameters.AddWithValue("@Type", id.Type.Value);
        command.Parameters.AddWithValue("@Instance", id.Instance.Value);
        command.Parameters.AddWithValue("@Owner", owner.Value);
        command.Parameters.AddWithValue("@Content", content);

        var position = await command.ExecuteScalarAsync();
        return new Position(position!.ToString()!);
    }

    public Task<IReadOnlyList<Position>> Append(StoredId id, IReadOnlyList<Tuple<Owner, Content>> contents)
    {
        throw new NotImplementedException();
    }

    private string? _getEntries;
    public async Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id)
    {
        await using var conn = await CreateConnection();
        _getEntries ??= @$"    
            SELECT Position, Owner, Content
            FROM {tablePrefix}_Logs
            WHERE Type = @Type AND Instance = @Instance
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(_getEntries, conn);
        command.Parameters.AddWithValue("@Type", id.Type.Value);
        command.Parameters.AddWithValue("@Instance", id.Instance.Value);

        return await ReadEntries(command);
    }

    private string? _getEntriesWithOffset;
    public async Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id, Position offset)
    {
        await using var conn = await CreateConnection();
        _getEntriesWithOffset ??= @$"    
            SELECT Position, Owner, Content
            FROM {tablePrefix}_Logs
            WHERE Type = @Type AND Instance = @Instance AND Position > @Position          
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(_getEntriesWithOffset, conn);
        command.Parameters.AddWithValue("@Type", id.Type.Value);
        command.Parameters.AddWithValue("@Instance", id.Instance.Value);
        command.Parameters.AddWithValue("@Position", int.Parse(offset.Value));

        return await ReadEntries(command);
    }

    private string? _getEntriesWithOffsetAndOwner;
    public async Task<MaxPositionAndEntries> GetEntries(StoredId id, Position offset, Owner owner)
    {
        await using var conn = await CreateConnection();
        _getEntriesWithOffsetAndOwner ??= @$"    
            SELECT Position, Owner, CASE WHEN owner = @Owner THEN content END AS Content
            FROM {tablePrefix}_Logs
            WHERE Type = @Type AND Instance = @Instance AND Position > @Position           
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(_getEntriesWithOffsetAndOwner, conn);
        command.Parameters.AddWithValue("@Type", id.Type.Value);
        command.Parameters.AddWithValue("@Instance", id.Instance.Value);
        command.Parameters.AddWithValue("@Position", int.Parse(offset.Value));
        command.Parameters.AddWithValue("@Owner", owner.Value);

        var entries =  await ReadEntries(command);
        if (entries.Count == 0)
            return new MaxPositionAndEntries(offset, Entries: []);
        
        var maxPosition = entries[^1].Position;
        return new MaxPositionAndEntries(
            maxPosition,
            entries.Where(e => e.Content != null!).ToList()
        );
    }

    private async Task<IReadOnlyList<StoredLogEntry>> ReadEntries(SqlCommand command)
    {
        var logEntries = new List<StoredLogEntry>();
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var position = new Position(reader.GetInt32(0).ToString());
            var owner = new Owner(reader.GetInt32(1));
            var content = reader.IsDBNull(2) ? null : (byte[]) reader.GetValue(2);
            logEntries.Add(new StoredLogEntry(owner, position, content!));
        }

        return logEntries;
    }
}
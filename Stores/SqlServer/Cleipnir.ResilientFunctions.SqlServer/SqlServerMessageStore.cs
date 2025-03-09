using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

public class SqlServerMessageStore(string connectionString, SqlGenerator sqlGenerator, string tablePrefix = "") : IMessageStore
{
    private string? _initializeSql;
    public async Task Initialize()
    {
        await using var conn = await CreateConnection();

        _initializeSql ??= @$"
        CREATE TABLE {tablePrefix}_Messages (
            FlowType INT,
            FlowInstance UNIQUEIDENTIFIER,
            Position INT NOT NULL,
            MessageJson VARBINARY(MAX) NOT NULL,
            MessageType VARBINARY(MAX) NOT NULL,   
            IdempotencyKey NVARCHAR(255),          
            PRIMARY KEY (FlowType, FlowInstance, Position)
        );";
        var command = new SqlCommand(_initializeSql, conn);
        try
        {
            await command.ExecuteNonQueryAsync();    
        } catch (SqlException exception) when (exception.Number == 2714) {}
    }

    private string? _truncateTableSql;
    public async Task TruncateTable()
    {
        await using var conn = await CreateConnection();
        _truncateTableSql ??= $"TRUNCATE TABLE {tablePrefix}_Messages;";
        var command = new SqlCommand(_truncateTableSql, conn);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<FunctionStatus?> AppendMessage(StoredId storedId, StoredMessage storedMessage)
        => await AppendMessage(storedId, storedMessage, depth: 0);

    public async Task AppendMessages(IReadOnlyList<StoredIdAndMessage> messages, bool interrupt = true)
    {
        if (messages.Count == 0)
            return;
        
        var storedIds = messages.Select(m => m.StoredId).Distinct().ToList();
        var maxPositions = await GetMaxPositions(storedIds);
        
        var interuptsSql = sqlGenerator.Interrupt(storedIds)!;

        await using var conn = await CreateConnection();
        var sql = @$"    
            INSERT INTO {tablePrefix}_Messages
                (FlowType, FlowInstance, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES 
                 {messages.Select((_, i) => $"(@FlowType{i}, @FlowInstance{i}, @Position{i}, @MessageJson{i}, @MessageType{i}, @IdempotencyKey{i})").StringJoin($",{Environment.NewLine}")};

            {(interrupt ? interuptsSql.Sql : string.Empty)}";

        await using var command = new SqlCommand(sql, conn);
        for (var i = 0; i < messages.Count; i++)
        {
            var (storedId, (messageContent, messageType, idempotencyKey)) = messages[i];
            var (storedType, storedInstance) = storedId;
            var position = ++maxPositions[storedId];
            command.Parameters.AddWithValue($"@FlowType{i}", storedType.Value);
            command.Parameters.AddWithValue($"@FlowInstance{i}", storedInstance.Value);
            command.Parameters.AddWithValue($"@Position{i}", position);
            command.Parameters.AddWithValue($"@MessageJson{i}", messageContent);
            command.Parameters.AddWithValue($"@MessageType{i}", messageType);
            command.Parameters.AddWithValue($"@IdempotencyKey{i}", idempotencyKey ?? (object)DBNull.Value);
        }

        await command.ExecuteNonQueryAsync();
    }

    private string? _appendMessageSql;
    private async Task<FunctionStatus?> AppendMessage(StoredId storedId, StoredMessage storedMessage, int depth)
    {
        await using var conn = await CreateConnection();
        
        _appendMessageSql ??= @$"    
            INSERT INTO {tablePrefix}_Messages
                (FlowType, FlowInstance, Position, MessageJson, MessageType, IdempotencyKey)
            VALUES ( 
                @FlowType, 
                @FlowInstance, 
                (SELECT COALESCE(MAX(position), -1) + 1 FROM {tablePrefix}_Messages WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance), 
                @MessageJson, @MessageType, @IdempotencyKey
            );";
        
        await using var command = new SqlCommand(_appendMessageSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@MessageJson", storedMessage.MessageContent);
        command.Parameters.AddWithValue("@MessageType", storedMessage.MessageType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedMessage.IdempotencyKey ?? (object)DBNull.Value);
        try
        {
            await command.ExecuteNonQueryAsync();
        }
        catch (SqlException e)
        {
            if (depth == 10 || (e.Number != SqlError.DEADLOCK_VICTIM && e.Number != SqlError.UNIQUENESS_VIOLATION)) 
                throw;
            
            // ReSharper disable once DisposeOnUsingVariable
            await conn.DisposeAsync();
            await Task.Delay(Random.Shared.Next(50, 250));
            return await AppendMessage(storedId, storedMessage, depth + 1); 
        }
        
        return await GetSuspensionStatus(storedId, conn);
    }

    private string? _replaceMessageSql;
    public async Task<bool> ReplaceMessage(StoredId storedId, int position, StoredMessage storedMessage)
    {
        await using var conn = await CreateConnection();
        
        _replaceMessageSql ??= @$"    
            UPDATE {tablePrefix}_Messages
            SET MessageJson = @MessageJson, MessageType = @MessageType, IdempotencyKey = @IdempotencyKey
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Position = @Position";
        
        await using var command = new SqlCommand(_replaceMessageSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@Position", position);
        command.Parameters.AddWithValue("@MessageJson", storedMessage.MessageContent);
        command.Parameters.AddWithValue("@MessageType", storedMessage.MessageType);
        command.Parameters.AddWithValue("@IdempotencyKey", storedMessage.IdempotencyKey ?? (object)DBNull.Value);
        
        var affectedRows = await command.ExecuteNonQueryAsync();
        return affectedRows == 1;
    }

    private string? _truncateSql;
    public async Task Truncate(StoredId storedId)
    {
        await using var conn = await CreateConnection();
        _truncateSql ??= @$"    
            DELETE FROM {tablePrefix}_Messages
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";

        await using var command = new SqlCommand(_truncateSql, conn);
        
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        await command.ExecuteNonQueryAsync();
    }

    private string? _getMessagesSql;
    public async Task<IReadOnlyList<StoredMessage>> GetMessages(StoredId storedId, int skip)
    {
        await using var conn = await CreateConnection();
        _getMessagesSql ??= @$"    
            SELECT MessageJson, MessageType, IdempotencyKey
            FROM {tablePrefix}_Messages
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance AND Position >= @Position
            ORDER BY Position ASC;";
        
        await using var command = new SqlCommand(_getMessagesSql, conn);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);
        command.Parameters.AddWithValue("@Position", skip);
        
        var storedMessages = new List<StoredMessage>();
        try
        {
            await using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var messageJson = (byte[]) reader.GetValue(0);
                var messageType = (byte[]) reader.GetValue(1);
                var idempotencyKey = reader.IsDBNull(2) ? null : reader.GetString(2);
                storedMessages.Add(new StoredMessage(messageJson, messageType, idempotencyKey));
            }
        }
        catch (SqlException exception)
        {
            if (exception.Number != SqlError.UNIQUENESS_VIOLATION && exception.Number != SqlError.DEADLOCK_VICTIM) throw;

            conn.Dispose();
            return await GetMessages(storedId, skip);
        }

        return storedMessages;
    }

    public async Task<IDictionary<StoredId, int>> GetMaxPositions(IReadOnlyList<StoredId> storedIds)
    {
        var predicates = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(g => $"FlowType = {g.Key} AND FlowInstance IN ({g.Select(instance => $"'{instance}'").StringJoin(", ")})")
            .StringJoin(" OR " + Environment.NewLine);

        var sql = @$"    
            SELECT FlowType, FlowInstance, Position
            FROM {tablePrefix}_Messages
            WHERE {predicates};";

        await using var conn = await CreateConnection();
        await using var command = new SqlCommand(sql, conn);

        var positions = new Dictionary<StoredId, int>(capacity: storedIds.Count);
        foreach (var storedId in storedIds)
            positions[storedId] = -1;
        
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var type = reader.GetInt32(0).ToStoredType();
            var instance = reader.GetGuid(1).ToStoredInstance();
            var storedId = new StoredId(type, instance);
            var position = reader.GetInt32(2);
            positions[storedId] = position;
        }
        
        return positions;
    }

    private async Task<SqlConnection> CreateConnection()
    {
        var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        return conn;
    }

    private string? _getSuspensionStatusSql;
    private async Task<FunctionStatus?> GetSuspensionStatus(StoredId storedId, SqlConnection connection)
    {
        _getSuspensionStatusSql ??= @$"    
            SELECT Epoch, Status
            FROM {tablePrefix}
            WHERE FlowType = @FlowType AND FlowInstance = @FlowInstance";
        await using var command = new SqlCommand(_getSuspensionStatusSql, connection);
        command.Parameters.AddWithValue("@FlowType", storedId.Type.Value);
        command.Parameters.AddWithValue("@FlowInstance", storedId.Instance.Value);

        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var epoch = reader.GetInt32(0);
            var status = (Status) reader.GetInt32(1);
            return new FunctionStatus(status, epoch);
        }

        return null;
    }
}
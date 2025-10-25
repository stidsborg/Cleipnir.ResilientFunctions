using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

internal static class StoreCommandExtensions
{
    public static NpgsqlCommand ToNpgsqlCommand(this StoreCommand command, NpgsqlConnection conn)
    {
        var cmd = new NpgsqlCommand(command.Sql, conn);
        foreach (var (value, _) in command.Parameters)
            cmd.Parameters.Add(new NpgsqlParameter { Value = value });

        return cmd;
    }
    
    public static NpgsqlBatchCommand ToNpgsqlBatchCommand(this StoreCommand command)
    {
        var cmd = new NpgsqlBatchCommand(command.Sql);
        foreach (var (parameter, _) in command.Parameters)
            cmd.Parameters.Add(new NpgsqlParameter { Value = parameter });

        return cmd;
    }
    
    public static NpgsqlBatch ToNpgsqlBatch(this IEnumerable<StoreCommand> commands) => commands.CreateBatch();
    
}

internal static class StoreCommandsHelper
{
    public static NpgsqlBatch CreateBatch(params StoreCommand[] commands) => CreateBatch((IEnumerable<StoreCommand>) commands);
    public static NpgsqlBatch CreateBatch(this IEnumerable<StoreCommand> commands)
    {
        var batch = new NpgsqlBatch();
        foreach (var command in commands)
            batch.BatchCommands.Add(command.ToNpgsqlBatchCommand());

        return batch;
    }
    
    public static NpgsqlBatch WithConnection(this NpgsqlBatch batch, NpgsqlConnection conn)
    {
        batch.Connection = conn;
        return batch;
    }
}
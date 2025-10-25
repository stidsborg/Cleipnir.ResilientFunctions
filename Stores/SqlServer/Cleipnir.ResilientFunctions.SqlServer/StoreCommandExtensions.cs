using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer;

internal static class StoreCommandExtensions
{
    public static SqlCommand ToSqlCommand(this StoreCommand command, SqlConnection connection)
    {
        var cmd = new SqlCommand();
        cmd.Connection = connection;
        cmd.CommandText = command.Sql;
        foreach (var (value, name) in command.Parameters)
            cmd.Parameters.AddWithValue(name, value);

        return cmd;
    }

    public static SqlBatchCommand ToSqlBatchCommand(this StoreCommand command)
    {
        var cmd = new SqlBatchCommand(command.Sql);
        foreach (var (parameter, name) in command.Parameters)
            cmd.Parameters.AddWithValue(name, parameter);

        return cmd;
    }

    public static SqlBatch ToSqlBatch(this IEnumerable<StoreCommand> commands) => commands.CreateBatch();
    public static SqlBatch ToSqlBatch(this StoreCommands commands) => commands.Commands.CreateBatch();
}

internal static class StoreCommandsHelper
{
    public static SqlBatch CreateBatch(params StoreCommand[] commands) => CreateBatch((IEnumerable<StoreCommand>) commands);
    public static SqlBatch CreateBatch(this IEnumerable<StoreCommand> commands)
    {
        var batch = new SqlBatch();
        foreach (var command in commands)
            batch.BatchCommands.Add(command.ToSqlBatchCommand());

        return batch;
    }

    public static SqlBatch WithConnection(this SqlBatch batch, SqlConnection conn)
    {
        batch.Connection = conn;
        return batch;
    }
}
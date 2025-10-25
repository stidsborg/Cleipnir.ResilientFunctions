using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDB.StoreCommand;

internal static class StoreCommandExtensions
{
    public static MySqlCommand ToSqlCommand(this Storage.StoreCommand command, MySqlConnection connection)
    {
        var cmd = new MySqlCommand();
        cmd.Connection = connection;
        cmd.CommandText = command.Sql;

        foreach (var (value, _) in command.Parameters)
            cmd.Parameters.Add(new MySqlParameter { Value = value });

        return cmd;
    }

    public static MySqlBatchCommand ToMySqlBatchCommand(this Storage.StoreCommand command)
    {
        var cmd = new MySqlBatchCommand(command.Sql);
        foreach (var (parameter, _) in command.Parameters)
            cmd.Parameters.AddWithValue("", parameter);

        return cmd;
    }

    public static MySqlBatch ToMySqlBatch(this IEnumerable<Storage.StoreCommand> commands) => commands.CreateBatch();
    public static MySqlBatch ToMySqlBatch(this StoreCommands commands) => commands.Commands.CreateBatch();
}

internal static class StoreCommandsHelper
{
    public static MySqlBatch CreateBatch(params Storage.StoreCommand[] commands) => CreateBatch((IEnumerable<Storage.StoreCommand>) commands);
    public static MySqlBatch CreateBatch(this IEnumerable<Storage.StoreCommand> commands)
    {
        var batch = new MySqlBatch();
        foreach (var command in commands)
            batch.BatchCommands.Add(command.ToMySqlBatchCommand());

        return batch;
    }

    public static MySqlBatch WithConnection(this MySqlBatch batch, MySqlConnection conn)
    {
        batch.Connection = conn;
        return batch;
    }
}
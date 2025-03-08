using Cleipnir.ResilientFunctions.Storage;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb;

internal static class StoreCommandExtensions
{
    public static MySqlCommand ToSqlCommand(this StoreCommand command, MySqlConnection connection)
    {
        var cmd = new MySqlCommand();
        cmd.Connection = connection;
        cmd.CommandText = command.Sql;

        foreach (var (value, _) in command.Parameters)
            cmd.Parameters.Add(new MySqlParameter {Value = value });

        return cmd;
    }
}
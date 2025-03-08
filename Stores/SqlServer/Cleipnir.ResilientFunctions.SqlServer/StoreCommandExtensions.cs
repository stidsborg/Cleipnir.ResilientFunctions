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
}
using Cleipnir.ResilientFunctions.Storage;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

internal static class StoreCommandExtensions
{
    public static NpgsqlCommand ToNpgsqlCommand(this StoreCommand command, NpgsqlConnection conn)
    {
        var cmd = new NpgsqlCommand(command.Sql, conn);
        foreach (var parameter in command.Parameters)
            cmd.Parameters.Add(new NpgsqlParameter { Value = parameter });

        return cmd;
    }
    
    public static NpgsqlBatchCommand ToNpgsqlBatchCommand(this StoreCommand command)
    {
        var cmd = new NpgsqlBatchCommand(command.Sql);
        foreach (var parameter in command.Parameters)
            cmd.Parameters.Add(new NpgsqlParameter { Value = parameter });

        return cmd;
    }
}
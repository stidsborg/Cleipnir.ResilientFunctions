using System.Collections.Generic;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public class PostgresCommand
{
    public required string Sql { get; init; }
    public required List<object> Parameters { get; init; }

    public NpgsqlCommand ToNpgsqlCommand(NpgsqlConnection conn)
    {
        var cmd = new NpgsqlCommand(Sql, conn);
        foreach (var parameter in Parameters)
            cmd.Parameters.Add(new NpgsqlParameter { Value = parameter });

        return cmd;
    }
    
    public NpgsqlBatchCommand ToNpgsqlBatchCommand()
    {
        var cmd = new NpgsqlBatchCommand(Sql);
        foreach (var parameter in Parameters)
            cmd.Parameters.Add(new NpgsqlParameter { Value = parameter });

        return cmd;
    }
}
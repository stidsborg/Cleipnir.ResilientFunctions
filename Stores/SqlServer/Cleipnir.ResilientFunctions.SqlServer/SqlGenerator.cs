using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.SqlServer;

internal static class SqlGenerator
{
    public static string Interrupt(IEnumerable<StoredId> storedIds, string tableName)
    {
        var conditionals = storedIds
            .GroupBy(id => id.Type.Value, id => id.Instance.Value)
            .Select(group => $"(FlowType = {group.Key} AND FlowInstance IN ({group.Select(i => $"'{i}'").StringJoin(", ")}))")
            .StringJoin(" OR ");

        var sql = @$"
                UPDATE {tableName}
                SET 
                    Interrupted = 1,
                    Status = 
                        CASE 
                            WHEN Status = {(int)Status.Suspended} THEN {(int)Status.Postponed}
                            ELSE Status
                        END,
                    Expires = 
                        CASE
                            WHEN Status = {(int)Status.Postponed} THEN 0
                            WHEN Status = {(int)Status.Suspended} THEN 0
                            ELSE Expires
                        END
                WHERE {conditionals};";

        return sql;
    }
}
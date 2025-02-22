using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.MariaDb;

public static class SqlGenerator
{
    public static string? Interrupt(IEnumerable<StoredId> storedIds, string tablePrefix)
    {
        var conditionals = storedIds
            .Select(storedId => $"(type = {storedId.Type.Value} AND instance = '{storedId.Instance.Value:N}')")
            .StringJoin(" OR ");
        if (string.IsNullOrEmpty(conditionals))
            return null;

        var sql = @$"
                UPDATE {tablePrefix}
                SET 
                    interrupted = TRUE,
                    status = 
                        CASE 
                            WHEN status = {(int)Status.Suspended} THEN {(int)Status.Postponed}
                            ELSE status
                        END,
                    expires = 
                        CASE
                            WHEN status = {(int)Status.Postponed} THEN 0
                            WHEN status = {(int)Status.Suspended} THEN 0
                            ELSE expires
                        END
                WHERE {conditionals}";

        return sql;
    }
}
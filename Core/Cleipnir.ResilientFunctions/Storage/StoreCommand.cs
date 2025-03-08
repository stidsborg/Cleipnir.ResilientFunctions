using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Storage;

public class StoreCommand
{
    public string Sql { get; }
    public List<ParameterValueAndName> Parameters { get; }

    public StoreCommand(string sql, IEnumerable<object> values)
    {
        Sql = sql;
        Parameters = values.Select(v => new ParameterValueAndName(v)).ToList();
    }
    
    public void AddParameter(string name, object value)
        => Parameters.Add(new ParameterValueAndName(value, name));
}

public record ParameterValueAndName(object Value, string? Name = null);
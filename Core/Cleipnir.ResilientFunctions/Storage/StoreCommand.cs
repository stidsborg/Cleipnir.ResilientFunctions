using System;
using System.Collections.Generic;
using System.Linq;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class StoreCommand
{
    public string Sql { get; }
    public List<ParameterValueAndName> Parameters { get; }

    public StoreCommand(string sql)
    {
        Sql = sql;
        Parameters = new List<ParameterValueAndName>();
    }
    
    public StoreCommand(string sql, IEnumerable<object> values)
    {
        Sql = sql;
        Parameters = values.Select(v => new ParameterValueAndName(v)).ToList();
    }
    
    public StoreCommand(string sql, List<ParameterValueAndName> parameters)
    {
        Sql = sql;
        Parameters = parameters;
    }
    
    public void AddParameter(string name, object value) => Parameters.Add(new ParameterValueAndName(name, value));
    public void AddParameter(object value) => Parameters.Add(new ParameterValueAndName(value));

    public static StoreCommand Merge(params StoreCommand[] commands)
    {
        return new StoreCommand(
            sql: commands.Select(cmd => cmd.Sql).StringJoin(Environment.NewLine),
            parameters: commands.SelectMany(cmd => cmd.Parameters).ToList()
        );
    }
}

public class ParameterValueAndName
{
    public object Value { get; }
    public string? Name { get; }

    public ParameterValueAndName(object value)
    {
        Value = value;
        Name = null;
    }
    
    public ParameterValueAndName(string name, object value)
    {
        Name = name;
        Value = value;
    }

    public void Deconstruct(out object value, out string? name)
    {
        value = Value;
        name = Name;
    }
}
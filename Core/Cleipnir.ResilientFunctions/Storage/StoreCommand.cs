using System;
using System.Collections.Generic;
using System.Linq;

namespace Cleipnir.ResilientFunctions.Storage;

public record StoreCommand(string Sql, List<ParameterValueAndName> Parameters)
{
    public void AddParameter(string name, object value) => Parameters.Add(new ParameterValueAndName(name, value));
    public void AddParameter(object value) => Parameters.Add(new ParameterValueAndName(value));

    public StoreCommand Merge(StoreCommand? otherCommand) =>
        otherCommand == null
            ? this
            : new(
                Sql: Sql + Environment.NewLine + otherCommand.Sql,
                Parameters.Concat(otherCommand.Parameters).ToList()
            );
    
    public static StoreCommand Merge(StoreCommand firstCommand, params StoreCommand?[] commands)
    {
        foreach (var command in commands)
        {
            if (command == null) continue;

            firstCommand = firstCommand with
            {
                Sql = firstCommand.Sql + Environment.NewLine + command.Sql
            };
            
            foreach (var parameter in command.Parameters)
                firstCommand.Parameters.Add(parameter);
        }

        return firstCommand;
    }

    public static StoreCommand? Merge(params StoreCommand?[] commands)
    {
        var firstCommand = default(StoreCommand);
        foreach (var command in commands) 
            firstCommand = firstCommand == null 
                ? command 
                : firstCommand.Merge(command);

        return firstCommand;
    }
    
    public StoreCommand AppendSql(string sql) => new(Sql + Environment.NewLine + sql, Parameters);
    public static StoreCommand Create(string sql) => new StoreCommand(sql, new List<ParameterValueAndName>());
    public static StoreCommand Create(string sql, IEnumerable<object> values) => new(sql, values.Select(v => new ParameterValueAndName(v)).ToList());
    public static StoreCommand Create(string sql, List<ParameterValueAndName> parameters) => new(sql, parameters);
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

    public override string ToString() => $"{Name}: {Value}";
}
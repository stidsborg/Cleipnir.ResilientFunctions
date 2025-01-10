using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Utils;

public class UnderlyingInMemoryRegister : IUnderlyingRegister
{
    private readonly Dictionary<Id, string> _dictionary = new();
    private readonly Lock _sync = new();

    public Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        var id = new Id(registerType, group, name);
        lock (_sync)
            if (_dictionary.ContainsKey(id))
                return false.ToTask();
            else
                _dictionary[id] = value;

        return true.ToTask();
    }
    
    public Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        var id = new Id(registerType, group, name);
        lock (_sync)
            if (!setIfEmpty && !_dictionary.ContainsKey(id))
                return false.ToTask();
            else if (!_dictionary.ContainsKey(id) || _dictionary[id].Equals(expectedValue))
                _dictionary[id] = newValue;
            else
                return false.ToTask();

        return true.ToTask();
    }

    public Task<string?> Get(RegisterType registerType, string group, string name)
    {
        var id = new Id(registerType, group, name);
        lock (_sync)
            if (_dictionary.ContainsKey(id))
                return ((string?) _dictionary[id]).ToTask();

        return default(string).ToTask();
    }

    public Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        var id = new Id(registerType, group, name);
        lock (_sync)
            if (!_dictionary.ContainsKey(id) || _dictionary[id].Equals(expectedValue))
                _dictionary.Remove(id);
            else
                return false.ToTask();

        return true.ToTask();
    }

    public Task Delete(RegisterType registerType, string group, string name)
    {
        var id = new Id(registerType, group, name);
        lock (_sync)
            _dictionary.Remove(id);

        return Task.CompletedTask;
    }
    
    public Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        var id = new Id(registerType, group, name);
        lock (_sync)
            return _dictionary.ContainsKey(id).ToTask();
    }

    private record Id(RegisterType RegisterType, string Group, string Name);
}
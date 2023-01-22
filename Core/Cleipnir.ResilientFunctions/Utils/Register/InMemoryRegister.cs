using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Utils.Register;

public class InMemoryRegister : IRegister
{
    private readonly Dictionary<Id, string> _dictionary = new();
    private readonly object _sync = new();

    public Task<bool> SetIfEmpty(string group, string key, string value)
    {
        var id = new Id(group, key);
        lock (_sync)
            if (_dictionary.ContainsKey(id))
                return false.ToTask();
            else
                _dictionary[id] = value;

        return true.ToTask();
    }

    public Task<bool> CompareAndSwap(string group, string key, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        var id = new Id(group, key);
        lock (_sync)
            if (!setIfEmpty && !_dictionary.ContainsKey(id))
                return false.ToTask();
            else if (!_dictionary.ContainsKey(id) || _dictionary[id].Equals(expectedValue))
                _dictionary[id] = newValue;
            else
                return false.ToTask();

        return true.ToTask();
    }

    public Task<string?> Get(string group, string key)
    {
        var id = new Id(group, key);
        lock (_sync)
            if (_dictionary.ContainsKey(id))
                return ((string?) _dictionary[id]).ToTask();

        return default(string).ToTask();
    }

    public Task<bool> Delete(string group, string key, string expectedValue)
    {
        var id = new Id(group, key);
        lock (_sync)
            if (!_dictionary.ContainsKey(id) || _dictionary[id].Equals(expectedValue))
                _dictionary.Remove(id);
            else
                return false.ToTask();

        return true.ToTask();
    }

    public Task Delete(string group, string key)
    {
        var id = new Id(group, key);
        lock (_sync)
            _dictionary.Remove(id);

        return Task.CompletedTask;
    }

    public Task<bool> Exists(string group, string key)
    {
        var id = new Id(group, key);
        lock (_sync)
            return _dictionary.ContainsKey(id).ToTask();
    }

    private record Id(string Group, string Key);
}
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Utils.Arbitrator;

public class InMemoryArbitrator : IArbitrator
{
    private readonly Dictionary<string, string> _groupOnlyDecisions = new();
    private readonly Dictionary<Tuple<string, string>, string> _decisions = new();
    private readonly object _sync = new();
    
    public Task<bool> Propose(string group, string key, string value)
    {
        var groupAndKey = Tuple.Create(group, key);
        lock (_sync)
            if (!_decisions.ContainsKey(groupAndKey))
            {
                _decisions[groupAndKey] = value;
                return true.ToTask();
            }
            else
                return (_decisions[groupAndKey] == value).ToTask();
    }

    public Task<bool> Propose(string group, string value)
    {
        lock (_sync)
            if (!_groupOnlyDecisions.ContainsKey(group))
            {
                _groupOnlyDecisions[group] = value;
                return true.ToTask();
            }
            else
                return (_groupOnlyDecisions[group] == value).ToTask();
    }

    public Task Delete(string groupId)
    {
        lock (_sync)
            _groupOnlyDecisions.Remove(groupId);

        return Task.CompletedTask;
    }

    public Task Delete(string groupId, string instanceId)
    {
        lock (_sync)
            _decisions.Remove(Tuple.Create(groupId, instanceId));

        return Task.CompletedTask;
    }
}
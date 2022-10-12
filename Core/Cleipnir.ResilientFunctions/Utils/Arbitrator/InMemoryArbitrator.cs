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
    
    public Task<bool> Propose(string groupId, string instanceId, string value)
    {
        var key = Tuple.Create(groupId, instanceId);
        lock (_sync)
            if (!_decisions.ContainsKey(key))
            {
                _decisions[key] = value;
                return true.ToTask();
            }
            else
                return (_decisions[key] == value).ToTask();
    }

    public Task<bool> Propose(string groupId, string value)
    {
        lock (_sync)
            if (!_groupOnlyDecisions.ContainsKey(groupId))
            {
                _groupOnlyDecisions[groupId] = value;
                return true.ToTask();
            }
            else
                return (_groupOnlyDecisions[groupId] == value).ToTask();
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
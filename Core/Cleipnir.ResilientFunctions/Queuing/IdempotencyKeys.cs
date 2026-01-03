using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Queuing;

internal class IdempotencyKeys(EffectId parentId, Effect effect, int maxCount, TimeSpan? keyTtl, UtcNow utcNow)
{
    private int _nextId;
    private readonly Dictionary<int, Entry> _dictionary = new();
    private readonly Lock _lock = new();
    
    public void Initialize()
    {
        var children = effect.GetChildren(parentId);
       
        foreach (var childId in children)
        {
            var value = effect.Get<Tuple<long, string, long?>>(childId);
            _dictionary[childId.Id] = Entry.FromTuple(value);
            _nextId = Math.Max(_nextId, childId.Id + 1);
        }
        
        CleanUp();
    }

    public EffectResult? Add(string idempotencyKey, long position)
    {
        CleanUp();

        var entry = new Entry(
            position,
            idempotencyKey,
            keyTtl == null ? null : utcNow().Ticks + keyTtl.Value.Ticks
        );
        int id;
        lock (_lock)
        {
            if (_dictionary.Any(e => e.Value.IdempotencyKey == idempotencyKey))
                return null;
            
            id = _nextId++;
            _dictionary[id] = entry;
        }

        return new EffectResult(parentId.CreateChild(id), entry.ToTuple(), Alias: null);
    }

    private void Remove(int id)
    {
        lock (_lock)
            _dictionary.Remove(id);
        
        effect.ClearNoFlush(parentId.CreateChild(id));
    }
    
    private void CleanUp()
    {
        var now = utcNow().Ticks;
        List<int> toRemove;
        lock (_lock)
            toRemove = _dictionary
                .Where(kv => kv.Value.Ttl.HasValue && kv.Value.Ttl < now)
                .Select(kv => kv.Key)
                .ToList();
        
        foreach (var id in toRemove)
            Remove(id);

        while (_dictionary.Count > maxCount) 
            Remove(_dictionary.Keys.Min());
    }

    public bool Contains(string idempotencyKey)
    {
        lock (_lock)
            return _dictionary.Values.Any(s => s.IdempotencyKey == idempotencyKey);
    }

    private record Entry(long Position, string IdempotencyKey, long? Ttl)
    {
        public Tuple<long, string, long?> ToTuple() => new(Position, IdempotencyKey, Ttl);

        public static Entry FromTuple(Tuple<long, string, long?> tuple) => new Entry(tuple.Item1, tuple.Item2, tuple.Item3);
    }
}
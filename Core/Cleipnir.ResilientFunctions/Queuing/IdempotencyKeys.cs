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
    
    public async Task Initialize()
    {
        var children = effect.GetChildren(parentId);
        var now = utcNow().Ticks;
        
        foreach (var childId in children)
        {
            var value = effect.Get<Tuple<long, string, long?>>(childId);
            if (value.Item1 < now)
                await Remove(childId.Id);
            else
                _dictionary[childId.Id] = Entry.FromTuple(value);
            
            _nextId = Math.Max(_nextId, childId.Id + 1);
        }
    }

    public async Task Add(string idempotencyKey, long position)
    {
        var entry = new Entry(
            position,
            idempotencyKey,
            keyTtl == null ? -1 : utcNow().Ticks + keyTtl.Value.Ticks
        );
        int id;
        lock (_lock)
        {
            if (_dictionary.Any(e => e.Value.IdempotencyKey == idempotencyKey))
                return;
            
            id = _nextId++;
            _dictionary[id] = entry;
        }

        await effect.Upsert(parentId.CreateChild(id), entry.ToTuple(), alias: null, flush: false);
    }

    private async Task Remove(int id)
    {
        lock (_lock)
            _dictionary.Remove(id);
        
        await effect.Clear(parentId.CreateChild(id), flush: false);
    }

    public async Task CleanUp()
    {
        var now = utcNow().Ticks;
        List<int> toRemove;
        lock (_lock)
            toRemove = _dictionary
                .Where(kv => kv.Value.Ttl.HasValue && kv.Value.Ttl < now)
                .Select(kv => kv.Key)
                .ToList();
        
        foreach (var id in toRemove)
            await Remove(id);

        while (_dictionary.Count > maxCount)
            await Remove(_dictionary.Keys.Min());
    }

    public bool Contains(string idempotencyKey, long position)
    {
        lock (_lock)
            return _dictionary.Values.Any(s => s.IdempotencyKey == idempotencyKey && s.Position != position);
    }

    private record Entry(long Position, string IdempotencyKey, long? Ttl)
    {
        public Tuple<long, string, long?> ToTuple() => new(Position, IdempotencyKey, Ttl);

        public static Entry FromTuple(Tuple<long, string, long?> tuple) => new Entry(tuple.Item1, tuple.Item2, tuple.Item3);
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Queuing;

/// <summary>
/// The keys a flow has already accepted a message under. An entry knows nothing about where its message came from -
/// no position, no message identity: <see cref="Reserve"/> hands back the effect entry recording the key, and the
/// caller writes it in the same upsert as the message it admitted. The two are therefore durable together, which is
/// what makes the identity unnecessary - a recorded key always has its message persisted behind it, so a message
/// that comes back around is re-staged from that message rather than re-checked against its own key.
/// </summary>
internal class IdempotencyKeys(EffectId rootId, Effect effect, int maxCount, TimeSpan? keyTtl, UtcNow utcNow)
{
    private int _nextId;
    private readonly Dictionary<int, Entry> _dictionary = new();
    private readonly Lock _lock = new();

    public void Initialize()
    {
        var children = effect.GetChildren(rootId);

        foreach (var childId in children)
        {
            var value = effect.Get<Tuple<string, long?>>(childId);
            _dictionary[childId.Id] = Entry.FromTuple(value);
            _nextId = Math.Max(_nextId, childId.Id + 1);
        }

        CleanUp();
    }

    /// <summary>
    /// Claims the key for a message about to be staged and returns the effect entry recording it - which the caller
    /// must write together with that message. Null when the key is already held, meaning the message is a duplicate.
    /// </summary>
    public EffectResult? Reserve(string idempotencyKey)
    {
        CleanUp();

        var entry = new Entry(
            idempotencyKey,
            keyTtl == null ? null : utcNow().Ticks + keyTtl.Value.Ticks
        );
        int id;
        lock (_lock)
        {
            if (_dictionary.Values.Any(e => e.IdempotencyKey == idempotencyKey))
                return null;

            id = _nextId++;
            _dictionary[id] = entry;
        }

        return EffectResult.Create(rootId.CreateChild(id), entry.ToTuple());
    }

    private void Remove(int id)
    {
        lock (_lock)
            _dictionary.Remove(id);

        effect.FlushlessClear(rootId.CreateChild(id));
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

    private record Entry(string IdempotencyKey, long? Ttl)
    {
        public Tuple<string, long?> ToTuple() => new(IdempotencyKey, Ttl);

        public static Entry FromTuple(Tuple<string, long?> tuple) => new Entry(tuple.Item1, tuple.Item2);
    }
}

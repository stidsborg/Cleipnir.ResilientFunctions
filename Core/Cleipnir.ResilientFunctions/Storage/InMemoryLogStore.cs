using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Storage;

public class InMemoryLogStore : ILogStore
{
    private record LogState(Owner Owner, byte[] Content);
    
    private readonly Dictionary<StoredId, Dictionary<Position, LogState>> _logStates = new();
    private readonly object _sync = new();
    
    public Task<Position> Update(StoredId id, Position position, byte[] content, Owner owner)
    {
        lock (_sync)
            GetDictionary(id)[position] = new LogState(owner, content);

        return position.ToTask();
    }

    public Task Delete(StoredId id, Position position)
    {
        lock (_sync)
            GetDictionary(id).Remove(position);

        return Task.CompletedTask;
    }

    public Task<Position> Append(StoredId id, byte[] content, Owner owner)
    {
        lock (_sync)
        {
            var dict = GetDictionary(id);
            var position = dict.Count == 0 
                ? new Position("0") 
                : new Position((dict.Keys.Select(k => int.Parse(k.Value)).Max() + 1).ToString());
            
            dict[position] = new LogState(owner, content);
            return position.ToTask();
        }
    }

    public async Task<IReadOnlyList<Position>> Append(IEnumerable<AppendEntry> entries)
    {
        var positions = new List<Position>();
        foreach (var (storedId, owner, content) in entries)
        {
            var position = await Append(storedId, content, owner);
            positions.Add(position);
        }

        return positions;
    }

    public Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id)
    {
        lock (_sync)
            return GetDictionary(id)
                .Select(kv => new { Position = kv.Key, Content = kv.Value.Content, Owner = kv.Value.Owner })
                .Select(a => new StoredLogEntry(a.Owner, a.Position, a.Content))
                .ToList()
                .CastTo<IReadOnlyList<StoredLogEntry>>()
                .ToTask();
    }

    public Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id, Position offset)
    {
        return GetEntries(id)
            .Result
            .Where(e => int.Parse(e.Position.Value) > int.Parse(offset.Value))
            .ToList()
            .CastTo<IReadOnlyList<StoredLogEntry>>()
            .ToTask();
    }

    public Task<MaxPositionAndEntries> GetEntries(StoredId id, Position offset, Owner owner)
    {
        lock (_sync)
        {
            var allEntries = GetEntries(id).Result;
            if (allEntries.Count == 0)
                return new MaxPositionAndEntries(offset, Entries: []).ToTask();
            
            var entries = allEntries
                .Where(e => e.Owner == owner)
                .Where(e => int.Parse(e.Position.Value) > int.Parse(offset.Value))
                .ToList()
                .CastTo<IReadOnlyList<StoredLogEntry>>();

            var maxPosition = allEntries.Max(e => int.Parse(e.Position.Value));
            return new MaxPositionAndEntries(new Position(maxPosition.ToString()), entries).ToTask();
        }
    }

    private Dictionary<Position, LogState> GetDictionary(StoredId id)
    {
        lock (_sync)
            if (!_logStates.TryGetValue(id, out var logState))
                return _logStates[id] = new Dictionary<Position, LogState>();
            else
                return logState;
    }
}
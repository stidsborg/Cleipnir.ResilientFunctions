using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface ILogStore
{
    public Task<Position> Update(StoredId id, Position position, byte[] content, Owner owner);
    public Task Delete(StoredId id, Position position);
    public Task<Position> Append(StoredId id, byte[] content, Owner owner);
    public Task<IReadOnlyList<Position>> Append(StoredId id, IReadOnlyList<Tuple<Owner, Content>> contents);
    public Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id);
    public Task<IReadOnlyList<StoredLogEntry>> GetEntries(StoredId id, Position offset);
    public Task<MaxPositionAndEntries> GetEntries(StoredId id, Position offset, Owner owner);
}

public record MaxPositionAndEntries(Position MaxPosition, IReadOnlyList<StoredLogEntry> Entries);
public record StoredLogEntry(Owner Owner, Position Position, byte[] Content);
public record Owner(int Value);
public record Position(string Value);
public record Content(byte[] Value);
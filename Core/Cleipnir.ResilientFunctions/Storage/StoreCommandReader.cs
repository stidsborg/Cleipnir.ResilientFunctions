using System;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IStoreCommandReader : IAsyncDisposable
{
    int AffectedRows { get; }
    Task<bool> MoveToNextResults();
    Task<bool> ReadAsync();
    
    Task<bool> IsDbNullAsync(int ordinal); 
    Guid GetGuid(int ordinal);
    object GetValue(int ordinal);
    bool GetBoolean(int ordinal);
    int GetInt32(int ordinal);
    long GetInt64(int ordinal);
    string GetString(int ordinal);
    bool IsDbNull(int ordinal);
}
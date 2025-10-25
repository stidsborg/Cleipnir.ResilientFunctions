using System.Collections.Generic;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IStoreReader<T>
{
    public List<T> Read();
}
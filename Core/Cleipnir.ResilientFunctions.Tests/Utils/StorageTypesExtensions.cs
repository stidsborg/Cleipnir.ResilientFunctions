using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class StorageTypesExtensions
{
    public static object DefaultDeserialize(this StoredParameter parameter)
        => parameter.Deserialize<object>(DefaultSerializer.Instance);

    public static object? DefaultDeserialize(this StoredResult result)
        => result.Deserialize<object>(DefaultSerializer.Instance);
}
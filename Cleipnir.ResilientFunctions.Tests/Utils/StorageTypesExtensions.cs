using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class StorageTypesExtensions
{
    public static object DefaultDeserialize(this StoredParameter parameter)
        => parameter.Deserialize(DefaultSerializer.Instance);

    public static RScrapbook DefaultDeserialize(this StoredScrapbook scrapbook)
        => scrapbook.Deserialize(DefaultSerializer.Instance);

    public static object? DefaultDeserialize(this StoredResult result)
        => result.Deserialize(DefaultSerializer.Instance);
}
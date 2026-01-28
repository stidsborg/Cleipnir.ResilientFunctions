using System;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public interface ISerializer
{
    byte[] Serialize(object value, Type type);
    object Deserialize(byte[] bytes, Type type);

    Type? ResolveType(byte[] type) => Type.GetType(type.ToStringFromUtf8Bytes());
    byte[] SerializeType(Type type) => type.SimpleQualifiedName().ToUtf8Bytes();
}
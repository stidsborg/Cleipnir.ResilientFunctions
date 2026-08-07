using System;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public interface ISerializer
{
    byte[] Serialize(object value, Type type);
    object Deserialize(byte[] bytes, Type type);
}
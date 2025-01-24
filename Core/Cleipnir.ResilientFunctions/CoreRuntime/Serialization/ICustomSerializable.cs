namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public interface ICustomSerializable
{
    public byte[] Serialize(ISerializer serializer);
    public static abstract object Deserialize(byte[] bytes, ISerializer serializer);
}
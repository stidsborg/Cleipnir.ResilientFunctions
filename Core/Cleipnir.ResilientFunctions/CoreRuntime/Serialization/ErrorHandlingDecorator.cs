using System;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Serialization;

public class ErrorHandlingDecorator(ISerializer inner) : ISerializer
{
    public byte[] Serialize(object value, Type type)
        => inner.Serialize(value, type);

    public object Deserialize(byte[] bytes, Type type)
    {
        try
        {
            return inner.Deserialize(bytes, type)!;
        }
        catch (DeserializationException)
        {
            throw;
        }
        catch (Exception e)
        {
            throw new DeserializationException(
                $"Unable to deserialize value with type: '{type.FullName}' and bytes: '{Convert.ToBase64String(bytes)}'",
                e
            );
        }
    }
}

public static class ErrorHandlingDecoratorExtensions
{
    public static ISerializer DecorateWithErrorHandling(this ISerializer serializer)
        => new ErrorHandlingDecorator(serializer);
}
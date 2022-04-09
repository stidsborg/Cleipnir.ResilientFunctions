using System;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public record RError(string ErrorMessage, string StackTrace, string ErrorType);

public static class RErrorExtensions
{
    public static RError ToError(this Exception exception)
        => new RError(
            exception.Message,
            exception.StackTrace ?? "",
            ErrorType: exception.GetType().SimpleQualifiedName()
        );
}
using System;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.Domain;

public record Error(string ErrorMessage, string StackTrace, string ErrorType);

public static class RErrorExtensions
{
    public static Error ToError(this Exception exception)
        => new Error(
            exception.Message,
            exception.StackTrace ?? "",
            ErrorType: exception.GetType().SimpleQualifiedName()
        );
}
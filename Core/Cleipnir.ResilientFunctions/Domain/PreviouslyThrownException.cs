using System;

namespace Cleipnir.ResilientFunctions.Domain;

public record PreviouslyThrownException(string ErrorMessage, string? StackTrace, Type ErrorType);
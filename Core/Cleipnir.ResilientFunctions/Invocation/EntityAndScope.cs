using System;

namespace Cleipnir.ResilientFunctions.Invocation;

public record EntityAndScope<T>(T Entity, Action DisposeScope);
using System;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal record RestartedFunction(StoredFunction StoredFunction, IDisposable RunningFunctionDisposable);
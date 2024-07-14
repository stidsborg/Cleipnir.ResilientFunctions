using System;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal record RestartedFunction(StoredFlow StoredFlow, IDisposable RunningFunctionDisposable);
using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.SignOfLife;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class NeverExecutingSignOfLifeUpdaterFactory : ISignOfLifeUpdaterFactory
{
    public IDisposable CreateAndStart(FunctionId functionId, int epoch) => DisposableEmpty.Instance;
}
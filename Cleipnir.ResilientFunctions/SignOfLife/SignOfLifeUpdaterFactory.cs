using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.SignOfLife
{
    internal interface ISignOfLifeUpdaterFactory
    {
        IDisposable CreateAndStart(FunctionId functionId, int epoch);
    }

    internal class SignOfLifeUpdaterFactory : ISignOfLifeUpdaterFactory
    {
        private readonly IFunctionStore _functionStore;
        private readonly Action<RFunctionException> _unhandledExceptionHandler;
        private readonly TimeSpan? _updateFrequency;

        public SignOfLifeUpdaterFactory(
            IFunctionStore functionStore, 
            Action<RFunctionException> unhandledExceptionHandler, 
            TimeSpan unhandledFunctionsCheckFrequency
        )
        {
            _functionStore = functionStore;
            _unhandledExceptionHandler = unhandledExceptionHandler;
            _updateFrequency = unhandledFunctionsCheckFrequency / 2;
        }

        public IDisposable CreateAndStart(FunctionId functionId, int epoch)
        {
            var signOfLifeUpdater = new SignOfLifeUpdater(
                functionId,
                epoch,
                _functionStore,
                _unhandledExceptionHandler,
                _updateFrequency
            );
            
            _ = signOfLifeUpdater.Start();
            return signOfLifeUpdater;
        }
    }
}
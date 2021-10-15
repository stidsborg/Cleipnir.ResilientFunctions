using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions
{
    public class SignOfLifeUpdaterFactory
    {
        private readonly IFunctionStore _functionStore;
        private readonly TimeSpan? _updateFrequency;

        public SignOfLifeUpdaterFactory(IFunctionStore functionStore, TimeSpan unhandledFunctionsCheckFrequency)
        {
            _functionStore = functionStore;
            _updateFrequency = unhandledFunctionsCheckFrequency / 2;
        }

        public SignOfLifeUpdater CreateAndStart(FunctionId functionId, long expectedSignOfLife)
        {
            var signOfLifeUpdater = new SignOfLifeUpdater(
                functionId,
                expectedSignOfLife, 
                _functionStore, 
                _updateFrequency
            );
            
            _ = signOfLifeUpdater.Start();
            return signOfLifeUpdater;
        }
    }
}
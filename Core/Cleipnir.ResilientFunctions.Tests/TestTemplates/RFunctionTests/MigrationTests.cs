using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class MigrationTests
{
    public abstract Task MigrationExceptionIsThrownOnVersionMismatch();
    protected async Task MigrationExceptionIsThrownOnVersionMismatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var migrator = store.Migrator;
        await migrator.SetVersion(0);
        await store.Initialize().ShouldThrowAsync<SchemaMigrationRequiredException>();
        await migrator.SetVersion(Version.CurrentMajor);
    }
    
    public abstract Task InitializeSucceedsOnVersionMatch();
    protected async Task InitializeSucceedsOnVersionMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var atVersion = await store.Migrator.Initialize(Version.CurrentMajor);
        atVersion.ShouldBe(Version.CurrentMajor);
    }
}
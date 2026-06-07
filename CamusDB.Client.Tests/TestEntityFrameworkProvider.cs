using CamusDB.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.Client.Tests;

public class TestEntityFrameworkProvider
{
    [Fact]
    public void TestUseCamusDBRegistersOptionsExtension()
    {
        DbContextOptionsBuilder builder = new();

        builder.UseCamusDB("Endpoint=http://localhost:5095;Database=test");

        CamusDBOptionsExtension? extension = builder.Options.FindExtension<CamusDBOptionsExtension>();

        Assert.NotNull(extension);
        Assert.Equal("Endpoint=http://localhost:5095;Database=test", extension!.ConnectionString);
    }

    [Fact]
    public void TestAddEntityFrameworkCamusDBRegistersDatabaseProvider()
    {
        ServiceCollection services = new();

        services.AddEntityFrameworkCamusDB();

        ServiceProvider provider = services.BuildServiceProvider();
        IDatabaseProvider? databaseProvider = provider.GetService<IDatabaseProvider>();

        Assert.NotNull(databaseProvider);
    }
}

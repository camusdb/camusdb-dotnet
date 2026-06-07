using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusDBDbContextOptionsBuilder
{
    public CamusDBDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    {
        OptionsBuilder = optionsBuilder;
    }

    public DbContextOptionsBuilder OptionsBuilder { get; }
}

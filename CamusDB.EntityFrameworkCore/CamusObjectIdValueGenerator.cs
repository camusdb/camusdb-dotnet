using CamusDB.Core.Util.ObjectIds;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ValueGeneration;

namespace CamusDB.EntityFrameworkCore;

public sealed class CamusObjectIdValueGenerator : ValueGenerator<string>
{
    public override bool GeneratesTemporaryValues => false;

    public override string Next(EntityEntry entry) => CamusObjectIdGenerator.GenerateAsString();
}

using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace CamusDB.EntityFrameworkCore;

public class CamusDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection services)
    {
        services.AddEntityFrameworkCamusDB();
        services.AddSingleton<AnnotationCodeGeneratorDependencies>();
        services.AddSingleton<IAnnotationCodeGenerator, CamusAnnotationCodeGenerator>();
    }
}

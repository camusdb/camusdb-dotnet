# CamusDB Connector for .NET

.NET idiomatic client libraries for [CamusDB](https://github.com/camusdb/camusdb)

CamusDB.Client is the ADO.NET provider for CamusDB. It is the recommended package for regular CamusDB database access from .NET.

## Installation

Install the CamusDB.Client package from NuGet. Add it to your project in the normal way (for example by right-clicking on the project in Visual Studio and choosing "Manage NuGet Packages...").

#### Using .NET CLI

```shell
dotnet add package CamusDB.Client --version 0.0.4-alpha
```

### Using NuGet Package Manager

Search for CamusDB.Client and install it from the NuGet package manager UI, or use the Package Manager Console:

```shell
Install-Package CamusDB.Client -Version 0.0.4-alpha
```

### Run Tests

To run the unit tests, it is necessary to have an instance of CamusDB running on the local machine on the standard port 7141. 
After this, the tests can be run with the following command:

```shell
dotnet test -l "console;verbosity=normal" --filter  "FullyQualifiedName~CamusDB.Client.Tests"
```

## Contribution

CamusDB.Client is an open-source project, and contributions are heartily welcomed! Whether you are looking to fix bugs, add new features, or improve documentation, your efforts and contributions will be appreciated. Check out the CONTRIBUTING.md file for guidelines on how to get started with contributing to CamusDB.Client.

## License

CamusDB.Client is released under the MIT License.

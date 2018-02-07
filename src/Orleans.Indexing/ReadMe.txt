Porting the Indexing Code
-------------------------

Purpose
-------

Port the Indexing code to the latest Orleans runtime.

Nomenclature
------------

In this document I shall refer to the working implementation of
the indexing code written by Mohammad Dashti as V1 and the
port of the code, which I am currently writing as V2. The V1
code uses .NET 4.5.1 while V2 uses .NET Standard 2.0.
In addition V1 is based on an old version of the Orleans runtime
while V2 is based on the lastest version of the Orleans runtime.

Strategy
--------

In the ideal world all that would have to be done is copy over
the indexing files from V1 to V2 build and run. Unfortunately
the porting process is not that easy as a number of
difficulties have arisen. This include

    - dropping of inheritance from the runtime architecture
    - access to the current Silo has changed
    - etc

Git Repositories
----------------

    V1: https://github.com/OrleansContrib/Orleans.Indexing.git
    V2: https://github.com/KirkOlynyk/orleans.git

If you wish, in our porting job V1 is the source and V2 is
the destination.

The V1 code base is never touched. All changes are made on the V2
side. I have not settled on a strategy for having someone other
than me contribute to V2. For now I suppose you could create a branch
to work on and then submit a pull request to me. I work in the
V2 'develop' branch and check in early and often. I you want
to see my latest progress look there.


Accessing the current Silo
--------------------------

As of 2/5/2018 there is no access to the current Silo in the indexing code.
This means that some of the V1 indexing code will not compile.
In order to get around this so I could make progress I have
commented out some sections of code that will not compile in the V2
environment. These are sections are marked in the following
manner

    #if (!RUNTIMECLIENT)
        Code that accesses current Silo
    #endif

Affected Files:
- src\Orleans.Indexing\Core\IndexRegistry.cs
- src\Orleans.Indexing\Core\Utils.TypeCodeMapper.cs

Using the Test Framework
------------------------

Enable Test Framework debugging on your development machine
-----------------------------------------------------------

In order to run the Tests under the Test Exporer in Visual Studio
you must change the settings under
Test -> Test Settings -> Default Processor Architecture
to X64

Compiling the Test Framwork
---------------------------

This is the way that I do it. It works but I am not sure that
it is optimal.

- Open the Solution Explorer window
- Right Click on the Test Directory
- Choose 'Rebuild'
- After the build is successful, open the Team Explorer
  by chosing 'Test -> Windows -> Test Explorer'

Because the tests have just been built the Test Exploer will
take some time scanning the tests for those annotated with
[Fact]. You search for your test by typing the name of the
test in the search box. The name of your test should be
filtered to into the the list.

- Right click on your test name in the Test Explorer windown
  and choose to Run or Debug. If you choose to Debug make
  sure you have a break point set somewhere in your test
  code.

- After running the test you can check the output to the
  Test output window by going to the Test Explorer window
  and clicking on "Output" at the bottom of the window.
  An output window will appear just underneath the
  Test Explorer window. You can also get the same
  output by looking at the log file generated in the logs
  directory, in my case it is located in

  orleans\test\Orleans.Indexing.Tests\bin\Debug\net461\win10-x64\logs

  But yourse will appropriately different.

Adding your own test to the Test Framework
------------------------------------------
Each test is marked with a [Fact] annontation.


Enable Code Generation in the Test Framework
--------------------------------------------
Insert the following line the test's
(and any other grain application) .csproj file:

<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <BlahBlahBlah> ... </BlahBlahBlah>
    ....
    <OrleansBuildTimeCodeGen>true</OrleansBuildTimeCodeGen> <-- add this!
  </PropertyGroup>
  ...
</Project>


Suggested Work
--------------

When a grain is activated on the server side it calls the
following code.

[<root>\orleans\src\Orleans.Runtime\Catalog\Catalog.cs]

namespace Orleans.Runtime {
  internal class Catalog {
    public ActivationData GetOrCreateActivation(
            ActivationAddress address,
            bool newPlacement,
            string grainType,
            string genericArguments,
            Dictionary<string, object> requestContextData,
            out Task activatedPromise)
  }
}
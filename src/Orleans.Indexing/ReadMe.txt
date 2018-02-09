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
while V2 is based on the latest version of the Orleans runtime.
In this document, I refer to the roots of the V1 and V2 clones
of the Git repositories as <V1> and <V2> respectively.

Status
------

I have copied most of the indexing code from V1 to V2 and
created some tests as described below. About 2/3 of the
indexing code compiles and is now part of the assembly.
The code that I copied is taken from the OrleansIndexing
directory of V1. This is not all of the code that has to 
be ported. In order to make indexing work in V1, some 
modifications were made to the Orleans runtime. These 
modifications have not been moved to V2 as yet.


Useful People 
-------------

- Phil Bernstein
- Xiao Zeng

  Xiao is a developer in the current Orleans project and has helped
  me set up the tests.


What is Orleans?
----------------

Here is my understanding. Orleans is a framework for a distributed 
service. It is composed of Silos (servers) and Grains (service classes).
The location of the Grains are virtual. I think of them as equivalent
to a virtual memory system where memory pages can be swapped to
an from disk without the uses knowledge. In the same way Grain can 
be moved to and from disk or Silos as the Orleans system requires but
the user can always access a specified Grain and Orleans will make 
sure you get access to it. Indexing is a way of accessing multiple 
Grains. For example, you can get access to all the grains with the 
same name or other property. 

How does Indexing work?
-----------------------

In V1, Grains are marked indexable by inheritance. The properties
of the grain that are to be indexed are marked using .NET annotations.
At Silo startup time, all the classes in the assembly are searched
for indexable classes and their indexed properties using reflection.
A dictionary is created in the Silo for each of the Class-IndexProperty
types. This dictionary will keep track of the values of the 
property and a list of the Grains that have been assigned a
particular value to that property. For example, suppose there
is a grain type called Engineer which has an indexable property called
Name, the the corresponding dictionary will effectively be a function
of type: string -> Grain list, that is, you give the function
a name and it will return a list of all the Engineer grains that have
that value for a Name.

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


Work Item: Clone these repositories

If you wish, in our porting job V1 is the source and V2 is
the destination.

The V1 code base is never touched. All changes are made on the V2
side. I have not settled on a strategy for having someone other
than me contribute to V2. For now I suppose you could create a branch
to work on and then submit a pull request to me. I work in the
V2 'develop' branch and check in early and often. I you want
to see my latest progress look there.

I work by cloning both of the repositories above and working side by
side. I have already copied most of indexing code from V1 to V2. To 
see how indexing works I run tests in V1 and trace it in the debugger. 
Then I create a similar test in V2 and compare. Of course there are 
some complications.


Accessing the current Silo
--------------------------

As of 2/5/2018 there is no access to the current Silo in the indexing code.
This means that some of the V1 indexing code will not compile.
In order to get around this so I could make progress I have
commented out some sections of code that will not compile in the V2
environment. These are sections are marked in the following
manner

    #if false
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

Compiling the Test Framework
---------------------------

This is the way that I do it. It works but I am not sure that
it is optimal.

- Open the Solution Explorer window
- Right Click on the Test Directory
- Choose 'Rebuild'
- After the build is successful, open the Team Explorer
  by choosing 'Test -> Windows -> Test Explorer'

Because the tests have just been built the Test Explorer will
take some time scanning the tests for those annotated with
[Fact]. You search for your test by typing the name of the
test in the search box. The name of your test should be
filtered to into the the list.

- Right click on your test name in the Test Explorer window
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

  But your version will appropriately different.

Adding your own test to the Test Framework
------------------------------------------
Each test is marked with a [Fact] annotation.


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

[<V2>\orleans\src\Orleans.Runtime\Catalog\Catalog.cs]

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

What is done, what is not
-------------------------

I have copied all the V1 files under the src\OrleansIndexing  directory
to the V2 project under src\Orleans.Indexing. I changed the name
in order to be more consistent with the naming conventions used
in the V2 project. In addition to moving the files I have changed
<V2>\orleans.sln to include the new files in the build. There 
are a total of 90 .cs files that were copied from V1 to V2, and 
most of them compile with only minor modifications. These modifications 
included putting "I" in front of interface definitions and 
adding "this." in front of reference to class member references.
These were required by the current style requirements of my project.
I am not sure if these requirements were part of the solution as 
written by the Orleans team or just a default of my Visual Studio. In
any case I made the cosmetic changes in order to get the build to 
work. 

Here is a list of files that have some or all their code commented
out with #if false .. #endif brackets in order to get the build
to work:

- Core\FaultTolerance\IndexableGrain.cs
- Core\FaultTolerance\IndexWorkflowQueueBase.cs
- Core\FaultTolerance\IndexWorkflowQueueHandlerBase.cs
- Core\FaultTolerance\IndexWorkflowQueueHandlerSystemTarget.cs
- Core\FaultTolerance\IndexWorkflowQueueSystemTarget.cs
- Core\FaultTolerance\ReincarnatedIndexWorkflowQueue.cs
- Core\FaultTolerance\ReincarnatedIndexWorkflowQueueHandler.cs
- Core\IndexableGrainNonFaultTolerant.cs
- Core\IndexFactory.cs
- Core\IndexRegistry.cs
- Core\Utils\SiloUtils.cs
- Core\Utils\TypeCodeMapper.cs
- Extensions\GrainExtensions.cs
- Extensions\GrainFactoryExtensions.cs
- Extensions\IndexExtensions.cs
- Indexes\ActiveIndexes\ActiveHashIndexPartitionedPerKeyBucketImpl.cs
- Indexes\ActiveIndexes\ActiveHashIndexPartitionedPerSiloBucketImpl.cs
- Indexes\ActiveIndexes\ActiveHashIndexPartitionedPerSiloImpl.cs
- Indexes\ActiveIndexes\ActiveHashIndexSingleBucketImpl.cs
- Indexes\HashIndexPartitionedPerKeyBucket.cs
- Indexes\HashIndexSingleBucket.cs
- Indexes\StorageManagedIndexes\DirectStorageManagedIndexImpl.cs
- Indexes\TotalIndexes\TotalHashIndexPartitionedPerKeyBucketImpl.cs
- Indexes\TotalIndexes\TotalHashIndexSingleBucketImpl.cs
- Query\OrleansQueryProvider.cs
- Query\QueryIndexedGrainsNode.cs
- Scanners\ActiveGrainEnumeratorGrain.cs
- Scanners\ActiveGrainScanner.cs

That is a total of 28 files, about a third of all the files. I don't 
think that this is a disaster. A large portion of the build difficulties 
arise from the fact that V2 no longer supports access to the interface
IRuntimeClient " .. a subset of the runtime API that is exposed to
both silo and client ..."

In the neutered code list above there are many reference to 

  RuntimeClient.Current

which must be resolved in the port to V2.

Tests
-----

I have created a number of tests in V2. One set of tests uses the
.NET Test framework provided in Visual Studio. It made part of the 
larger test suite written by the Orleans team. My part of this suite is
found in 

  <V2>\test\Orleans.Indexing.Tests
    Develop.cs
    SimpleTests.cs

These files contain several tests that can be run individually in
the test suite framework.

- Orleans.Indexing.Tests.Develop.IndexSimpleTest2

  This is a test that inspects the assembly and find
  "Indexable" grains and properties in a manner 
  that follows the Silo initialization procedure in V1. The 
  idea was to create the same dictionaries as are created in V1. 
  This works is unfinished.

- Orleans.Indexing.Tests.SimpleTests.Test1

  This creates a grain of our construction using the new grain
  registration procedure of V2.


Building V2
-----------

This is what I do. I am not sure that it is optimal. First I build
from the command line. Build the project using the build.cmd script

root> build.cmd

Then, from the root directory, I start "Orleans.sln". Once again, I 
build the entire project. Not sure if this is necessary but that is what 
I do.

Building and Running the Test suite
-----------------------------------

This assumes that you have modified the test code.
Inside Visual Studio, I highlight "test" in the Solution Explorer, right 
click and build.

IMPORTANT: Make sure that the test targets x64. To do this click
"Test->Test Settings->Default Processor Architecture->x64"

After the build make sure that you have made the 
Test Explorer window visible. This is done by going to the top menu bar and 
clicking "Test->Windows->Test Explorer"

After building the test suite, the Test Explorer will look into the code
and search for those functions that have been annotated with [Fact]. 
After some time, the Test Explorer will present a list of all such
marked tests. You can use the search dialog to find your particular test.
From that you right click on that test and choose to view, run or debug.
If you wish to debug then make sure that you have set a break point
somewhere in your code.

Building V1
-----------

cd <V1>
build.cmd

or

- cd <V1>\src
- open Orleans.sln with Visual studio
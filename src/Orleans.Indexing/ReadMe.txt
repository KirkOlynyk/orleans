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

In order to run the Tests under the Test Exporer in Visual Studio
you must change the settings under
Test -> Test Settings -> Default Processor Architecture
to X64

Adding your own test to the Test Framework
------------------------------------------
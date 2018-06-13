# presto-kudu
presto connector for kudu database

注意: presto-kudu执行器目前已经有德国大佬开发了一个,后续应该会合并到presto官方分支中.
这里目前只作为学习的demo

## Requirements

* Mac OS X or Linux
* Presto 0.203
* Java 8 Update 92 or higher (8u92+), 64-bit
* Maven 3.3.9+ (for building)
* Python 2.7+ (for running with the launcher script)
* kudu client 1.4.0

## Building Presto

Presto-kudu is a standard Maven project. Simply run the following command from the project root directory:

    ./mvnw clean install "-Dair.main.basedir=#presto source root#"

On the first build, Maven will download all the dependencies from the internet and cache them in the local repository (`~/.m2/repository`), which can take a considerable amount of time. Subsequent builds will be faster.

Presto has a comprehensive set of unit tests that can take several minutes to run. You can disable the tests when building:

    ./mvnw clean install -DskipTests "-Dair.main.basedir=#presto source root#"


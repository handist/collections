# handist collections library

A Java distributed collections library.

## Documentation / Information

| Version                                                              |                                                                                                                                                                                   |
|-|-|
| master head                                                          | [Javadoc](master-latest/apidocs/index.html)<br>[Test Coverage Report](master-latest/jacoco/index.html)                                                                            |
| [v1.3.0](https://github.com/handist/collections/releases/tag/v1.3.0) | May 11th 2022: Fourth release of the library<br>[Javadoc](https://handist.github.io/collections/v1.3.0/apidocs/index.html)<br>[Test Coverage Report](https://handist.github.io/collections/v1.3.0/jacoco/index.html)<br>[Maven Report](https://handist.github.io/collections/v1.3.0/index.html)     |
| [v1.2.0](https://github.com/handist/collections/releases/tag/v1.2.0) | March 4th 2022: Third release of the library<br>[Javadoc](https://handist.github.io/collections/v1.2.0/apidocs/index.html)<br>[Test Coverage Report](https://handist.github.io/collections/v1.2.0/jacoco/index.html)<br>[Maven Report](https://handist.github.io/collections/v1.2.0/index.html)     |
| [v1.1.0](https://github.com/handist/collections/releases/tag/v1.1.0) | October 16th 2021: Second release of the library<br>[Javadoc](https://handist.github.io/collections/v1.1.0/apidocs/index.html)<br>[Test Coverage Report](https://handist.github.io/collections/v1.1.0/jacoco/index.html)<br>[Maven Report](https://handist.github.io/collections/v1.1.0/index.html) |
| [v1.0.0](https://github.com/handist/collections/releases/tag/v1.0.0) | March 27th 2021: First release of the library<br>[Javadoc](https://handist.github.io/collections/v1.0.0/apidocs/index.html)<br>[Test Coverage Report](https://handist.github.io/collections/v1.0.0/jacoco/index.html)<br>[Maven Report](https://handist.github.io/collections/v1.0.0/index.html)    |

## Build instructions

### Dependencies

This Java libraries relies on a number of libraries:

+ A slightly customized version of [APGAS for Java](https://github.com/handist/apgas)
+ [OpenMPI](https://www.open-mpi.org/) which provides both an implementation of MPI and bindings for Java
+ [MPI-JUnit](https://github.com/handist/mpi-junit/) to run the distributed tests

The **MPI-JUnit** dependency will be downloaded automatically. 
However, you will need to install OpenMPI _with its Java bindings_ as well as _APGAS for Java_ manually.

1. First, download and compile a version of OpenMPI on your system. 
2. Then set the environment variable `OPENMPI_LIB` to indicate the location of the `mpi.jar` file created during OpenMPI compilation (set `OPENMPI_LIB` such that the compiled JAR can be found at `${OPENMPI_LIB}/mpi.jar`). 
3. Clone repository [git@github.com:handist/apgas.git](git@github.com:handist/apgas.git) and run command `mvn install` to compile and install the APGAS for Java library into your local Maven installation (by default under `~/.m2` directory).

To check that the OpenMPI environment is set correctly and to automatically download and install the **APGAS for Java** and **MPI-JUnit** projects, you can run the `install-dependencies.sh` script. 
If all the requirements are there, this script will output something like the following (the installation path of OpenMPI may differ on your system):

```shell
user@host:~/collections$ ./install-dependencies.sh
Checking if OPENMPI_LIB environment variable is defined ...
- OPENMPI_LIB: /home/user/openmpi-3.1.6/lib
Checking that /home/user/openmpi-3.1.6/lib/mpi.jar is present ...
- OPENMPI_LIB/mpi.jar found
Checking if Maven is installed ...
- Maven is installed
Apache Maven 3.6.0
Maven home: /usr/share/maven
Java version: 1.8.0_342, vendor: Private Build, runtime: /usr/lib/jvm/java-8-openjdk-amd64/jre
Default locale: en_US, platform encoding: UTF-8
OS name: "linux", version: "4.15.0-189-generic", arch: "amd64", family: "unix"
Local Repository located at: /home/user/.m2/repository
- Dependency com.github.handist:mpi-junit:v1.2.3 already installed
- Dependency com.github.handist:apgas:v2.0.0 already installed
```

### Compiling the project

We use Maven to compile and test the library.
You can compile the library from source by cloning the GitHub and running command `mvn package`.
This will create two java archives under folder `core/target/`: 

- `core-v1.4.0.jar` which contains the distributed collections
- `core-v1.4.0-jar-with-dependencies.jar` which contains the distributed collections as well as the dependencies our library relies on

If you want to register these JARs in your local Maven repository (located in `~/.m2` by default), use command `mvn install`.

You will then be able to use this project as a dependency in your own project with 
- **groupId**: com.github.handist.collections
- **artifactId**: core

### Running the tests

There are two kinds of tests for this project:

+ Normal Junit4 tests (classes named `Test<class under test>.java`)
+ Tests dealing with distributed features of the library that involve multiple hosts (classes named `IT_<class under test>.java`)

The former are bound to the `test` phase of the standard lifecycle of Maven.
You can run them without any prerequisite using the `mvn test`.
They will also be run when generating the Java ARchive with `mvn package`.

The distributed tests are not enabled by default.
In order to run them, you need the OpenMPI library compiled with the Java bindings installed on your system.
To run the tests, use command `mvn verify -Popenmpi`.

### Generating Project report / Javadoc / Code Coverage report

Use command `mvn site` to generate the standard Maven project report and the Javadoc. 
If you successfully ran `mvn verify -Popenmpi` prior to running command `mvn site`, the code coverage report (which includes the distributed tests) will also be generated.
After the command has run, you can browse the generated HTML pages in `target/site`, `core/target/site` etc.

## Program execution

We rely on MPI to support the various communication patterns used by our library. 
To launch a program written with our library, follow this general pattern:


```bash
mpirun -np 4 --hostfile ${HOSTFILE} \\ 
	java -cp core-v1.4.0-jar-with-dependencies.jar:program.jar \\
	-Djava.library.path=${OPENMPI_LIB} \\ 
	handist.collection.launcher.Launcher \\
	${MAIN_CLASS} firstArgument secondArgument
```

- The nubmer of processes used and their allocation is set using the usual `mpirun` command options (-np / hostfile).
- As per usual Java programs, specify the classpath through the `-cp` option.
- Specify the location of the compiled OpenMPI library
- The main class for the program needs to be our launcher `handist.collection.launcher.Launcher`. Pass your main class and its arguments as arguments to the launcher.

## Related projects

This work was inspired by the distributed collections library of X10. You can check this project named "Cassia" there: [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github.

The following programs rely on this library:
- [PlhamJ](https://github.com/plham/plhamJ) financial market simulator

## Authors
- [Tomio Kamada](https://www.nc.ii.konan-u.ac.jp/members/kamada/)
- [Patrick Finnerty](https://www.fine.cs.kobe-u.ac.jp/finnerty/)
- Yoshiki Kawanishi

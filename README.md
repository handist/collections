# handist collections library

A Java distributed collections library.

## Documentation / Information

| Version                                                              |                                                                                                                                                                                   |
| master head                                                          | [Javadoc](master-latest/apidocs/index.html)<br>[Test Coverage Report](master-latest/jacoco/index.html)                                                                            |
| [v1.2.0](https://github.com/handist/collections/releases/tag/v1.2.0) | March 2nd 2022: Third release of the library<br>[Javadoc](v1.2.0/apidocs/index.html)<br>[Test Coverage Report](v1.2.0/jacoco/index.html)<br>[Maven Report](v1.2.0/index.html)     |
| [v1.1.0](https://github.com/handist/collections/releases/tag/v1.1.0) | October 16th 2021: Second release of the library<br>[Javadoc](v1.1.0/apidocs/index.html)<br>[Test Coverage Report](v1.1.0/jacoco/index.html)<br>[Maven Report](v1.1.0/index.html) |
| [v1.0.0](https://github.com/handist/collections/releases/tag/v1.0.0) | March 27th 2021: First release of the library<br>[Javadoc](v1.0.0/apidocs/index.html)<br>[Test Coverage Report](v1.0.0/jacoco/index.html)<br>[Maven Report](v1.0.0/index.html)    |

## Compiling projects with this library as a dependency

Our library comes in the form of a Maven project. 

Its artifacts can be downloaded automatically from [JitPack](https://jitpack.io/#handist/collections).
Alternatively, you can clone this repository and run `mvn install` to install this library to your local Maven repository. 

In either case you will then be able to use this project as a dependency with _groupId: com.github.handist_ _artifactId: collections_. 

## Program execution

We rely on MPI to support the various communication patterns used by our library. 
We use the [MPJ-Express library](http://www.mpjexpress.org/) as the intermediary compatibility layer between Java and the "native" C MPI calls.
It is necessary to compile the ``native'' part of this library prior to execution.
Fortunately, this is thoroughly explained in the [MPJ-Express documentation](http://www.mpjexpress.org/documentation.html).

To launch a program written with our library, follow this general pattern:


```bash
mpirun -np 4 --hostfile ${HOSTFILE} \\ 
	java -cp collections-v1.2.0.jar:program.jar \\
	-Djava.library.path=${MPJ_HOME}/lib \\ 
	handist.collection.launcher.Launcher \\
	${MAIN_CLASS} firstArgument secondArgument
```

- The nubmer of processes used and their allocation is set using the usual `mpirun` command options (-np / hostfile).
- As per usual Java programs, specify the classpath through the `-cp`.
- Specify the location of the MPJ-Express shared library using `-Djava.library.path`. As per the MPJ-Express compilation instructions, its location is usually `${MPJ_HOME}/lib`.
- The main class for the program needs to be our launcher `handist.collection.launcher.Launcher`. Pass your main class and its arguments as arguments to our launcher.

## Build instructions (MAVEN)

### Compiling the project

We use Maven to compile and test the library.
You can compile the library from source by checking it out with github and running command `mvn package`.
This will create two java archives under folder `target/`: 

- `collections-v1.2.0.jar` which contains only the source files of our library
- `collections-v1.2.0-jar-with-dependencies.jar` which also contains the dependencies our library depends on

### Running the tests

There are two kinds of tests for this project:

+ Normal Junit4 tests (classes named `Test<class under test>.java`)
+ Tests dealing with distributed features of the library that involve multiple hosts (classes named `IT_<class under test>.java`)

The former are bound to the `test` phase of the standard lifecycle of Maven.
You can run them without any prerequisite using the `mvn test`.
They will also be run when generating the Java ARchive with `mvn package`.

The distributed tests are not enabled by default.
In order to run them, you need MPI and the "native" MPJ-Express library installed on your system.
To run the tests, use command `mvn verify -PmpjNative`.
The "mpjNative" profile requires that the `MPJ_HOME` environment variable be defined appropriately. 
For more details, we refer you to the [compilation instructions of the MPJ library](http://mpj-express.org/)

### Dependencies

This Java libraries relies on a number of libraries:

+ A slightly customized version of [APGAS for Java](https://github.com/handist/apgas)
+ [MPJ-Express](http://mpj-express.org/) which provides Java bindings to native MPI calls
+ [MPI-JUnit](https://github.com/handist/mpi-junit/) to run the distributed tests

## Related projects

This work was inspired by the distributed collections library of X10. You can check this project named "Cassia" there: [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github.

The following programs rely on this library:
- [PlhamJ](https://github.com/plham/plhamJ) financial market simulator

# Authors
- Tomio Kamada
- Patrick Finnerty
- Yoshiki Kawanishi

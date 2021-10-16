# handist collections library

A Java distributed collections library.

# Documentation / Information

| Version     |                                                                                                                                                                                |
| master head | [Javadoc](master-latest/apidocs/index.html)<br>[Test Coverage Report](master-latest/jacoco/index.html)                                                                         |
| [v1.1.0](https://github.com/handist/collections/releases/tag/v1.1.0)  | October 16th 2021: Second release of the library<br>[Javadoc](v1.1.0/apidocs/index.html)<br>[Test Coverage Report](v1.1.0/jacoco/index.html)<br>[Maven Report](v1.1.0/index.html) |
| [v1.0.0](https://github.com/handist/collections/releases/tag/v1.0.0)  | March 27th 2021: First release of the library<br>[Javadoc](v1.0.0/apidocs/index.html)<br>[Test Coverage Report](v1.0.0/jacoco/index.html)<br>[Maven Report](v1.0.0/index.html) |

# Build instructions (MAVEN)

## Dependencies

This Java libraries relies on a number of libraries:

+ A slightly customized version of [APGAS for Java](https://github.com/x10-lang/apgas/tree/master/apgas) [located in this repository](https://github.com/handist/apgas)
+ A library providing Java bindings to MPI calls: either [MPJ-Express](http://mpj-express.org/) or [mpiJava](https://sourceforge.net/projects/mpijava/).
+ A small utility to test the distributed features of the library that rely on MPI calls: [mpi-junit](https://github.com/handist/mpi-junit/)


## Compiling the project

We use Maven to compile and test the library. You can compile the library from source by checking it out with github and running command `mvn package`. This will create a java archive under folder `target/`. 

## Running the tests

The test code is located under the `src/test/java` directory. There are two kinds of tests for this project:

+ Normal Junit4 test (classes named `Test<class under test>.java`)
+ Test dealing with distributed features of the library that involve multiple hosts (classes named `IT_<class under test>.java`)

The former are bound to the `test` phase of the standard lifecycle of Maven. You can run them without any prerequisite using the `mvn test`. They will also be run when generating the Java ARchive with `mvn package`.

In order to run the distributed tests, you need to have either the `mpiJava` v1.2.7 or the MPJ "native" version of the library installed on your system. 
You can then run the distributed tests with the matching profile prepared for these two libraries with either command `mvn verify -Pmpijava` or `mvn verify -PmpjNative`. 

### with mpijava:

As a requirement, you will need to specify the location of the Shared Object libraries (`libmpijava.so` and potentially `libsavesignals.so`) generated during the compilation of the mpiJava library on your particular system. This is done by setting the environment variable `MPIJAVA_LIB` to wherever these files are located on your system. For instance:
```
user@computer:~/mpiJava/lib$ ls -l
total 108
drwxr-xr-x 3 user group  4096 Apr 15 02:25 classes
-rwxr-xr-x 1 user group 96872 Apr 14 19:37 libmpijava.so
-rwxr-xr-x 1 user group  8000 Apr 14 19:37 libsavesignals.so
user@computer:~/mpiJava/lib$ export MPIJVA_LIB=/home/user/mpiJava/lib
user@computer:~/mpiJava/lib$ cd ~/handistCollections
user@computer:~/handistCollections$ mvn verify -Pmpijava
...
```

### with MPJ:

The "mpjNative" profile relies on the usual declaration of the `MPJ_HOME` envirnment variable declaration. For more details, refer to the [compilation instructions of the MPJ library](http://mpj-express.org/)

# Related repository

This work was inspired by the distributed collections library of X10. You can check this project named "Cassia" there: [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github

# Authors
- Tomio Kamada
- Patrick Finnerty
- Yoshiki Kawanishi

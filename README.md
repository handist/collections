# handist collections library

A Java distributed collections library.

# Documentation / Information

| Version | |
| master branch | [Javadoc](master-latest/apidocs/index.html)<br>[Test Coverage Report](master-latest/jacoco/index.html) |

# Build instructions (MAVEN)

## Dependencies

This Java libraries relies on a number of libraries:

+ A slightly customized version of [APGAS for Java](https://github.com/x10-lang/apgas/tree/master/apgas) [located in this repository](https://github.com/handist/apgas)
+ A library providing Java bindings to MPI calls: either [MPJ-Express](http://mpj-express.org/) or [mpiJava](https://sourceforge.net/projects/mpijava/).

To ease the compilation process, we placed these two libraries which are usually downloaded from their project's website in a git repository so that they will be downloaded automatically like any normal maven dependency. We provide two compilation profiles for the library: `mpj` (default) and `mpijava` which allows you to switch the dependency to the MPJ-Express library or the mpiJava library respectively.

Whichever library/profile you choose does not change the JAR produced by running `mvn package` or `mvn package -Pmpijava`. You can very well compile the library with default profile (using the MPJ-Express library) and execute your programs using the mpiJava library. The implementation of the Java bindings used is determined by the classpath you provide when launching your program.

## Creating the JAR

You can compile the library from source by checking out the library and running `mvn package` of `mvn package -Pmpijava`. The JAR will be created under the `target` directory.

## Running the tests

The test code is located under the `src/test/java` directory. There are two kinds of tests for this project:

+ Normal Junit4 test (classes named `Test<class under test>.java`)
+ Test dealing with distributed features of the library that involve multiple hosts (classes named `IT_<class under test>.java`)

The former are bound to the `test` phase of the standard lifecycle of Maven. You can run them without any prerequisite using the `mvn test` command. They will also be run when generating the Java ARchive with `mvn package`.

The former are bound to the `verify` phase. **HOWEVER**, only the `mpijava` profile that relies on the mpiJava library can run these tests at the moment. Attempting to run these tests in the default profile will result in failure. You need to use the `mpijava` profile as such: `mvn verify -Pmpijava`.

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

# Related repository

This work was inspired by the distributed collections library of X10. You can check this project named "Cassia" there: [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github

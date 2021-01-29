A Java distributed collections library.

# Documentation / Information

| Version | |
|-|-|
| master branch | [Javadoc](https://handist.github.io/handistCollections/master-latest/apidocs/index.html)<br>[Test Coverage Report](https://handist.github.io/handistCollections/master-latest/jacoco/index.html) |

# Build instructions (MAVEN)

## Dependencies

This Java libraries relies on a number of libraries:

+ A slightly customized version of [APGAS for Java](https://github.com/x10-lang/apgas/tree/master/apgas) [located in this repository](https://github.com/handist/apgas)
+ A library providing Java bindings to MPI calls: either [MPJ-Express](http://mpj-express.org/) or [mpiJava](https://sourceforge.net/projects/mpijava/).

To ease the compilation process, we placed these two libraries which are usually downloaded from their project's website in a git repository so that they will be downloaded automatically like any normal maven dependency. We provide three compilation profiles for the library: 

+ `mpj` (default) to be used to compile the project on any computer
+ `mpjnative` to be used to compile and test the project with the MPJ-native mode
+ `mpijava` to be used to compile and test the project with the mpiJava native bindings

Whichever library/profile you choose does not change the JAR produced by running `mvn package` or `mvn package -Pmpijava`. You can very well compile the library with the default profile (using the MPJ-Express library) and execute your programs using the mpiJava library. The implementation of the Java bindings used is determined by the classpath you provide when launching your program.

## Creating the JAR

You can compile the library from source by checking out the library and running `mvn package` of `mvn package -Pmpijava`. The JAR will be created under the `target` directory.

## Running the tests

The test code is located under the `src/test/java` directory. There are two kinds of tests for this project:

+ Normal Junit4 test (classes named `Test<class under test>.java`)
+ Test dealing with distributed features of the library that involve multiple hosts (classes named `IT_<class under test>.java`)

The former are bound to the `test` phase of the standard lifecycle of Maven. You can run them without any prerequisite using the `mvn test` command. They will also be run when generating the Java ARchive with `mvn package`.

The former are bound to the `verify` phase. **HOWEVER**, only the `mpijava` or `mpjnative` profiles that rely on the mpiJava or MPJ-Express library can run these tests. Attempting to run these tests in the default profile will result in failure. You need to specify the `mpijava` (or `mpjnative`) profile as such: `mvn verify -Pmpijava`.

**Configuration for `mpijava` profile**

To running all the tests with the mpiJava library, you will need to specify the location of the Shared Object libraries (`libmpijava.so` and potentially `libsavesignals.so`) generated during the compilation of the mpiJava library on your particular system. This is done by setting the environment variable `MPIJAVA_LIB` to wherever these files are located on your system. For instance:

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

**Configuration for `mpjnative` profile**

In the case of the MPJ-Express library, you will need to set the `MPJ_HOME` environment variable at the location of the library on your system. We expect that the `libnativempjdev.so` compiled for your specific system is located in the `$MPJ_HOME/lib` directory. 

**Specifying a hostfile**

When running the distributed tests under profiles `mpijava` or `mpjnative`, you can specify a hostfile by adding `-Dhostfile=/path/to/hostfile` to your maven command. For instance: 

```
mvn -Pmpjnative -Dhostfile=hosts clean verify site
```

In addition, if you place a file named *HOSTFILE* in the root directory of the project, it will be used automatically without needing to specify any particular option.


# Related repository

This work was inspired by the distributed collections library of X10. You can check this project named "Cassia" there: [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github

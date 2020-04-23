# handist collections library

* cassia Distributed Collections @X10 -> handist Collections library @ Java


# directory structure

* **src/main/java**: main source files of the library
  * **handist.collections**: sequential/multi-threaded classes 
  * **handist.collections.dist**:  distributed collections

* **src/tests/java**: test routines using Junit

# Build instructions (MAVEN)

The configuration of the project is defined at the root of the project directory by the file `pom.xml`. 

## Dependencies

This project relies on several libraries:
* [APGAS for Java](https://github.com/x10-lang/apgas/tree/master/apgas) 
* [mpiJava v1.2.7](https://sourceforge.net/projects/mpijava/) to make native MPI calls from Java

These two libraries can have slight variations depending on the platform on which they need to run. Therefore, they should be compiled independantly prior to compiling this project. The Maven builder of this project expects the two Java archives (JAR) of these projects to be present under a certain directory on your system indicated by the environment variable ${APGAS_HOME}. You should therefore define the environment variable `APGAS_HOME` to reflect this location. For instance on a linux system: 

~~~
$ ls /home/user/apgaslibs
-rw-r--r-- 1 user group 7844811  3月 11 11:46 apgas.jar
-rw-r--r-- 1 user group   27154  3月 11 11:55 mpi.jar
$ export APGAS_HOME=/home/user/apgaslibs
~~~
Note: As an alternative to mpiJava, it may also be possible to use the MPJ project as it uses the same classes and method signatures to allow MPI calls from a Java program.

Other dependencies (`hazelcast`, `kryo`, and their respective dependencies) will be automatically downloaded by maven.

## Compiling, testing, and JAR creation

To compile the project from the command line, you can use the following commands:

| Command | Action performed |
| ------ | ------ |
| `mvn validate` | checks if the project configuration is correct. Use it to check if you have set the APGAS_HOME variable correctly |
| `mvn compile` | does the above, and compiles the source files to folder `target/classes` |
| `mvn test`| does all the above, compiles the test source files to folder `target/test-classes`, and runs the Junit tests. The result of the Junit tests can be found in the directory `target/surefire-reports` | 
| `mvn package` | does all the above, and packages the source files into a JAR: `target/collections-0.0.1-SNAPSHOT.jar` |
| `mvn clean` | Deletes the `target` folder which contains the compiled files, the test reports, and the JAR |

If you are using Eclipse, you should use maven to compile the project. You can create Maven run configurations with `validate`, `compile` etc as "Goals". To create a new run configuration, *select the project in the package explorer -> Right click -> Run As -> Maven build ...*. A pop-up window will appear and will let you choose your "goal". No other particular settings are needed at this point. 


# branch maintenance

* master@gittk is a release branch that will be also stored to handist/handist@github (use scash merge)
  * Developer version: develop@gittk 


# related repository

* [Java GLB](https://github.com/handist/JavaGLB)
  * developped by Patrick

* X10 version [cassia](https://gittk.cs.kobe-u.ac.jp/x10kobeu/cassia)@gittk, [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github

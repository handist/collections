# handist collections library 

| Branch | Status |
| --- | --- |
| develop | [![pipeline status](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/badges/develop/pipeline.svg)](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/-/commits/develop) [![coverage report](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/badges/develop/coverage.svg)](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/-/commits/develop) |
| master | [![pipeline status](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/badges/master/pipeline.svg)](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/-/commits/master) [![coverage report](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/badges/master/coverage.svg)](https://gittk.cs.kobe-u.ac.jp/x10kobeu/handistCollections/-/commits/master) |

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
* A portion of [APGAS for Java](https://github.com/x10-lang/apgas/tree/master/apgas) which is present in repository [github.com/handist/apgas](https://github.com/handist/apgas/)
* Either library [mpiJava v1.2.7](https://sourceforge.net/projects/mpijava/) or [MPJ Exress](http://mpj-express.org/) library to make native MPI calls from Java. 

At the moment, the MPJ Express library is included in the dependencies to compile the project. The source code is downloaded automatically from [handist/mpj](github.com/handist.mpj). Other dependencies (`hazelcast`, `kryo`, and their respective dependencies) will be automatically downloaded by maven.

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

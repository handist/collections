
# handist collections library

* cassia Distributed Collections @X10 -> handist Collections library @ Java


# directory structure

* **src/main/java**: main source files of the library
  * **handist.distcolls**: sequential/multi-threaded classes 
  * **handist.distcolls.dist**:  distributed collections

* **src/test/java**: test routines

# build instructions

This project relies on several libraries:
* [APGAS for Java](https://github.com/x10-lang/apgas/tree/master/apgas) 
* [mpiJava v1.2.7](https://sourceforge.net/projects/mpijava/) to make native MPI calls from Java
As an alternative to mpiJava, it is also possible to use MPJ for compilation as it uses the same classes and method signatures. However, 

These two libraries can have slight variations depending on the platform on which they need to run. Therefore, they should be compiled independantly prior to compiling this project. 
The Maven builder of this project expects two Java archives (JAR) to be present under a certain directoryi on your system indicated by the environment variable ${APGAS_HOME}. You should therefore define the environment variable `APGAS_HOME` to reflect this location. In addition, the *APGAS for Java* library relies on a number of other libraries. These also need to be present in the `APGAS_HOME` directory to launch the automated tests. 

For instance:

~~~
$ ls /home/user/apgaslibs
-rw-r--r-- 1 user group 7844811  3月 11 11:46 apgas.jar
-rw-r--r-- 1 user group 7195593 10月 25  2018 hazelcast.jar
-rw-r--r-- 1 user group  685232  3月 11 11:45 javaglb.jar
-rw-r--r-- 1 user group  285211 10月 25  2018 kryo.jar
-rw-r--r-- 1 user group    5711 10月 25  2018 minlog.jar
-rw-r--r-- 1 user group   27154  3月 11 11:55 mpi.jar
-rw-r--r-- 1 user group   41755 10月 25  2018 objenesis.jar
-rw-r--r-- 1 user group   74282 10月 25  2018 reflectasm.jar
$ export APGAS_HOME=/home/user/apgaslibs
~~~

The standard Maven targets apply: *clean compile test package*. To compile the project, run the command `mvn compile` 

~~~
$ mvn compile
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building distcolls 0.0.1-SNAPSHOT
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-resources-plugin:3.0.2:resources (default-resources) @ distcolls ---
[INFO] Using 'UTF-8' encoding to copy filtered resources.
[INFO] Copying 2 resources
[INFO]
[INFO] --- maven-compiler-plugin:3.8.0:compile (default-compile) @ distcolls ---
[INFO] Changes detected - recompiling the module!
[INFO] Compiling 31 source files to /home/user/handistCollections/target/classes
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 7.098 s
[INFO] Finished at: 2020-04-13T13:48:16+09:00
[INFO] Final Memory: 24M/1540M
[INFO] ------------------------------------------------------------------------

~~~

# branch maintenance

* master@gittk is a release branch that will be also stored to handist/handist@github (use scash merge)
  * Developer version: develop@gittk 


# related repository

* [Java GLB](https://github.com/handist/JavaGLB)
  * developped by Patrick

* X10 version [cassia](https://gittk.cs.kobe-u.ac.jp/x10kobeu/cassia)@gittk, [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github


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



* Gradle or some build systems are used to setup APGAS Library or something


# branch maintenance

* master@gittk is a release branch that will be also stored to handist/handist@github (use scash merge)
  * Developer version: develop@gittk 


# related repository

* [Java GLB](https://github.com/handist/JavaGLB)
  * developped by Patrick

* X10 version [cassia](https://gittk.cs.kobe-u.ac.jp/x10kobeu/cassia)@gittk, [cassiaX10lib](https://github.com/handist/cassiaX10lib)@github

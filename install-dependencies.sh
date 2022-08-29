#!/bin/bash


################################################################################
# Function used to check if a Maven artifact is installed in the local         #
# repository. If not, then the project is cloned in the home directory and     #
# installed                                                                    #
################################################################################
function checkArtifact {
    GROUP=$1
    ARTIFACT=$2
    VERSION=$3
    GITREPO=$4

    # Go to User home directory
    cd ~

    # Check if dependency present
    PRESENT=`mvn dependency:get -Dartifact=$GROUP:$ARTIFACT:$VERSION -o -DremoteRepositories=file:/${M2REPO} -q | grep -c " required artifact is missing."`

    if [ $PRESENT == 0 ]
    then
        echo "- Dependency $GROUP:$ARTIFACT:$VERSION already installed"
    else
        echo "- Dependency $GROUP:$ARTIFACT:$VERSION missing"
        echo "-- Downloading from $GITREPO into ~/${ARTIFACT}-${VERSION}"
        git clone $GITREPO $ARTIFACT-$VERSION
        cd $ARTIFACT-$VERSION

        # `git checkout $VERSION` assumes the presence of a tag that matches the artifact version
        git checkout $VERSION
        mvn install -DskipTests
        cd ..
    fi

    # Return to current project directory
    cd $PROJECT_DIR
}

################################################################################
# Main script                                                                  #
################################################################################

# Check that environment variable OPENMPI_LIB is defined
echo "Checking if OPENMPI_LIB environment variable is defined ..."
if [ ${OPENMPI_LIB} ];
then
    echo "- OPENMPI_LIB: ${OPENMPI_LIB}"
else
    echo "- OPENMPI_LIB not defined, exiting"
    exit 1
fi

# Check that ${OPENMPI_LIB}/mpi.jar is present
echo "Checking that ${OPENMPI_LIB}/mpi.jar is present ..."
if [ -f ${OPENMPI_LIB}/mpi.jar ]
then
    echo "- OPENMPI_LIB/mpi.jar found"
else
    echo "- OPENMPI_LIB/mpi.jar not found. Compile OpenMPI with the Java bindings and re-launch this script. Exiting ..."
    exit 1
fi

# Check if maven is installed
echo "Checking if Maven is installed ..."
if hash mvn &>/dev/null
then
    echo "- Maven is installed"
    mvn --version
else
    echo "- Maven not installed, install Maven and re-run this script. Exiting ..."
    exit 1
fi

# Obtain location of local repository
M2REPO=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
echo "Local Repository located at: ${M2REPO}"

# Invoking a dummy mvn dependency:get without the offline "-o" option to force the installation of this plugin if not previously isntalled
mvn dependency:get -Dartifact=com.github.handist:mpi-junit:v1.2.3 -DremoteRepositories=file:/${M2REPO} -q >/dev/null 2>&1

# Check if the dependencies that are not available on Maven Central are already
# installed in the local Maven repository. If not, clone and install them.
checkArtifact com.github.handist mpi-junit v1.2.3 https://github.com/handist/mpi-junit.git
checkArtifact com.github.handist apgas v2.0.0 https://github.com/handist/apgas.git


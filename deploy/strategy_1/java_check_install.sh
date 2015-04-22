start_dir=$(pwd)

if [ $# -gt 0 ]; then
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

if type -p java; then
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    _java="$JAVA_HOME/bin/java"
else
    #No Java
    _java=""
fi

if [[ "$_java" ]]; then
    version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}')
    echo version "$version"
    if [[ "$version" > "1.6" ]]; then
        echo java version is good
    else
        echo version is less than 1.7
        _java=""
    fi
fi

if [ -z "$_java" ]; then
        mkdir java
        cd java
        wget --no-check-certificate --no-cookies --header "Cookie: oraclelicense=accept-securebackup-cookie" http://download.oracle.com/otn-pub/java/jdk/8u40-b26/jdk-8u40-linux-x64.tar.gz
        tar -xzf jdk-8u40-linux-x64.tar.gz
        cd jdk1.8.0_40
        export JAVA_HOME=$(pwd) 
fi

cd $start_dir

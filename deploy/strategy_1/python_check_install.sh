start_dir=$(pwd)

if [ $# -gt 0 ]; then
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

if type -p python; then
    _python=python
fi

if [[ "$_python" ]]; then
    version=$("$_python" --version 2>&1 | awk  '{print $2}')
    echo version "$version"
    if [[ "$version" > "2.6" ]]; then
        echo python version is good
    else
        echo version is less than 2.6
        _python=""
    fi
fi

if [ -z "$_python" ]; then
    if type -p yum; then
        yum -y update
        yum -y install python
    elif type -p apt-get; then
        apt-get -y update
        apt-get -y install python
    else
        echo "[Error] Unable to install latest version of python."
    fi
fi

cd $start_dir
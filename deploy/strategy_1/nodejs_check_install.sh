if [ $# -gt 0 ]
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

if type -p nodejs; then
    if type -p yum; then
        yum -y update
        yum -y groupinstall "Development Tools"
        curl -sL https://rpm.nodesource.com/setup | bash -
        yum install -y nodejs
        npm install -g express
    elif type -p apt-get; then
        apt-get -y update
        apt-get -y install nodejs
        apt-get install -y build-essential
        npm install -g express
    else
        echo "[Error] Unable to install nodejs."
    fi
fi

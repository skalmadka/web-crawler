if [ $# -gt 0 ]
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

wget https://github.com/apache/storm/archive/v0.9.4.tar.gz
tar -xzf v0.9.4.tar.gz
cd storm-0.9.4
start_dir=$(pwd)

if [ $# -gt 0 ]; then
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

wget http://apache.mirrors.pair.com/storm/apache-storm-0.9.4/apache-storm-0.9.4.tar.gz
tar -xzf apache-storm-0.9.4.tar.gz
rm apache-storm-0.9.4.tar.gz
#cd storm-0.9.4

cd $start_dir
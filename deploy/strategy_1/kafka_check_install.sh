start_dir=$(pwd)

if [ $# -gt 0 ]; then
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

wget http://apache.mirrors.pair.com/kafka/0.8.2.0/kafka_2.10-0.8.2.0.tgz
tar -xzf kafka_2.10-0.8.2.0.tgz
rm kafka_2.10-0.8.2.0.tgz

cd $start_dir
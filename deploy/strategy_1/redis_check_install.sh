start_dir=$(pwd)

if [ $# -gt 0 ]; then
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

wget http://download.redis.io/releases/redis-3.0.0.tar.gz
tar xzf redis-3.0.0.tar.gz
cd redis-3.0.0
make

cd $start_dir

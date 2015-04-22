start_dir=$(pwd)

if [ $# -gt 0 ]; then
    cd $1
else
    echo "[ERROR] Usage: $0 <Working directory>"
    return
fi

wget https://download.elastic.co/elasticsearch/elasticsearch/elasticsearch-1.5.1.tar.gz
tar -xzf elasticsearch-1.5.1.tar.gz
rm elasticsearch-1.5.1.tar.gz

cd $start_dir
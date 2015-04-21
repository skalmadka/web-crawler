package storm.crawler.filter.bloomfilter;


import storm.crawler.common.MurmurHash;
import redis.clients.jedis.Jedis;

/**
 * Created by Sunil Kalmadka on 3/10/2015.
 */

public class RedisBloomFilter<T> {
    protected final int m;     //Number of bits in BloomFilter
    protected final short k;    //Number of hash functions
    protected Jedis jedis;
    public String bloomFilterName;

    public RedisBloomFilter(final int expectedCount, final double desiredFalsePositiveRate,
                            final String redisHost, final short redisPort,
                            final String bloomFilterName){
        final Double estimatedBitArraySize = estimateOptimalBitArraySize(expectedCount, desiredFalsePositiveRate);

        if(estimatedBitArraySize > Integer.MAX_VALUE){
            throw new ArrayStoreException("Number of bits required is too large");
        }

        m = estimatedBitArraySize.intValue();
        k = estimateOptimalHashCount(expectedCount, m);

        //Connect Redis
        jedis = new Jedis(redisHost, redisPort);
        this.bloomFilterName = bloomFilterName;
    }

    public void expire(int seconds){
        jedis.expire(bloomFilterName, seconds);
    }

    public void add(final T obj){
        final byte[] bytes = obj.toString().getBytes();
        final int hash1 = MurmurHash.hash32(bytes, bytes.length, 0);
        final int hash2 = MurmurHash.hash32(bytes, bytes.length, hash1);

        int hashBit;
        for(int i=0;i<k;i++){
            hashBit = Math.abs((hash1 + i*hash2)%m);
            //bitSet.set(hashBit);
            jedis.setbit(bloomFilterName, hashBit, String.valueOf(1));
        }
    }

    public boolean exists(final T obj){
        final byte[] bytes = obj.toString().getBytes();
        final int hash1 = MurmurHash.hash32(bytes, bytes.length, 0);
        final int hash2 = MurmurHash.hash32(bytes, bytes.length, hash1);

        int hashBit;
        for(int i=0;i<k;i++){
            hashBit = Math.abs((hash1 + i*hash2)%m);
            if(!jedis.getbit(bloomFilterName, hashBit)){
                return false;
            }
        }
        return true;
    }

    public void clearFilter() {
        jedis.set(bloomFilterName, "0x000000");
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if(jedis != null) {
            jedis.close();
        }
    }

    protected Double estimateOptimalBitArraySize(int expectedCount, double desiredFalsePositiveRate){
        return Math.ceil( -1 * expectedCount * Math.log(desiredFalsePositiveRate)
                        /((Math.log(2)) * (Math.log(2)))
        );
    }

    protected short estimateOptimalHashCount(int expectedCount, int bitArraySize){
        return  (short) ((bitArraySize/expectedCount) * Math.log(2));
    }

}

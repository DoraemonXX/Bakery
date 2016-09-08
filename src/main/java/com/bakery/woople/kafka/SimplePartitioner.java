package com.bakery.woople.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {

    public SimplePartitioner (VerifiableProperties props) {
    }

    public int partition(Object key, int numPartitions) {
        int partition = 0;
        String k = (String)key;
        partition = Math.abs(k.hashCode()) % numPartitions;
        return partition;
    }

}

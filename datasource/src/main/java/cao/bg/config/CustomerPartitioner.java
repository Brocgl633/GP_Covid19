package cao.bg.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @Author : CGL
 * @Date : 2022 2022/1/4 20:28
 * @Desc : 自定义分区器指定分区规则(默认是按照key的hash)
 */
public class CustomerPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer k  = (Integer)key;
        Integer num = cluster.partitionCountForTopic(topic);        // 获取主题的分区数
        int partiton = k % num;
        return partiton;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}

//package com.ayuan.etldemo.low;
//
//import kafka.cluster.BrokerEndPoint;
//import org.apache.kafka.common.requests.FetchRequest;
//import org.apache.kafka.common.requests.FetchResponse;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//
//public class LowerConsumer {
//    //保存offset
//    private long offset;
//    //保存分区副本
//    private Map<Integer, List<BrokerEndPoint>> partitionsMap = new HashMap<Integer, List<BrokerEndPoint>>(1024);
//
//    public static void main(String[] args) throws InterruptedException {
//        List<String> brokers = Arrays.asList("k8s-n1", "k8s-n2","k8s-n3");
//        int port = 9092;
//        int partition = 1;
//        long offset=2;
//        LowerConsumer lowerConsumer = new LowerConsumer();
//        while(true){
////            offset = lowerConsumer.getOffset();
//            lowerConsumer.getData(brokers,port,"mytest",partition,offset);
//            TimeUnit.SECONDS.sleep(1);
//        }
//
//    }
//private BrokerEndPoint findLeader(Collection<String> brokers,int port,String topic,int partition){
//        for (String broker : brokers) {
//        //创建消费者对象操作每一台服务器
//        SimpleConsumer getLeader = new SimpleConsumer(broker, port, 10000, 1024 * 24, "getLeader");
//        //构造元数据请求
//        TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
//        //发送元数据请求
//        TopicMetadataResponse response = getLeader.send(topicMetadataRequest);
//        //解析元数据
//        List<TopicMetadata> topicMetadatas = response.topicsMetadata();
//        //遍历数据
//        for (TopicMetadata topicMetadata : topicMetadatas) {
//        //获取分区元数据信息
//        List<PartitionMetadata> partitionMetadatas = topicMetadata.partitionsMetadata();
//        //遍历分区元数据
//        for (PartitionMetadata partitionMetadata : partitionMetadatas) {
//        if(partition == partitionMetadata.partitionId()){
//        //保存，分区对应的副本，如果需要主副本挂掉重新获取leader只需要遍历这个缓存即可
//        List<BrokerEndPoint> isr = partitionMetadata.isr();
//        this.partitionsMap.put(partition,isr);
//        return partitionMetadata.leader();
//        }
//        }
//        }
//        }
//        return null;
//        }
//
//    private void getData(List<String> brokers, int port, String mytest, int partition, long offset) {
//        //获取leader
//        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
//        if(leader==null) return;
//        String host = leader.host();
//        //获取数据的消费者对象
//        SimpleConsumer getData = new SimpleConsumer(host, port, 10000, 1024 * 10, "getData");
//        //构造获取数据request 这里一次可以添加多个topic addFecth 添加即可
//        FetchRequest fetchRequestBuilder = new FetchRequestBuilder().addFetch(topic, partition, offset, 1024 * 10).build();
//        //发送获取数据请求
//        FetchResponse fetchResponse = getData.fetch(fetchRequestBuilder);
//        //解析元数据返回，这是message的一个set集合
//        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
//        //遍历消息集合
//        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
//            long offset1 = messageAndOffset.offset();
//            this.setOffset(offset);
//            ByteBuffer payload = messageAndOffset.message().payload();
//            byte[] buffer = new byte[payload.limit()];
//            payload.get(buffer);
//            String message = new String(buffer);
//            System.out.println("offset:"+ offset1 +"--message:"+ message);
//
//        }
//    }
//    public long getOffset() {
//        return offset;
//    }
//}

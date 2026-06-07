package main.java.com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;


//requestTypes

public class Protocol {

    //request types
    public static final byte PRODUCE = 0x0;
    public static final byte FETCH = 0x1;
    public static final byte METADATA = 0x2;
    public static final byte CREATE_TOPIC = 0x3;
    public static final byte REPLICATE = 0x4;
    public static final byte TOPIC_NOTIFICATION = 0x5;

    //repsonse types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;
    public static final byte ERROR_RESPONSE = 0x14;

    //nested class
    public static class ProduceResult{
        public final boolean success;
        public final long offset;
        public final String errorMessage;

        public ProduceResult(boolean success, long offset, String errorMessage) {
            this.success = success;
            this.offset = offset;
            this.errorMessage = errorMessage;
        }
    }
    public static class FetchResult {
        public final boolean success;
        public final String[] messages;
        public final String errorMessage;

        public FetchResult(boolean success, String[] messages, String errorMessage){
            this.success = success;
            this.messages = messages;
            this.errorMessage = errorMessage;
        }
    }

    public static class MetadataResult {
        public final ArrayList<BrokerInfo> brokers;
        public final ArrayList<TopicMetadata> topics;
        public final String errorMessage;

        public MetadataResult(ArrayList<BrokerInfo> brokers, ArrayList<TopicMetadata> topics, String error){
            this.brokers = brokers;
            this.topics = topics;
            this.errorMessage = error;
        }
    }

    public static class TopicMetadata{
        public final String topic;
        public final ArrayList<PartitionMetadata> partitions;

        public TopicMetadata(String topic, ArrayList<PartitionMetadata> partitions){
            this.topic = topic;
            this.partitions = partitions;

        }
    }

    public static class PartitionMetadata {
        public final int partitionId;
        public final int leaderId; // Leader is the one replica that is "in charge." All reads and writes go through the leader only. The other replicas just follow along and copy what the leader has.
        public final int[] replicaIds; //Replicas are just copies of the same data stored on multiple brokers for safety. 

        public PartitionMetadata(int partitionId, int leaderId, int[] replicaIds) {
            this.partitionId = partitionId;
            this.leaderId = leaderId;
            this.replicaIds = replicaIds;
        }
    }

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message){
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 +  topic.length() + 4 + 4 + message.length);
        buffer.put(PRODUCE);
        buffer.putInt(topic.length());
        byte[] topicArray = topic.getBytes(StandardCharsets.UTF_8);
        buffer.put(topicArray);
        buffer.putInt(partition);
        buffer.putInt(message.length);
        buffer.put(message);
        return buffer;
    }

    public static ByteBuffer encodeFetchRequest(String topic, int partition, long offset, int maxBytes){
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4+ topic.length() + 4 + 8+4);
        buffer.put(FETCH);
        buffer.putInt(topic.length());
        byte[] topicArray = topic.getBytes(StandardCharsets.UTF_8);
        buffer.put(topicArray);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(maxBytes);
        return buffer;
    }
    public static ByteBuffer encodeMetadataRequest(){
        ByteBuffer buffer = ByteBuffer.allocate(1);
        buffer.put(METADATA);
        return buffer;
    }

    public static ByteBuffer encodeCreateTopicBuffer(String topic, int numPartitions, short replicationFactor){

        //replication factor = how many copies of the data we want to keep across the cluster, short is used because replication factor is usually small and it saves space compared to using an int.
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + topic.length() + 4 + 2);
        buffer.put(CREATE_TOPIC);
        buffer.putInt(topic.length());
        byte[] topicArray = topic.getBytes(StandardCharsets.UTF_8);
        buffer.put(topicArray);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        return buffer;
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer){
        byte responseType = buffer.get();
        if(responseType != PRODUCE_RESPONSE){
            String errorMessage = "Invalid request type: " + responseType;
            return new ProduceResult(false, -1, errorMessage);
        }
        boolean success = buffer.get() == 1;
        long offset = buffer.getLong();
        String errorMessage = null;
        if(!success){
            int errorLength = buffer.getInt();
            byte[] errorBytes = new byte[errorLength];
            buffer.get(errorBytes);
            errorMessage = new String(errorBytes, StandardCharsets.UTF_8);
        }
        return new ProduceResult(success, offset, errorMessage);
    }

    public static FetchResult decodeFetchResponse(ByteBuffer buffer){
        byte responseType = buffer.get();
         if(responseType != FETCH_RESPONSE){
             String errorMessage = "Invalid request type: " + responseType;
             return new FetchResult(false, null, errorMessage);
         }
        boolean success = buffer.get() == 1;
        String errorMessage = null;
        String[] messages = null;
        if(success){
            int messageCount = buffer.getInt();
            messages = new String[messageCount];
            for(int i = 0; i < messageCount; i++){
                long offset = buffer.getLong();    // each message's offset
                int messageLength = buffer.getInt();
                byte[] messageBytes = new byte[messageLength];
                buffer.get(messageBytes);
                messages[i] = new String(messageBytes, StandardCharsets.UTF_8);
            }
        } else{
            int errorLength = buffer.getInt();
            byte[] errorBytes = new byte[errorLength];
            buffer.get(errorBytes);
            errorMessage = new String(errorBytes, StandardCharsets.UTF_8);
        }
        return new FetchResult(success, messages, errorMessage);
    }

    public static MetadataResult decodeMetadataResponse(ByteBuffer buffer){    
        byte responseType = buffer.get();
        if(responseType != METADATA){
             String errorMessage = "Invalid request type: " + responseType;
             return new MetadataResult(null, null, errorMessage);
        }
        boolean success = buffer.get() == 1;
        String errorMessage = null;
        if(!success){
            int errorLength = buffer.getInt();
            byte[] errorBytes = new byte[errorLength];
            buffer.get(errorBytes);
            errorMessage = new String(errorBytes,StandardCharsets.UTF_8);
            return new MetadataResult(null, null, errorMessage);
        }
        else{
            int numBrokers = buffer.getInt();
            ArrayList<BrokerInfo> brokers = new ArrayList<BrokerInfo>();
            for(int i=0;i<numBrokers;i++){
                int brokerId = buffer.getInt();
                int hostLength  =  buffer.getInt();
                byte[] hostBytes = new byte[hostLength];
                buffer.get(hostBytes);
                String host = new String(hostBytes, StandardCharsets.UTF_8);
                int  port = buffer.getInt();
                BrokerInfo newBroker = new BrokerInfo(brokerId,host,port);
                brokers.add(newBroker);
            }
            int numTopics = buffer.getInt();
            ArrayList<TopicMetadata> topics = new ArrayList<TopicMetadata>();
            for(int i=0;i<numTopics;i++){
                int topicLength = buffer.getInt();
                byte[] topicBytes = new byte[topicLength];
                buffer.get(topicBytes);
                String topic = new String(topicBytes, StandardCharsets.UTF_8);
                int numPartitions = buffer.getInt();
                ArrayList<PartitionMetadata> partitions = new ArrayList<PartitionMetadata>();
                for(int j=0;j<numPartitions;j++){
                    int partitionId = buffer.getInt();
                    int leaderId = buffer.getInt();
                    int replicaCount = buffer.getInt();
                    int[] replicaIds = new int[replicaCount];
                    for(int k=0;k<replicaCount;k++){
                        replicaIds[k] = buffer.getInt();
                    }
                    PartitionMetadata partitionMetadata = new PartitionMetadata(partitionId, leaderId, replicaIds);
                    partitions.add(partitionMetadata);
                }
                TopicMetadata topicMetadata = new TopicMetadata(topic, partitions);
                topics.add(topicMetadata);
            }
            return new MetadataResult(brokers, topics, errorMessage);
        }
       
    }

    //broker to broker communication methods
    public static ByteBuffer encodeReplicationRequest(String topic, int partition, long offset, byte[] message){
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 +  topic.length() + 4 + 8+ 4 + message.length);
        buffer.put(REPLICATE);
        buffer.putInt(topic.length());
        byte[] topicArray = topic.getBytes(StandardCharsets.UTF_8);
        buffer.put(topicArray);
        buffer.putInt(partition);
        buffer.putLong(offset);
        buffer.putInt(message.length);
        buffer.put(message);
        return buffer;
    }

    public static ByteBuffer encodeTopicNotification(String topic){
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 +  topic.length());
        buffer.put(TOPIC_NOTIFICATION);
        buffer.putInt(topic.length());
        byte[] topicArray = topic.getBytes(StandardCharsets.UTF_8);
        buffer.put(topicArray);
        return buffer; // use metdatarequest if need more information about the topic

    }

    //error handling
    public static void sendErrorResponse(SocketChannel channel, String errorMessage){

    }

}

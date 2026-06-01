package main.java.com.simplekafka.broker;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

//requestTypes

public class Protocol {

    //request types
    public static final byte PRODUCE = 0x0;
    public static final byte FETCH = 0x1;
    public static final byte METADATA = 0x2;
    public static final byte CREATE_TOPIC = 0x3;

    //repsonse types
    public static final byte PRODUCE_RESPONSE = 0x11;
    public static final byte FETCH_RESPONSE = 0x12;

    private String id;

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

    public Protocol(){
        UUID uuid = UUID.randomUUID();
        this.id = uuid.toString();
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
        public final int brokerId;
        public final String topic;
        public final String errorMessage;

        public MetadataResult(int brokerId, String topic, String errorMessage) {
            this.brokerId = brokerId;
            this.topic = topic;
            this.errorMessage = errorMessage;
        }
    }

    public static ByteBuffer encodeProduceRequest(String topic, int partition, byte[] message){
        ByteBuffer buffer = ByteBuffer.allocate(1 + topic.length() + 4 + 4 + message.length);
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
        ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + topic.length() + 4 + 2);
        buffer.put(CREATE_TOPIC);
        buffer.putInt(topic.length());
        byte[] topicArray = topic.getBytes(StandardCharsets.UTF_8);
        buffer.put(topicArray);
        buffer.putInt(numPartitions);
        buffer.putShort(replicationFactor);
        return buffer;
    }

    public static ProduceResult decodeProduceResponse(ByteBuffer buffer){
        byte requestType = buffer.get();
        if(requestType != PRODUCE){
            String errorMessage = "Invalid request type: " + requestType;
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

}

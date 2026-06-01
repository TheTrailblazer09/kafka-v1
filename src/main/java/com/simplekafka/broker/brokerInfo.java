package main.java.com.simplekafka.broker;

//keeps information about each broker in the cluster
public class brokerInfo{
    private int brokerId;
    private String host;
    private int port;

    //constrctor
    public brokerInfo(int brokerId, String host, int port){
        this.brokerId = brokerId;
        this.host = host;
        this.port = port;
    }

    public int getBrokerId(){
        return this.brokerId;
    }

    public String getHost(){
        return this.host;
    }

    public int getPort(){
        return this.port;
    }

    
}

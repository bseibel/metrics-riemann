package ca.seibelnet.riemann;

import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannClient;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;

public class Riemann implements Closeable {

    String riemannHost;
    Integer riemannPort;
    String localHostname;

    RiemannClient client;

    Float defaultTtl = 10.0f;

    public Riemann(String host, Integer port) throws IOException {
        this.riemannHost = host;
        this.riemannPort = port;
        this.localHostname = InetAddress.getLocalHost().getHostName();
        this.client = RiemannClient.tcp(riemannHost, riemannPort);
    }

    public void connect() throws IOException {
        if (!client.isConnected()) {
            client.connect();
        }
    }

    public void setDefaultTtl(Float ttl) {
        this.defaultTtl = ttl;
    }

    public EventDSL event(String service) throws IOException {
        if (client.isConnected()) {
            return client.event().service(service).host(localHostname).ttl(defaultTtl);
        } else throw new IOException("Client not connected.");
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.disconnect();
        }

    }

}

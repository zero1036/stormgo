package config;

import com.rabbitmq.client.ConnectionFactory;
import io.latent.storm.rabbitmq.config.ConfigUtils;
import io.latent.storm.rabbitmq.config.ConnectionConfig;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Thinkpads on 2018/3/15.
 */
public class ExampleConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private String host;
    private int port;

    public static ExampleConfig forTest() {
        return new ExampleConfig("localhost", 8888);
    }

    public ExampleConfig(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static long getSerialVersionUID() {
        return serialVersionUID;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public static final String HOST_NAME = "example.host";
    public static final String PORT_NAME = "example.port";

    public static ExampleConfig getFromStormConfig(Map<String, Object> stormConfig) {
        return new ExampleConfig(
                stormConfig.containsKey(HOST_NAME) ? ConfigUtils.getFromMap(HOST_NAME, stormConfig) : "",
                stormConfig.containsKey(PORT_NAME) ? Integer.parseInt(ConfigUtils.getFromMap(PORT_NAME, stormConfig)) : 0);

    }

    public Map<String, Object> asMap() {
        HashMap map = new HashMap();

        map.put(HOST_NAME, this.host);
        map.put(PORT_NAME, this.port);

        return map;
    }
}

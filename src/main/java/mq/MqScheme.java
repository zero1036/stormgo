package mq;

import com.google.common.collect.Lists;
import io.latent.storm.rabbitmq.MessageScheme;
import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * Created by Thinkpads on 2018/3/12.
 */
public class MqScheme implements Scheme {
    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        List result = Lists.newArrayListWithCapacity(1);
        result.add(byteBufferToString(byteBuffer));
        return result;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("testMqMessage");
    }

    public static String byteBufferToString(ByteBuffer buffer) {
        CharBuffer charBuffer = null;
        try {
            Charset charset = Charset.forName("UTF-8");
            CharsetDecoder decoder = charset.newDecoder();
            charBuffer = decoder.decode(buffer);
            buffer.flip();
            return charBuffer.toString();
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }
    }
}

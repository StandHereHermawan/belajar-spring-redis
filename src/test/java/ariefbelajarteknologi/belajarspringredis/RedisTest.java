package ariefbelajarteknologi.belajarspringredis;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.geo.*;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
public class RedisTest {

    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void redisTemplate() {
        assertNotNull(redisTemplate);
    }

    @Test
    void string() throws InterruptedException {
        ValueOperations<String, String> operations = redisTemplate.opsForValue();

        operations.set("name", "Arief", Duration.ofSeconds(2));
        assertEquals("Arief", operations.get("name"));

        Thread.sleep(Duration.ofSeconds(3));
        assertNull(operations.get("name"));
    }

    @Test
    void list() {
        ListOperations<String, String> operations = redisTemplate.opsForList();

        operations.rightPush("names", "Arief");
        operations.rightPush("names", "Karditya");
        operations.rightPush("names", "Hermawan");

        assertEquals("Arief", operations.leftPop("names"));
        assertEquals("Karditya", operations.leftPop("names"));
        assertEquals("Hermawan", operations.leftPop("names"));
    }

    @Test
    void set() {
        SetOperations<String, String> operations = redisTemplate.opsForSet();

        operations.add("students", "Erlang");
        operations.add("students", "Erlang");
        operations.add("students", "Anggara");
        operations.add("students", "Anggara");
        operations.add("students", "Widjaksono");
        operations.add("students", "Widjaksono");

        Set<String> students = operations.members("students");
        assertEquals(3, students.size());
        assertThat(students, hasItems("Erlang", "Anggara", "Widjaksono"));
    }

    @Test
    void zSet() {
        ZSetOperations<String, String> operations = redisTemplate.opsForZSet();

        operations.add("score", "Arief", 49);
        operations.add("score", "Erlang", 62);
        operations.add("score", "Parhan", 61);

        assertEquals("Erlang", operations.popMax("score").getValue());
        assertEquals("Parhan", operations.popMax("score").getValue());
        assertEquals("Arief", operations.popMax("score").getValue());
    }

    @Test
    void hashOpsWithManualDeclaration() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();

        operations.put("user:1", "id", "1");
        operations.put("user:1", "name", "Arief");
        operations.put("user:1", "email", "arief@example.com");

        assertEquals("1", operations.get("user:1", "id"));
        assertEquals("Arief", operations.get("user:1", "name"));
        assertEquals("arief@example.com", operations.get("user:1", "email"));

        redisTemplate.delete("user:1");
    }

    @Test
    void hashOpsWithMapObject() {
        HashOperations<String, Object, Object> operations = redisTemplate.opsForHash();

        HashMap<Object, Object> mapData = new HashMap<>();
        mapData.put("id", "1");
        mapData.put("name", "Arief");
        mapData.put("email", "arief@example.com");

        operations.putAll("user:1", mapData);

        assertEquals("1", operations.get("user:1", "id"));
        assertEquals("Arief", operations.get("user:1", "name"));
        assertEquals("arief@example.com", operations.get("user:1", "email"));
    }

    @Test
    @Disabled
    void geo() {
        GeoOperations<String, String> operations = redisTemplate.opsForGeo();

        operations.add("commerce", new Point(107.62881816326308, -6.924672030896318), "OYO 2625");
        operations.add("commerce", new Point(107.62794636185166, -6.924843575789419), "Mie Gacoan Gatsu");
        operations.add("commerce", new Point(107.62348048387767, -6.923325866955385), "Hotel Papandayan");

        Distance distance = operations.distance("commerce", "OYO 2625", "Hotel Papandayan", Metrics.KILOMETERS);
        assertEquals(new Distance(0.6081, Metrics.KILOMETERS), distance);

        GeoResults<RedisGeoCommands.GeoLocation<String>> commerce = operations.search("commerce", new Circle(
                new Point(107.6271439064675, -6.924359336921793),
                new Distance(0.3, Metrics.KILOMETERS)
        ));

        assertEquals(2, commerce.getContent().size());
        assertEquals("Mie Gacoan Gatsu", commerce.getContent().get(0).getContent().getName());
        assertEquals("OYO 2625", commerce.getContent().get(1).getContent().getName());
    }

    @Test
    void hyperLogLog() {
        HyperLogLogOperations<String, String> operations = redisTemplate.opsForHyperLogLog();

        operations.add("traffics", "Arief", "Erlang", "Indra");
        operations.add("traffics", "Erlang", "Indra", "Adit");
        operations.add("traffics", "Rizky", "Miftahul", "Atqia");

        assertEquals(7L, operations.size("traffics"));
    }

    @Test
    void transaction() {
        redisTemplate.execute(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.multi();

                operations.opsForValue().set("member1", "Arief", Duration.ofSeconds(3));
                operations.opsForValue().set("member2", "Gema", Duration.ofSeconds(3));

                operations.exec();
                return null;
            }
        });

        assertEquals("Arief", redisTemplate.opsForValue().get("member1"));
        assertEquals("Gema", redisTemplate.opsForValue().get("member2"));
    }

    @Test
    void pipeline() {
        List<Object> statuses = redisTemplate.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                operations.opsForValue().set("test1", "Arief", Duration.ofSeconds(3));
                operations.opsForValue().set("test2", "Arief", Duration.ofSeconds(3));
                operations.opsForValue().set("test3", "Arief", Duration.ofSeconds(3));
                operations.opsForValue().set("test4", "Arief", Duration.ofSeconds(3));
                operations.opsForValue().set("test5", "Arief", Duration.ofSeconds(3));
                return null;
            }
        });

        assertThat(statuses, hasSize(5));
        assertThat(statuses, hasItem(true));
        assertThat(statuses, not(hasItem(false)));
    }

    @Test
    void publishStream() {
        StreamOperations<String, Object, Object> operations = redisTemplate.opsForStream();

        MapRecord<String, String, String> record = MapRecord.create("stream-1", Map.of(
                "name", "Hilmi Akbar Muharrom",
                "address", "Bandung"
        ));

        for (int i = 0; i < 10; i++) {
            operations.add(record);
        }
    }

    @Test
    void subscribeStream() {
        StreamOperations<String, Object, Object> operations = redisTemplate.opsForStream();

        try {
            operations.createGroup("stream-1", "sample-group");
        } catch (RedisSystemException exception) {
            // group already exists
        }

        List<MapRecord<String, Object, Object>> records = operations.read(Consumer.from("sample-group", "sample-1"),
                StreamOffset.create("stream-1", ReadOffset.from(String.valueOf(2))));

        for (MapRecord<String, Object, Object> record : records) {
            System.out.println(record);
        }
    }

    @Test
    void pubSub() {
        redisTemplate.getConnectionFactory().getConnection().subscribe(new MessageListener() {
            @Override
            public void onMessage(Message message, byte[] pattern) {
                String event = new String(message.getBody());
                System.out.println("Receive message : " + event);
            }
        }, "my-channel".getBytes());

        for (int i = 0; i < 10; i++) {
            redisTemplate.convertAndSend("my-channel", "Hello World : " + i);
        }
    }
}

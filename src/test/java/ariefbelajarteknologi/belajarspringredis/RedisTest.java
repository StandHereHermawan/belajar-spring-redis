package ariefbelajarteknologi.belajarspringredis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.*;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.*;

import java.time.Duration;
import java.util.HashMap;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItems;
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

        operations.set("name","Arief", Duration.ofSeconds(2));
        assertEquals("Arief",operations.get("name"));

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
}

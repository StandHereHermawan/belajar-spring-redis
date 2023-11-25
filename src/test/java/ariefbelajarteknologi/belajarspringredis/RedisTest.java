package ariefbelajarteknologi.belajarspringredis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
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
}

package ariefbelajarteknologi.belajarspringredis;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
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
}

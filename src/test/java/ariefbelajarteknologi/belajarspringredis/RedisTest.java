package ariefbelajarteknologi.belajarspringredis;

import ariefbelajarteknologi.belajarspringredis.entity.Product;
import ariefbelajarteknologi.belajarspringredis.repository.ProductRepository;
import ariefbelajarteknologi.belajarspringredis.service.ProductService;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
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
import org.springframework.data.redis.support.collections.DefaultRedisMap;
import org.springframework.data.redis.support.collections.RedisList;
import org.springframework.data.redis.support.collections.RedisSet;
import org.springframework.data.redis.support.collections.RedisZSet;

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

    @Autowired
    private ProductRepository productRepository;

    @Autowired
    private CacheManager cacheManager;

    @Autowired
    private ProductService productService;

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

    @Test
    void redisList() {
        List<String> list = RedisList.create("names", redisTemplate);
        list.add("Arief");
        list.add("Erlang");
        list.add("Indra");
        list.add("Fatan");
        assertThat(list, hasItems("Arief", "Erlang", "Indra", "Fatan"));

        List<String> result = redisTemplate.opsForList().range("names", 0, -1);
        assertThat(result, hasItems("Arief", "Erlang", "Indra", "Fatan"));
    }

    @Test
    void redisSet() {
        Set<String> set = RedisSet.create("traffic", redisTemplate);
        set.addAll(Set.of("Arief", "Erlang", "Indra", "Fatan"));
        set.addAll(Set.of("Arief", "Karditya", "Anggara", "Basyari"));
        set.addAll(Set.of("Julia", "Karditya", "Anggara", "Basyari"));
        assertThat(set, hasItems("Arief", "Karditya", "Erlang", "Anggara", "Fatan", "Basyari", "Indra", "Julia"));

        Set<String> members = redisTemplate.opsForSet().members("traffic");
        assertThat(members, hasItems("Arief", "Karditya", "Erlang", "Anggara", "Fatan", "Basyari", "Indra", "Julia"));
    }

    @Test
    void redisZSet() {
        RedisZSet<String> set = RedisZSet.create("winner", redisTemplate);
        set.add("Arief", 81);
        set.add("Erlang", 85);
        set.add("Fatan", 90);
        set.add("Indra", 84);
        assertThat(set, hasItems("Arief", "Fatan", "Erlang", "Indra"));

        Set<String> winner = redisTemplate.opsForZSet().range("winner", 0, -1);
        assertThat(winner, hasItems("Arief", "Fatan", "Erlang", "Indra"));

        assertEquals("Fatan", set.popLast());
        assertEquals("Erlang", set.popLast());
        assertEquals("Indra", set.popLast());
        assertEquals("Arief", set.popLast());
    }

    @Test
    void redisMap() {
        Map<String, String> map = new DefaultRedisMap<>("user:1", redisTemplate);
        map.put("name", "Arief");
        map.put("address", "Indonesia");
        assertThat(map, hasEntry("name", "Arief"));
        assertThat(map, hasEntry("address", "Indonesia"));

        Map<Object, Object> entries = redisTemplate.opsForHash().entries("user:1");
        assertThat(entries, hasEntry("name", "Arief"));
        assertThat(entries, hasEntry("address", "Indonesia"));
    }

    @Test
    void repository() {
        Product product = Product.builder()
                .id("1")
                .name("Pizza Tuna Melt")
                .price(110_000L)
                .build();
        productRepository.save(product);

        Product query = productRepository.findById("1").get();
        assertEquals(product, query);

        Map<Object, Object> map = redisTemplate.opsForHash().entries("products:1");
        assertEquals(product.getId(), map.get("id"));
        assertEquals(product.getName(), map.get("name"));
        assertEquals(product.getPrice().toString(), map.get("price"));
    }

    @Test
    void ttl() throws InterruptedException {
        Product product = Product.builder()
                .id("1")
                .name("Pizza Chicken Supreme")
                .price(110_000L)
                .ttl(3L)
                .build();
        productRepository.save(product);

        assertTrue(productRepository.findById("1").isPresent());

        Thread.sleep(Duration.ofSeconds(5));
        assertFalse(productRepository.findById("1").isPresent());
    }

    @Test
    void cache() {
        Cache cache = cacheManager.getCache("scores");
        cache.put("Arief", 85);
        cache.put("Hilmi", 85);

        assertEquals(85, cache.get("Arief", Integer.class));
        assertEquals(85, cache.get("Hilmi", Integer.class));

        cache.evict("Arief");
        cache.evict("Hilmi");

        assertNull(cache.get("Arief"));
        assertNull(cache.get("Hilmi"));
    }

    @Test
    void cacheable() {
        Product query1 = productService.getProduct("001");
        assertEquals("001", query1.getId());

        Product query2 = productService.getProduct("001");
        assertEquals(query1, query2);

        Product query3 = productService.getProduct("002");
        assertEquals(query1, query2);
    }

    @Test
    void cachePut() {
        Product product = Product.builder()
                .id("P002")
                .name("Asal")
                .price(100L)
                .build();
        productService.save(product);

        Product query = productService.getProduct("P002");
        assertEquals(product, query);
    }

    @Test
    void cacheEvict() {
        Product product = productService.getProduct("003");
        assertEquals("003", product.getId());

        productService.remove("003");

        Product product2 = productService.getProduct("003");
        assertEquals(product2, product);
    }
}

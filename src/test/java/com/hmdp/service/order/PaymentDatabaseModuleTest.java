package com.hmdp.service.order;

import com.hmdp.mapper.OrderLifecycleMapper;
import org.apache.ibatis.executor.statement.StatementHandler;
import org.apache.ibatis.plugin.*;
import org.apache.ibatis.reflection.SystemMetaObject;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;

import java.net.URI;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;

class PaymentDatabaseModuleTest {
    private static final String[] TABLES = {"tb_voucher_order", "tb_seckill_voucher",
            "tb_voucher_purchase_qualification", "tb_order_compensation_task"};

    @Test
    void realMysqlRollsBackAndSerializesPaymentAgainstClose() throws Exception {
        String url = System.getenv("HMDP_MYSQL_TEST_URL");
        String user = System.getenv("HMDP_MYSQL_TEST_USER");
        String password = System.getenv("HMDP_MYSQL_TEST_PASSWORD");
        assumeTrue(url != null && user != null, "Dedicated MySQL test environment is not configured");
        assertTrue(url.startsWith("jdbc:mysql://"), "Only a standard MySQL JDBC test URL is allowed");
        URI target = URI.create(url.substring("jdbc:".length()));
        String database = target.getPath() == null ? "" : target.getPath().substring(1);
        assertTrue(database.matches("hmdp_test(?:_[A-Za-z0-9_]+)?"), "Only an hmdp_test database is allowed");
        assertNull(target.getUserInfo(), "Pass test credentials through the dedicated environment variables");

        DriverManagerDataSource source = new DriverManagerDataSource(url, user, password == null ? "" : password);
        JdbcTemplate jdbc = new JdbcTemplate(source);
        jdbc.setQueryTimeout(10);
        assertEquals(database, jdbc.queryForObject("SELECT DATABASE()", String.class));
        String prefix = "hmdp_it_" + UUID.randomUUID().toString().replace("-", "").substring(0, 12) + "_";
        List<String> created = new ArrayList<>();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            create(jdbc, created, prefix + TABLES[0], "id BIGINT PRIMARY KEY,user_id BIGINT NOT NULL," +
                    "voucher_id BIGINT NOT NULL,status INT NOT NULL,pay_time DATETIME(3)," +
                    "pay_expire_time DATETIME(3),update_time DATETIME(3)");
            create(jdbc, created, prefix + TABLES[1], "voucher_id BIGINT PRIMARY KEY,stock INT NOT NULL");
            create(jdbc, created, prefix + TABLES[2], "user_id BIGINT NOT NULL,voucher_id BIGINT NOT NULL," +
                    "order_id BIGINT NOT NULL,UNIQUE KEY uq_user_voucher(user_id,voucher_id),UNIQUE KEY uq_order(order_id)");
            create(jdbc, created, prefix + TABLES[3], "id BIGINT AUTO_INCREMENT PRIMARY KEY,order_id BIGINT NOT NULL," +
                    "user_id BIGINT NOT NULL,voucher_id BIGINT NOT NULL,task_type VARCHAR(32) NOT NULL,status INT NOT NULL," +
                    "retry_count INT NOT NULL,next_execute_time DATETIME(3),claim_token VARCHAR(64),lease_until DATETIME(3)," +
                    "last_error VARCHAR(500),create_time DATETIME(3),update_time DATETIME(3),completed_time DATETIME(3)," +
                    "UNIQUE KEY uq_order_task(order_id,task_type)");

            Configuration configuration = new Configuration();
            configuration.setMapUnderscoreToCamelCase(true);
            configuration.addInterceptor(new IsolatedTables(prefix));
            SqlSessionFactoryBean factory = new SqlSessionFactoryBean();
            factory.setDataSource(source);
            factory.setConfiguration(configuration);
            factory.afterPropertiesSet();
            factory.getObject().getConfiguration().addMapper(OrderLifecycleMapper.class);
            SqlSessionTemplate sessions = new SqlSessionTemplate(factory.getObject());
            OrderLifecycleMapper mapper = sessions.getMapper(OrderLifecycleMapper.class);
            DataSourceTransactionManager manager = new DataSourceTransactionManager(source);
            OrderLifecycleService service = new OrderLifecycleService(mapper, mock(SeckillRedisStore.class), manager);
            String orders = prefix + TABLES[0];
            String stock = prefix + TABLES[1];
            String qualification = prefix + TABLES[2];
            String tasks = prefix + TABLES[3];

            jdbc.update("INSERT INTO " + stock + " VALUES(30,0),(31,0),(32,0)");
            jdbc.update("INSERT INTO " + orders + "(id,user_id,voucher_id,status,pay_expire_time) " +
                    "VALUES(10,20,30,1,DATE_SUB(NOW(3),INTERVAL 1 SECOND))");
            assertThrows(IllegalStateException.class, () -> service.closeExpired(10L));
            assertEquals(1, number(jdbc, "SELECT status FROM " + orders + " WHERE id=10"));
            assertEquals(0, number(jdbc, "SELECT stock FROM " + stock + " WHERE voucher_id=30"));
            assertEquals(0, number(jdbc, "SELECT COUNT(*) FROM " + tasks));
            jdbc.update("INSERT INTO " + qualification + " VALUES(20,30,10)");
            jdbc.update("INSERT INTO " + tasks + "(order_id,user_id,voucher_id,task_type,status,retry_count) " +
                    "VALUES(10,20,30,'CLOSE_RELEASE',0,0)");
            assertThrows(DuplicateKeyException.class, () -> service.closeExpired(10L));
            assertEquals(1, number(jdbc, "SELECT status FROM " + orders + " WHERE id=10"));
            assertEquals(0, number(jdbc, "SELECT stock FROM " + stock + " WHERE voucher_id=30"));
            assertEquals(1, number(jdbc, "SELECT COUNT(*) FROM " + qualification + " WHERE order_id=10"));
            assertEquals(1, number(jdbc, "SELECT COUNT(*) FROM " + tasks));
            jdbc.update("DELETE FROM " + tasks + " WHERE order_id=10");
            assertTrue(service.closeExpired(10L));
            assertFalse(service.closeExpired(10L));
            assertEquals(1, number(jdbc, "SELECT stock FROM " + stock + " WHERE voucher_id=30"));
            assertEquals(0, number(jdbc, "SELECT COUNT(*) FROM " + qualification));
            assertEquals(1, number(jdbc, "SELECT COUNT(*) FROM " + tasks));
            assertFalse(service.pay(10L, 20L).getSuccess());

            jdbc.update("INSERT INTO " + qualification + " VALUES(20,30,11)");
            assertThrows(DuplicateKeyException.class,
                    () -> jdbc.update("INSERT INTO " + qualification + " VALUES(20,30,12)"));
            assertThrows(DuplicateKeyException.class,
                    () -> jdbc.update("INSERT INTO " + qualification + " VALUES(21,30,11)"));

            // Hold an actual paid row lock while close competes for the same order.
            jdbc.update("INSERT INTO " + orders + "(id,user_id,voucher_id,status,pay_expire_time) " +
                    "VALUES(100,21,31,1,DATE_ADD(NOW(3),INTERVAL 30 SECOND))");
            jdbc.update("INSERT INTO " + qualification + " VALUES(21,31,100)");
            CountDownLatch paymentUpdated = new CountDownLatch(1);
            CountDownLatch allowCommit = new CountDownLatch(1);
            Future<Integer> payment = workers.submit(() -> {
                TransactionTemplate transaction = new TransactionTemplate(manager);
                transaction.setTimeout(10);
                return transaction.execute(state -> {
                    int changed = mapper.pay(100L, 21L);
                    paymentUpdated.countDown();
                    await(allowCommit);
                    return changed;
                });
            });
            assertTrue(paymentUpdated.await(5, TimeUnit.SECONDS));
            Future<Boolean> close = workers.submit(() -> service.closeExpired(100L));
            allowCommit.countDown();
            assertEquals(1, payment.get(10, TimeUnit.SECONDS));
            assertFalse(close.get(10, TimeUnit.SECONDS));
            assertTrue(service.pay(100L, 21L).getSuccess());
            assertEquals(2, number(jdbc, "SELECT status FROM " + orders + " WHERE id=100"));
            assertEquals(0, number(jdbc, "SELECT stock FROM " + stock + " WHERE voucher_id=31"));
            assertEquals(1, number(jdbc, "SELECT COUNT(*) FROM " + qualification + " WHERE order_id=100"));

            jdbc.update("INSERT INTO " + orders + "(id,user_id,voucher_id,status,pay_expire_time) " +
                    "VALUES(200,22,32,1,DATE_SUB(NOW(3),INTERVAL 1 SECOND))");
            jdbc.update("INSERT INTO " + qualification + " VALUES(22,32,200)");
            CyclicBarrier race = new CyclicBarrier(2);
            Future<Boolean> overduePayment = workers.submit(() -> {
                race.await(5, TimeUnit.SECONDS);
                return service.pay(200L, 22L).getSuccess();
            });
            Future<Boolean> overdueClose = workers.submit(() -> {
                race.await(5, TimeUnit.SECONDS);
                return service.closeExpired(200L);
            });
            assertFalse(overduePayment.get(10, TimeUnit.SECONDS));
            assertTrue(overdueClose.get(10, TimeUnit.SECONDS));
            assertEquals(4, number(jdbc, "SELECT status FROM " + orders + " WHERE id=200"));
            assertEquals(1, number(jdbc, "SELECT stock FROM " + stock + " WHERE voucher_id=32"));
            assertEquals(0, number(jdbc, "SELECT COUNT(*) FROM " + qualification + " WHERE order_id=200"));
            assertEquals(1, number(jdbc, "SELECT COUNT(*) FROM " + tasks + " WHERE order_id=200"));
        } finally {
            workers.shutdownNow();
            workers.awaitTermination(10, TimeUnit.SECONDS);
            for (int i = created.size() - 1; i >= 0; i--) jdbc.execute("DROP TABLE " + created.get(i));
        }
    }

    private static int number(JdbcTemplate jdbc, String sql) {
        return jdbc.queryForObject(sql, Integer.class);
    }

    private static void create(JdbcTemplate jdbc, List<String> created, String table, String columns) {
        jdbc.execute("CREATE TABLE " + table + " (" + columns + ") ENGINE=InnoDB");
        created.add(table);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) throw new IllegalStateException("Test coordination timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Intercepts(@Signature(type = StatementHandler.class, method = "prepare", args = {Connection.class, Integer.class}))
    static final class IsolatedTables implements Interceptor {
        private final String prefix;
        IsolatedTables(String prefix) { this.prefix = prefix; }

        @Override
        public Object intercept(Invocation invocation) throws Throwable {
            StatementHandler statement = (StatementHandler) invocation.getTarget();
            String sql = statement.getBoundSql().getSql();
            for (String table : TABLES) sql = sql.replaceAll("\\b" + table + "\\b", prefix + table);
            SystemMetaObject.forObject(statement.getBoundSql()).setValue("sql", sql);
            return invocation.proceed();
        }

        @Override
        public Object plugin(Object target) { return Plugin.wrap(target, this); }
    }
}

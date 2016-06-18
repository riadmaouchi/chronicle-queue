package org.chronicle.queue;

import net.openhft.chronicle.core.Jvm;
import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.io.Closeable;
import net.openhft.chronicle.core.io.IOTools;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.JDBCResult;
import net.openhft.chronicle.queue.JDBCService;
import net.openhft.chronicle.queue.JDBCStatement;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.AbstractMarshallable;
import net.openhft.chronicle.wire.Marshallable;
import net.openhft.chronicle.wire.MethodReader;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;


public class JDBCServiceTest {

    private Connection conn;

    @Before
    public void setupDB() throws SQLException, SqlToolError, URISyntaxException, IOException {
        File file = new File(OS.TARGET, "hsqldb-" + System.nanoTime());
        file.deleteOnExit();

        conn = DriverManager.getConnection("jdbc:hsqldb:file:" + file.getAbsolutePath(), "SA", "");

        URL url = getClass().getResource("/test.sql");
        final SqlFile sqlFile = new SqlFile(new File(url.toURI()));
        sqlFile.setConnection(conn);
        sqlFile.execute();
    }

    @Test
    public void testExecuteQuery() throws SQLException, IOException {
        String path1 = OS.TARGET + "/createTable-" + System.nanoTime();
        String path2 = OS.TARGET + "/createTable-" + System.nanoTime();

        try (ChronicleQueue in = SingleChronicleQueueBuilder.binary(path1).build();
             ChronicleQueue out = SingleChronicleQueueBuilder.binary(path2).build()) {

            JDBCService service = new JDBCService(in, out, () -> conn);

            JDBCStatement writer = service.createWriter();

            writer.executeQuery("SELECT name, num from tableName", TableName.class);

            AtomicLong queries = new AtomicLong();
            AtomicLong updates = new AtomicLong();
            CountingJDBCResult countingJDBCResult = new CountingJDBCResult(queries, updates);

            MethodReader methodReader = service.createReader(countingJDBCResult);
            methodReader.readOne();

            Closeable.closeQuietly(service);

            System.out.println(in.dump());
            System.out.println(out.dump());

        } finally {
            try {
                IOTools.deleteDirWithFiles(path1, 2);
                IOTools.deleteDirWithFiles(path2, 2);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static class TableName extends AbstractMarshallable {
        public final String name;
        public final int num;

        public TableName(String name, int num) {
            this.name = name;
            this.num = num;
        }
    }

    private static class CountingJDBCResult implements JDBCResult {
        private final AtomicLong queries;
        private final AtomicLong updates;

        public CountingJDBCResult(AtomicLong queries, AtomicLong updates) {
            this.queries = queries;
            this.updates = updates;
        }

        @Override
        public void queryResult(Iterator<Marshallable> marshallableList, String query, Object... args) {
            queries.incrementAndGet();
        }

        @Override
        public void queryThrown(Throwable t, String query, Object... args) {
            throw Jvm.rethrow(t);
        }

        @Override
        public void updateResult(long count, String update, Object... args) {
            updates.incrementAndGet();
        }

        @Override
        public void updateThrown(Throwable t, String update, Object... args) {
            throw Jvm.rethrow(t);
        }
    }
}
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

import java.util.Random;
import java.util.UUID;

public class ConsistentSessionClient {
  private Keyspace keyspace;
  private AstyanaxContext<Keyspace> context;
  private ColumnFamily<String, Integer> EMP_CF;

  public void init() {
    context = new AstyanaxContext.Builder()
        .forCluster("Test Cluster")
        .forKeyspace("test1")
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
            .setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
        )
        .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
            .setPort(9160)
            .setMaxConnsPerHost(1)
            .setSeeds("127.0.0.1:9160")
        )
        .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
            .setCqlVersion("3.0.0")
            .setTargetCassandraVersion("1.2"))
        .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
        .buildKeyspace(ThriftFamilyFactory.getInstance());

    context.start();
    keyspace = context.getEntity();

    EMP_CF = ColumnFamily.newColumnFamily(
        "mojo_session",
        StringSerializer.get(),
        IntegerSerializer.get());
  }

  public ConsistentSession primitiveUpdate(String sessionid, int version, String data) {
    return primitiveUpdate(sessionid, version, data, true);
  }

  public ConsistentSession primitiveUpdate(String sessionid, int version, String data, boolean updateVersionNumber) {
    version = updateVersionNumber ? version + 1 : version;
//    System.out.println("Updated version to " + version);
    OperationResult<Void> result = null;
    long randomSkew = (long) (Math.random() * 300);
    MutationBatch m = keyspace.prepareMutationBatch()
        .withConsistencyLevel(ConsistencyLevel.CL_QUORUM)
        .withTimestamp(System.currentTimeMillis() + randomSkew);
    try {
        m.withRow(EMP_CF, sessionid)
            .putColumn(version, data, null);
        result = m.execute();
        return new ConsistentSession(sessionid, version, data);
    } catch (ConnectionException e) {
      System.out.println("failed to write data");
      throw new RuntimeException("failed to write data to C*", e);
    }
  }

  private ConsistentSession update(ConsistentSession read) {
    ConsistentSession existingSession = primitiveRead(read.getSessionid());
    if (read.getVersion() == existingSession.getVersion()) {
      return primitiveUpdate(existingSession.getSessionid(), existingSession.getVersion(), read.getData());
    } else {
      System.out.println("Stored version is " + existingSession.getVersion());
      System.out.println("Client version is " + read.getVersion());
      throw new IllegalStateException("Version conflict");
    }
  }

  public ConsistentSession read(String sessionid) {
    ConsistentSession read = primitiveRead(sessionid);
    return primitiveUpdate(sessionid, read.getVersion(), read.getData());
//    read.setVersion(read.getVersion() + 1);
//    return read;
  }

  public ConsistentSession primitiveRead(String sessionid) {
    OperationResult<ColumnList<Integer>> result;
    try {
      ColumnFamilyQuery<String, Integer> query = keyspace.prepareQuery(EMP_CF);
      query.setConsistencyLevel(ConsistencyLevel.CL_QUORUM);
      result = query
          .getKey(sessionid).withColumnRange(new RangeBuilder().setReversed(true).setLimit(1).build())
          .execute();

      ColumnList<Integer> cols = result.getResult();
      Column<Integer> latestColumn = cols.getColumnByIndex(0);
//      System.out.println("Latest column is " + latestColumn.getName());
      ConsistentSession session = new ConsistentSession(sessionid, latestColumn.getName(), latestColumn.getStringValue());
      return session;
    } catch (ConnectionException e) {
      System.out.println("failed to read from C*");
      System.out.println(e);

      throw new RuntimeException("failed to read from C*", e);
    }
  }

  public static void main(String[] args) throws Exception {
    ConsistentSessionClient c = new ConsistentSessionClient();
    String randomID = UUID.randomUUID().toString();
    c.init();
    c.put(new ConsistentSession(randomID, 0, "data"));
    for (int i = 0; i <= 10000; ++i) {
      ConsistentSession innerRead = c.read(randomID);
      innerRead.setData("data-" + i);
      ConsistentSession writtenSession = c.update(innerRead);
      innerRead = c.read(randomID);
      int expectedVersionAfterRead = writtenSession.getVersion() + 1;
      System.out.println((innerRead.getVersion()) + " == " + expectedVersionAfterRead + "?");
      if ((innerRead.getVersion()) != expectedVersionAfterRead) throw new Exception("Reproduced");
    }
  }

  private ConsistentSession put(ConsistentSession session) {
    return primitiveUpdate(session.getSessionid(), session.getVersion(), session.getData());
  }
}

//create column family session
//with comparator=UTF8Type
//and column_metadata = [
//    {column_name: version, validation_class: IntegerType}
//    ];

//create column family mojo_session
//with comparator = IntegerType
//and key_validation_class = UTF8Type
//and default_validation_class = UTF8Type;
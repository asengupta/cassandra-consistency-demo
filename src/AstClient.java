import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.query.ColumnFamilyQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

import java.util.UUID;

public class AstClient {
  private Keyspace keyspace;
  private AstyanaxContext<Keyspace> context;
  private ColumnFamily<String, String> EMP_CF;

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
        "session",
        StringSerializer.get(),
        StringSerializer.get());
  }

  public Session primitiveUpdate(String sessionid, int version) {
    return primitiveUpdate(sessionid, version, true);
  }

  public Session primitiveUpdate(String sessionid, int version, boolean updateVersionNumber) {
    version = updateVersionNumber ? version + 1 : version;
    OperationResult<Void> result = null;
    MutationBatch m = keyspace.prepareMutationBatch();
    try {
        m.withRow(EMP_CF, sessionid)
//            .putColumn("sessionid", sessionid, null)
            .putColumn("version", version, null);
        result = m.execute();
        return new Session(sessionid, version);
    } catch (ConnectionException e) {
      System.out.println("failed to write data");
      throw new RuntimeException("failed to write data to C*", e);
    }
  }

  private Session update(Session read) {
    Session existingSession = primitiveRead(read.getSessionid());
    if (read.getVersion() == existingSession.getVersion()) {
      return primitiveUpdate(existingSession.getSessionid(), existingSession.getVersion());
    } else {
      throw new IllegalStateException("Version conflict");
    }
  }

  public Session read(String sessionid) {
    Session read = primitiveRead(sessionid);
    primitiveUpdate(sessionid, read.getVersion(), false);
    return read;
  }

  public Session primitiveRead(String sessionid) {
    OperationResult<ColumnList<String>> result;
    try {
      ColumnFamilyQuery<String, String> query = keyspace.prepareQuery(EMP_CF);
      query.setConsistencyLevel(ConsistencyLevel.CL_ONE);
      result = query
          .getKey(sessionid)
          .execute();

      ColumnList<String> cols = result.getResult();

      Session session = new Session(sessionid, cols.getColumnByName("version").getIntegerValue());
      return session;
    } catch (ConnectionException e) {
      System.out.println("failed to read from C*");
      System.out.println(e);

      throw new RuntimeException("failed to read from C*", e);
    }
  }

  public static void main(String[] args) throws Exception {
    AstClient c = new AstClient();
    String randomID = UUID.randomUUID().toString();
    c.init();
    c.put(new Session(randomID, 0));
    for (int i = 0; i <= 10000; ++i) {
      Session innerRead = c.read(randomID);
      Session writtenSession = c.update(innerRead);
      innerRead = c.read(randomID);
      System.out.println(innerRead.getVersion() + " == " + writtenSession.getVersion() + "?");
      if (innerRead.getVersion() != writtenSession.getVersion()) throw new Exception("Reproduced");
    }
  }

  private void put(Session session) {
    primitiveUpdate(session.getSessionid(), session.getVersion());
  }
}

//create column family session
//with comparator=UTF8Type
//and column_metadata = [
//    {column_name: version, validation_class: IntegerType}
//    ];

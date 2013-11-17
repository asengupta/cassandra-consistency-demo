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

import java.util.Date;
import java.util.Random;

public class AstClient {
  private Keyspace keyspace;
  private AstyanaxContext<Keyspace> context;
  private ColumnFamily<Integer, String> EMP_CF;

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
        "employees2",
        IntegerSerializer.get(),
        StringSerializer.get());
  }

  public Session primitiveUpdate(int empId, int deptId, String lastName) {
    return primitiveUpdate(empId, deptId, lastName, true);
  }

  public Session primitiveUpdate(int empId, int deptId, String lastName, boolean updateVersionNumber) {
    deptId = updateVersionNumber ? deptId + 1 : deptId;
    OperationResult<Void> result = null;
    MutationBatch m = keyspace.prepareMutationBatch();
    try {
        m.withRow(EMP_CF, empId)
            .putColumn("empid", empId, null)
            .putColumn("deptid", deptId, null)
            .putColumn("first_name", new Date().toString(), null)
            .putColumn("last_name", lastName, null);
        result = m.execute();
        return new Session(empId, deptId, new Date().toString(), lastName);
    } catch (ConnectionException e) {
      System.out.println("failed to write data");
      throw new RuntimeException("failed to write data to C*", e);
    }
  }

  private Session update(Session read) {
    Session existingSession = primitiveRead(read.getEmpid());
    if (read.getDeptid() == existingSession.getDeptid()) {
      return primitiveUpdate(existingSession.getEmpid(), existingSession.getDeptid(), existingSession.getLast_name());
    } else {
      throw new IllegalStateException("Version conflict");
    }
  }

  public Session read(int empId) {
    Session read = primitiveRead(empId);
    primitiveUpdate(empId, read.getDeptid(), "Cartman", false);
    return read;
  }

  public Session primitiveRead(int empId) {
    OperationResult<ColumnList<String>> result;
    try {
      ColumnFamilyQuery<Integer, String> query = keyspace.prepareQuery(EMP_CF);
      query.setConsistencyLevel(ConsistencyLevel.CL_ONE);
      result = query
          .getKey(empId)
          .execute();

      ColumnList<String> cols = result.getResult();

      Column<String> first_name = cols.getColumnByName("first_name");
      String firstNameData = first_name == null ? "NONE" : first_name.getStringValue();
      Session session = new Session(cols.getColumnByName("empid").getIntegerValue(), cols.getColumnByName("deptid").getIntegerValue(), firstNameData, cols.getColumnByName("last_name").getStringValue());
      return session;
    } catch (ConnectionException e) {
      System.out.println("failed to read from C*");
      System.out.println(e);

      throw new RuntimeException("failed to read from C*", e);
    }
  }

  public static void main(String[] args) throws Exception {
    AstClient c = new AstClient();
    int randomID = new Random().nextInt(13);
    c.init();
    c.put(new Session(randomID, 0, new Date().toString(), "Cartman"));
    for (int i = 0; i <= 10000; ++i) {
      Session innerRead = c.read(randomID);
      Session writtenSession = c.update(innerRead);
      innerRead = c.read(randomID);
      System.out.println(innerRead.getDeptid() + " == " + writtenSession.getDeptid() + "?");
      if (innerRead.getDeptid() != writtenSession.getDeptid()) throw new Exception("Reproduced");
    }
  }

  private void put(Session session) {
    primitiveUpdate(session.getEmpid(), session.getDeptid(), "Cartman");
  }
}
//create column family employees2
//with comparator=UTF8Type
//and column_metadata = [
//    {column_name: empid, validation_class: IntegerType}
//    {column_name: deptid, validation_class: IntegerType}
//    {column_name: first_name, validation_class: UTF8Type}
//    {column_name: last_name, validation_class: UTF8Type}
//    ];

//create column family session
//with comparator=UTF8Type
//and column_metadata = [
//    {column_name: sessionid, validation_class: UTF8Type}
//    {column_name: version, validation_class: IntegerType}
//    {column_name: first_name, validation_class: UTF8Type}
//    {column_name: last_name, validation_class: UTF8Type}
//    ];

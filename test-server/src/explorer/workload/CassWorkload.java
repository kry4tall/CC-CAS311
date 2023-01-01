package explorer.workload;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.*;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.DropKeyspace;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import explorer.ExplorerConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.*;

class CassWorkload {

  private static Logger log = LoggerFactory.getLogger(CassWorkload.class);

  public static final int clusterPort = ExplorerConf.getInstance().clusterPort;
  public static final int poolTimeoutMillis = ExplorerConf.getInstance().poolTimeoutMillis;
  public static final int readTimeoutMillis = ExplorerConf.getInstance().clusterPort;
  public static final int timeBetweenQueriesMillis = ExplorerConf.getInstance().timeBetweenQueriesMillis;

  private static void initCas() {
    String host1 = "127.0.0.1";
    int port = 9042;
    Cluster cluster1 = Cluster.builder()
            .addContactPoint(host1)
            .withPort(port)
            .build();
    Session session1 = cluster1.connect();
    System.out.println(host1);
    List<KeyspaceMetadata> keyspaces1 = session1.getCluster().getMetadata().getKeyspaces();
    List<String> keyspaces_names = new ArrayList<>();
    for(KeyspaceMetadata keyspace : keyspaces1){
      System.out.println(keyspace.getName());
      keyspaces_names.add(keyspace.getName());
    }

    // drop keyspace test
    session1 = cluster1.connect();
    DropKeyspace dropKeyspace = SchemaBuilder.dropKeyspace("test").ifExists();
    session1.execute(dropKeyspace);

    //create keyspace test
    Map<String,Object> replication = new HashMap<>();
    replication.put("class","SimpleStrategy");
    replication.put("replication_factor",3);
    KeyspaceOptions options = SchemaBuilder.createKeyspace("test")
            .ifNotExists()
            .with()
            .replication(replication);
    session1.execute(options);

    //create table
    Create create = SchemaBuilder.createTable("test", "tests")
            .addPartitionKey("name", DataType.text())
            .addColumn("owner", DataType.text())
            .addColumn("value_1", DataType.text())
            .addColumn("value_2", DataType.text())
            .addColumn("value_3", DataType.text())
            .ifNotExists();
    session1.execute(create);

    //insert data
    String cql = "INSERT INTO test.tests (name,owner) VALUES ('testing','user_1')";
    ResultSet resultSet = session1.execute(cql);

    String host2 = "127.0.0.2";
    System.out.println(host2);
    Cluster cluster2 = Cluster.builder()
            .addContactPoint(host2)
            .withPort(port)
            .build();
    Session session2 = cluster2.connect();
    List<KeyspaceMetadata> keyspaces2 = session2.getCluster().getMetadata().getKeyspaces();
    for(KeyspaceMetadata keyspace : keyspaces2){
      System.out.println(keyspace.getName());
    }

    String host3 = "127.0.0.3";
    System.out.println(host3);
    Cluster cluster3 = Cluster.builder()
            .addContactPoint(host3)
            .withPort(port)
            .build();
    Session session3 = cluster3.connect();
    List<KeyspaceMetadata> keyspaces3 = session3.getCluster().getMetadata().getKeyspaces();
    for(KeyspaceMetadata keyspace : keyspaces3){
      System.out.println(keyspace.getName());
    }

  }
  static void execute6023() {
    try {

      initCas();

      executeCql(0, "test", "UPDATE tests SET value_1 = 'A' WHERE name = 'testing' IF owner = 'user_1'");
      //executeCql(0, "test", "UPDATE tests SET value_1 = 'A' WHERE name = 'testing' IF owner = 'user_1';");//.get();
      System.out.println("sleep 500ms");
      Thread.sleep(timeBetweenQueriesMillis);

      executeCql(1, "test", "UPDATE tests SET value_1 = 'B', value_2 = 'B' WHERE name = 'testing' IF  value_1 = 'A'");
      //executeCql(1, "test", "UPDATE tests SET value_1 = 'B', value_2 = 'B' WHERE name = 'testing' IF owner = 'user_1';");//.get();
      System.out.println("sleep 500ms");
      Thread.sleep(timeBetweenQueriesMillis);

      executeCql(2, "test", "UPDATE tests SET value_3 = 'C' WHERE name = 'testing' IF owner = 'user_1'");
      //executeCql(2, "test", "UPDATE tests SET value_3 = 'C' WHERE name = 'testing' IF owner = 'user_1';");//.get();
      System.out.println("sleep 500ms");
      Thread.sleep(timeBetweenQueriesMillis);
    } catch (InterruptedException e) {
      log.error("Interrupted while sleeping", e);
    }
  }

  static void reset6023() {
    executeCql(1, "test", "UPDATE tests SET value_1 = 'value_1', value_2 = 'value_2' WHERE name = 'testing'");
  }

  static void submitQuery(int nodeId, String query) {
    executeCql(nodeId, "test", query);
    try {
      Thread.sleep(timeBetweenQueriesMillis);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  static void submitQueries(List<Integer> nodeIds, List<String> queries) {
    if(nodeIds.size() != queries.size()) {
      log.error("The number of nodes to submit is not equal to the number of the queries.");
      System.exit(-1);
    }

    for(int i = 0; i < nodeIds.size(); i++) {
      try {
        executeCql(nodeIds.get(i), "test", queries.get(i));
        //executeCql(0, "test", "UPDATE tests SET value_1 = 'A' WHERE name = 'testing' IF owner = 'user_1';");//.get();
        Thread.sleep(timeBetweenQueriesMillis);
      } catch (InterruptedException e) {
        log.error("Interrupted while sleeping", e);
      }
    }

  }

  private static boolean executeCql(int nodeId, String keyspace, String cql) {
    try (Cluster cluster = getCluster(nodeId).init(); Session session = cluster.connect(keyspace).init()) {
      log.info("Executing query for cluster {}: {}", nodeId, cql);
      ResultSet resultSet = session.execute(cql);
      return resultSet.wasApplied();
    } catch (Exception ex) {
      log.warn("=== Error with communication to node {}", nodeId, ex);
    }
    return false;
  }

  private static Cluster getCluster(int nodeId) {
    String nodeIp = CassNodeConfig.address(nodeId);
    Cluster cluster = Cluster.builder()
            .addContactPoint(nodeIp)
            .withPort(clusterPort)
            .withRetryPolicy(new CustomRetryPolicy(1, 1, 1)) // should retry same host
            .withLoadBalancingPolicy(new WhiteListPolicy(new RoundRobinPolicy(), Collections.singleton(new InetSocketAddress(nodeIp, clusterPort))))
            .build();
    //cluster.getConfiguration().getPoolingOptions().setPoolTimeoutMillis(poolTimeoutMillis);
    //cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(readTimeoutMillis);
    return cluster;
  }

  static class CustomRetryPolicy implements RetryPolicy {

    private final int readAttempts;
    private final int writeAttempts;
    private final int unavailableAttempts;

    private final int errorAttempts = 1;

    public CustomRetryPolicy(int readAttempts, int writeAttempts, int unavailableAttempts) {
      this.readAttempts = readAttempts;
      this.writeAttempts = writeAttempts;
      this.unavailableAttempts = unavailableAttempts;
    }

    @Override
    public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataReceived, int rTime) {
      if (dataReceived) {
        return RetryDecision.ignore();
      } else if (rTime < readAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      } else {
        return RetryDecision.rethrow();
      }
    }

    @Override
    public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt, int requiredResponses, int receivedResponses, int wTime) {
      if (wTime < writeAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      }
      return RetryDecision.rethrow();
    }

    @Override
    public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, int uTime) {
      if (uTime < unavailableAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      }
      return RetryDecision.rethrow();
    }

    @Override
    public void init(Cluster cluster) {

    }

    @Override
    public void close() {

    }

    @Override
    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
      log.info("Timeout - Request Error");
      if (nbRetry < errorAttempts) {
        return RetryDecision.retry(ConsistencyLevel.QUORUM);
      }
      return RetryDecision.ignore();
    }
  }

//  static class CustomRetryPolicy implements ExtendedRetryPolicy {
//
//    private final int readAttempts;
//    private final int writeAttempts;
//    private final int unavailableAttempts;
//
//    private final int errorAttempts = 1;
//
//    public CustomRetryPolicy(int readAttempts, int writeAttempts, int unavailableAttempts) {
//      this.readAttempts = readAttempts;
//      this.writeAttempts = writeAttempts;
//      this.unavailableAttempts = unavailableAttempts;
//    }
//
//    @Override
//    public RetryDecision onReadTimeout(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, boolean dataReceived, int rTime) {
//      if (dataReceived) {
//        return RetryDecision.ignore();
//      } else if (rTime < readAttempts) {
//        return RetryDecision.retry(ConsistencyLevel.QUORUM);
//      } else {
//        return RetryDecision.rethrow();
//      }
//    }
//
//    @Override
//    public RetryDecision onWriteTimeout(Statement stmnt, ConsistencyLevel cl, WriteType wt, int requiredResponses, int receivedResponses, int wTime) {
//      if (wTime < writeAttempts) {
//        return RetryDecision.retry(ConsistencyLevel.QUORUM);
//      }
//      return RetryDecision.rethrow();
//    }
//
//    @Override
//    public RetryDecision onUnavailable(Statement stmnt, ConsistencyLevel cl, int requiredResponses, int receivedResponses, int uTime) {
//      if (uTime < unavailableAttempts) {
//        return RetryDecision.retry(ConsistencyLevel.QUORUM);
//      }
//      return RetryDecision.rethrow();
//    }
//
//    @Override
//    public RetryDecision onRequestError(Statement statement, ConsistencyLevel cl, Exception e, int nbRetry) {
//      log.info("Timeout - Request Error");
//      if (nbRetry < errorAttempts) {
//        return RetryDecision.retry(ConsistencyLevel.QUORUM);
//      }
//      return RetryDecision.ignore();
//    }
//  }
}

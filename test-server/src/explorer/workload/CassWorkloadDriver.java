package explorer.workload;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class CassWorkloadDriver implements WorkloadDriver {

  private static Logger log = LoggerFactory.getLogger(CassWorkloadDriver.class);

  private final WorkloadDirs dirs;
  private final int numNodes;
  private final String classpath;
  private String javaPath;

  private final Map<Integer, Process> nodeProcesses = new HashMap<>();

  private int testId;

  public CassWorkloadDriver(WorkloadDirs dirs, int numNodes, String javaPath) {
    this.numNodes = numNodes;
    this.dirs = dirs;
    this.classpath = getClasspath();
    this.javaPath = javaPath;
  }

  @Override
  public void prepare(int testId) {
    this.testId = testId;
    CassNodeConfig template = new CassNodeConfig(dirs);
    try {
      for (int i = 0; i < numNodes; i++) {
        template.prepareRuntime(i);
        template.applyNodeConfig(i, numNodes);
      }
    } catch(IOException e) {
      log.error("Cannot prepare test folders");
      e.printStackTrace();
      System.exit(-1);
    }

  }

  @Override
  public void startEnsemble() {
    for (int i = 0; i < numNodes; i++) {
      startNode(i);
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        log.warn("Interrupted during starting node {}", i);
      }
    }
  }

  @Override
  public void sendWorkload() {
    CassWorkload.execute6023();
  }

  @Override
  public void submitQuery(int nodeId, String query) {
    if(nodeId >= numNodes)
      log.error("Cannot submit query to node " + nodeId + ". No such node.");
    else
      CassWorkload.submitQuery(nodeId, query);
  }

  @Override
  public void submitQueries(List<Integer> nodeIds, List<String> queries) {
    CassWorkload.submitQueries(nodeIds, queries);
  }

  @Override
  public void sendResetWorkload() {
    CassWorkload.reset6023();
  }

  @Override
  public void prepareNextTest() {
    CassNodeConfig template = new CassNodeConfig(dirs);
    template.prepareBallotFile();
  }

  @Override
  public void stopEnsemble() {
    nodeProcesses.forEach((id, process) -> {
      log.info("Stopping process {}...", id);
      process.destroy();
    });
    nodeProcesses.clear();
  }

  @Override
  public void cleanup() {
    testId = 0;
    try {
      FileUtils.deleteDirectory(dirs.getRunDirectory().toFile());
    } catch (IOException e) {
      log.error("Can't delete run directory.", e);
    }
  }

  private void startNode(int nodeId) {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.directory(dirs.getRunDirectory().toFile());
    processBuilder.redirectOutput(dirs.nodeHome(nodeId, "console.out").toFile());
    processBuilder.redirectError(dirs.nodeHome(nodeId, "console.err").toFile());

    List<String> command = new ArrayList<>();
    //command.add("/home/paper387/.jenv/shims/java");
    command.add(javaPath);
    /*
    if (nodeId == 2) {
      command.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
    }
     */
    command.addAll(nodeArguments(nodeId));
    command.add("org.apache.cassandra.service.CassandraDaemon");

    processBuilder.command(command);

    /*
    /home/krystal/java/jdk-8u151-linux-x64/jdk1.8.0_151/bin/java -Dcassandra.jmx.local.port=7199 -Dlogback.configurationFile=logback.xml -Dcassandra.logDir=/home/krystal/cas-test/explorer-server/cassandra/run/node_0/log -Dlog4j.configuration=cass_log.properties -Dcassandra.storagedir=/home/krystal/cas-test/explorer-server/cassandra/run/node_0/data -Dcassandra-foreground=no -cp /home/krystal/cas-test/explorer-server/cassandra/run/node_0/config:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/build/classes/main:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/build/classes/thrift:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/ecj-4.4.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/ST4-4.0.8.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/log4j-over-slf4j-1.7.25.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/concurrent-trees-2.4.0.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jflex-1.6.0.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/snakeyaml-1.26.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/logback-core-1.2.9.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jackson-core-2.13.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/cassandra-driver-core-3.0.1-shaded.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/lz4-1.3.0.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/netty-all-4.0.44.Final.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jackson-databind-2.13.2.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/snappy-java-1.1.1.7.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/slf4j-api-1.7.25.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/joda-time-2.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jackson-annotations-2.13.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/caffeine-2.2.6.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/disruptor-3.0.1.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/commons-codec-1.9.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/logback-classic-1.2.9.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/reporter-config-base-3.0.3.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/compress-lzf-0.8.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/thrift-server-0.3.7.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/concurrentlinkedhashmap-lru-1.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/asm-5.0.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/commons-cli-1.1.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jbcrypt-0.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/HdrHistogram-2.1.9.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jamm-0.3.0.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/metrics-core-3.1.5.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/ohc-core-0.4.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jcl-over-slf4j-1.7.25.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/stream-2.5.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/libthrift-0.9.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/ohc-core-j8-0.4.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/snowball-stemmer-1.3.0.581.1.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/reporter-config3-3.0.3.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jna-4.2.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/guava-18.0.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/json-simple-1.1.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/metrics-jvm-3.1.5.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/antlr-runtime-3.5.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/airline-0.6.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/high-scale-lib-1.0.6.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/commons-math3-3.2.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/gson-2.8.5.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/hppc-0.5.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/metrics-logback-3.1.5.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/commons-lang3-3.1.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/javax.inject-1.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/sigar-1.6.4.jar:/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/lib/jctools-core-1.2.1.jar -Dlog4j.defaultInitOverride=true org.apache.cassandra.service.CassandraDaemon
    */

    // processBuilder.inheritIO();

    Process process;
    try {
      process = processBuilder.start();

      nodeProcesses.put(nodeId, process);
    } catch (IOException e) {
      log.error("Error starting node", e);
    }
  }

  private String getClasspath() {
    List<String> list = new ArrayList<>();
    try {
      list.add(dirs.getTargetHome().resolve("build/classes/main").toString());
      list.add(dirs.getTargetHome().resolve("build/classes/thrift").toString());
      list.addAll(Files.list(dirs.getTargetHome().resolve("lib"))
              .filter(path -> path.toString().endsWith(".jar"))
              .map(path -> path.toString())
              .collect(Collectors.toList()));
    } catch (IOException e) {
      log.error("Cannot get classpath for initializing data folders.");
      e.printStackTrace();
      System.exit(-1);
    }

    return list.stream().map(Object::toString)
        .collect(Collectors.joining(":"));
  }

  private List<String> nodeArguments(int nodeId) {
    return Arrays.asList(
        "-Dcassandra.jmx.local.port=" + (7199 + nodeId),
        "-Dlogback.configurationFile=logback.xml",
        "-Dcassandra.logDir=" + dirs.nodeHome(nodeId, "log"),
        "-Dlog4j.configuration=cass_log.properties", // + Paths.get(runDirectory.getPath(), "config", "cass_log.properties").toUri().toString(),
        "-Dcassandra.storagedir=" + dirs.nodeHome(nodeId, "data"),
        "-Dcassandra-foreground=no",
        "-cp",
        dirs.nodeHome(nodeId, "config") + ":" + classpath,
        "-Dlog4j.defaultInitOverride=true");
  }
}

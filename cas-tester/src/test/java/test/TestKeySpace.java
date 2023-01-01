package test;

import com.datastax.driver.core.*;
import com.datastax.driver.core.schemabuilder.Create;
import com.datastax.driver.core.schemabuilder.DropKeyspace;
import com.datastax.driver.core.schemabuilder.KeyspaceOptions;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import org.example.pojo.Tests;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

public class TestKeySpace {

    Session session1 = null;

    String host1 = null;

    Process process0 = null;
    Process process1 = null;
    Process process2 = null;

    @Test
    public void StartNodes() throws IOException, InterruptedException {
        StartNode0();
        Thread.sleep(100);
        StartNode1();
        Thread.sleep(100);
        StartNode2();
        Thread.sleep(100);
    }

    @Test
    public void StartNode0() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        Path binPath = Paths.get("/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11","bin");
        processBuilder.directory(binPath.toFile());
        Path outputPath = Paths.get("/home/krystal/Desktop/0.txt");
        processBuilder.redirectOutput(outputPath.toFile());

        List<String> command = new ArrayList<>();
        //command.add("ls");
        //command.add("./cassandra -f -Dcassandra.config=file:///home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/conf/cassandra.yaml -Dcassandra.jmx.local.port=7199");
        command.add("./cassandra");
        command.add("-f");
        command.add("-Dcassandra.config=file:///home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/conf/cassandra.yaml");
        command.add("-Dcassandra.jmx.local.port=7199");
        command.add("-Dcassandra.logDir=/home/krystal/cas-test/cas-311/node0/log");
        command.add("-Dcassandra.storagedir=/home/krystal/cas-test/cas-311/node0/data");
        command.add("-Dcassandra-foreground=no");

        processBuilder.command(command);

        process0 = processBuilder.start();
    }

    @Test
    public void StartNode1() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        Path binPath = Paths.get("/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11","bin");
        processBuilder.directory(binPath.toFile());
        Path outputPath = Paths.get("/home/krystal/Desktop/1.txt");
        processBuilder.redirectOutput(outputPath.toFile());

        List<String> command = new ArrayList<>();
        //command.add("ls");
        //command.add("./cassandra -f -Dcassandra.config=file:///home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/conf/cassandra.yaml -Dcassandra.jmx.local.port=7199");
        command.add("./cassandra");
        command.add("-f");
        command.add("-Dcassandra.config=file:///home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/conf/cassandra1.yaml");
        command.add("-Dcassandra.jmx.local.port=7200");
        command.add("-Dcassandra.logDir=/home/krystal/cas-test/cas-311/node1/log");
        command.add("-Dcassandra.storagedir=/home/krystal/cas-test/cas-311/node1/data");
        command.add("-Dcassandra-foreground=no");

        processBuilder.command(command);

        process1 = processBuilder.start();
    }

    @Test
    public void StartNode2() throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        Path binPath = Paths.get("/home/krystal/cas-test/cas-311/cassandra-cassandra-3.11","bin");
        processBuilder.directory(binPath.toFile());
        Path outputPath = Paths.get("/home/krystal/Desktop/2.txt");
        processBuilder.redirectOutput(outputPath.toFile());

        List<String> command = new ArrayList<>();
        //command.add("ls");
        //command.add("./cassandra -f -Dcassandra.config=file:///home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/conf/cassandra.yaml -Dcassandra.jmx.local.port=7199");
        command.add("./cassandra");
        command.add("-f");
        command.add("-Dcassandra.config=file:///home/krystal/cas-test/cas-311/cassandra-cassandra-3.11/conf/cassandra2.yaml");
        command.add("-Dcassandra.jmx.local.port=7201");
        command.add("-Dcassandra.logDir=/home/krystal/cas-test/cas-311/node2/log");
        command.add("-Dcassandra.storagedir=/home/krystal/cas-test/cas-311/node2/data");
        command.add("-Dcassandra-foreground=no");

        processBuilder.command(command);

        process2 = processBuilder.start();
    }


    @Test
    public void destroyProcess(){
        process0.destroy();
        process1.destroy();
        process2.destroy();
    }

    @Test
    public void connectLocalhost(){
        try {
            ServerSocket serverSocket = new ServerSocket(4444);
            serverSocket.accept();
            BufferedReader br = null;
            br = new BufferedReader(new InputStreamReader(System.in));
            while(true)
            {
                System.out.println("if cassandra nodes ready, type 'ok'!");
                String input = br.readLine();
                if(input.equals("ok"))
                    break;
            }
        } catch (IOException e) {
            System.out.println("Couldn't get I/O for the connection");
            System.exit(1);
        }
    }

    @Test
    public void findKeySpace(){
        host1 = "127.0.0.1";
        int port = 9042;
        Cluster cluster1 = Cluster.builder()
                .addContactPoint(host1)
                .withPort(port)
                .build();
        session1 = cluster1.connect();
        System.out.println(host1);
        List<KeyspaceMetadata> keyspaces1 = session1.getCluster().getMetadata().getKeyspaces();
        for(KeyspaceMetadata keyspace : keyspaces1){
            System.out.println(keyspace.getName());
        }

    }

    @Test
    public void createKeySpace(){
        host1 = "127.0.0.1";
        int port = 9042;
        Cluster cluster1 = Cluster.builder()
                .addContactPoint(host1)
                .withPort(port)
                .build();
        session1 = cluster1.connect();
        Map<String,Object> replication = new HashMap<>();
        replication.put("class","SimpleStrategy");
        replication.put("replication_factor",3);
        KeyspaceOptions options = SchemaBuilder.createKeyspace("test")
                .ifNotExists()
                .with()
                .replication(replication);
        session1.execute(options);
    }

    @Test
    public void dropKeySpace(){
        host1 = "127.0.0.1";
        int port = 9042;
        Cluster cluster1 = Cluster.builder()
                .addContactPoint(host1)
                .withPort(port)
                .build();
        session1 = cluster1.connect();
        DropKeyspace dropKeyspace = SchemaBuilder.dropKeyspace("test").ifExists();
        session1.execute(dropKeyspace);
    }

    @Test
    public void createTable(){
        host1 = "127.0.0.1";
        int port = 9042;
        Cluster cluster1 = Cluster.builder()
                .addContactPoint(host1)
                .withPort(port)
                .build();
        session1 = cluster1.connect();
        Create create = SchemaBuilder.createTable("test", "tests")
                .addPartitionKey("name", DataType.text())
                .addColumn("owner", DataType.text())
                .addColumn("value_1", DataType.text())
                .addColumn("value_2", DataType.text())
                .addColumn("value_3", DataType.text())
                .ifNotExists();
        session1.execute(create);
    }

    @Test
    public void insertData(){
        host1 = "127.0.0.1";
        int port = 9042;
        Cluster cluster1 = Cluster.builder()
                .addContactPoint(host1)
                .withPort(port)
                .build();
        session1 = cluster1.connect();
        String cql = "INSERT INTO test.tests (name,owner) VALUES ('testing','user_1')";
        ResultSet resultSet = session1.execute(cql);
    }

    @Test
    public void updateData(){
        host1 = "127.0.0.1";
        int port = 9042;
        Cluster cluster1 = Cluster.builder()
                .addContactPoint(host1)
                .withPort(port)
                .build();
        session1 = cluster1.connect();
        String cql = "UPDATE test.tests SET value_1 = 'B', value_2 = 'B' WHERE name = 'testing' IF  value_1 = 'A'";
        //String cql = "UPDATE test.tests SET value_1 = 'A' WHERE name = 'testing' IF owner = 'user_1'";
        //String cql = "UPDATE test.tests SET value_1 = 'null', value_2 = 'null' WHERE name = 'testing'";
        ResultSet resultSet = session1.execute(cql);
    }

    @Test
    public void getData(){
        host1 = "127.0.0.3";
        int port = 9042;
        Cluster cluster1 = Cluster.builder()
                .addContactPoint(host1)
                .withPort(port)
                .build();
        session1 = cluster1.connect();
        ResultSet resultSet = session1.execute(select().from("test","tests"));
        Mapper<Tests> mapper = new MappingManager(session1).mapper(Tests.class);
        List<Tests> testsList = mapper.map(resultSet).all();
        for(Tests tests : testsList)
        {
            System.out.println(tests);
        }
    }
}

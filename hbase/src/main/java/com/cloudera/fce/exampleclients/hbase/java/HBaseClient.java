package com.cloudera.fce.exampleclients.hbase.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HBaseClient {

  private static Connection connection;

  private String clientConfig;
  private String keytabLocation;
  private String user;
  private String table;

  public HBaseClient(String clientConfig, String keytabLocation, String user, String table) {
    this.clientConfig = clientConfig;
    this.keytabLocation = keytabLocation;
    this.user = user;
    this.table = table;
  }

  public void initialise() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path(clientConfig));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    UserGroupInformation.setConfiguration(conf);
    UserGroupInformation.loginUserFromKeytab(user, keytabLocation);

    System.out.println(conf.toString());

    connection = ConnectionFactory.createConnection(conf);
  }

  public void doScan() throws IOException {
    Table tableRef = connection.getTable(TableName.valueOf(table));
    Scan scan = new Scan();
    ResultScanner scanner = tableRef.getScanner(scan);
    long now = System.currentTimeMillis();
    System.out.println("Starting scan");
    for (Result res : scanner) {
      System.out.println(res);
    }
    System.out.printf("Scan finished: %d ms\n\n", System.currentTimeMillis() - now);
    tableRef.close();
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.printf("Usage: %s <conf> <user> <keytab> <table>\n", HBaseClient.class);
      System.exit(-1);
    }

    HBaseClient exampleJavaClient = new HBaseClient(args [0], args[1], args[2], args[3]);
    exampleJavaClient.initialise();

    System.out.println("Scan 1");
    System.out.println();
    exampleJavaClient.doScan();

    System.out.println("Scan 2");
    System.out.println();
    exampleJavaClient.doScan();

    System.out.println("Scan 3");
    System.out.println();
    exampleJavaClient.doScan();
  }

}

package com.cloudera.fce.exampleclients.hbase.java;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier;
import org.apache.hadoop.hbase.security.token.TokenUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.File;
import java.io.IOException;

public class HBaseTokenClient {

  private static Connection connection;

  private String clientConfig;
  private String table;
  private String tokenFile = null;

  public HBaseTokenClient(String clientConfig, String table, String tokenFile) {
    this.clientConfig = clientConfig;
    this.table = table;
    this.tokenFile = tokenFile;
  }

  public void initialise() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.addResource(new Path(clientConfig));
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));

    UserGroupInformation.setConfiguration(conf);
    if (null != tokenFile) {
      String urlTokenString = FileUtils.readFileToString(new File(tokenFile));
      Token token = new Token();
      token.decodeFromUrlString(urlTokenString);
      UserGroupInformation.getCurrentUser().addToken(token);
    }

    System.out.println(conf.toString());

    connection = ConnectionFactory.createConnection(conf);
    if (null == tokenFile) {
      Token<AuthenticationTokenIdentifier> token = TokenUtil.obtainToken(connection);
      File tempFile = File.createTempFile("hbasetoken", ".token");
      FileUtils.write(tempFile, token.encodeToUrlString());
      System.out.println("Token file: " + tempFile.getAbsolutePath());
    }
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
    if (args.length < 2) {
      System.err.printf("Usage: %s <conf> <table> [<tokenfile>]\n", HBaseTokenClient.class);
      System.exit(-1);
    }
    String tokenFile = null;
    if (args.length == 3) {
      tokenFile = args[2];
    }

    HBaseTokenClient exampleJavaClient = new HBaseTokenClient(args [0], args[1], tokenFile);
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

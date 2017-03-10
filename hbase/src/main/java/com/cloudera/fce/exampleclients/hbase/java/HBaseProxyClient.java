package com.cloudera.fce.exampleclients.hbase.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class HBaseProxyClient {

    private ConcurrentHashMap<String, Connection> connectionCache = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, User> userCache = new ConcurrentHashMap<>();

    private String user;
    private Configuration conf;
    private UserGroupInformation realUser;

    public HBaseProxyClient(String clientConfig, String keytabLocation, String user) throws
        IOException {
        this.user = user;

        conf = HBaseConfiguration.create();
        conf.addResource(new Path(clientConfig));

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(user, keytabLocation);
        conf.set("hbase.security.authentication", "kerberos");
        realUser = UserGroupInformation.getCurrentUser();
    }

    public Connection getConnection(String doAsUser) throws IOException {

        if (!connectionCache.contains(doAsUser)) {
            UserGroupInformation ugi = realUser;
            if (!doAsUser.equals(user)) {
                System.out.printf("Creating proxy user %s with real user %s\n", doAsUser, user);
                ugi = UserGroupInformation.createProxyUser(doAsUser, realUser);
            }

            if (!userCache.contains(doAsUser)) {
                userCache.put(doAsUser, User.create(ugi));
            }

            connectionCache.put(doAsUser,
                ConnectionFactory.createConnection(conf, userCache.get(doAsUser)));
        }

        return connectionCache.get(doAsUser);
    }

    public void doScanAs(final String asUser, final String tableName) throws IOException, InterruptedException {

        Table table = getConnection(asUser).getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        long now = System.currentTimeMillis();
        System.out.println("Starting scan");
        for (Result res : scanner) {
            System.out.println(res);
        }
        System.out.printf("Scan finished: %d ms\n\n", System.currentTimeMillis() - now);
        table.close();

    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.printf("Usage: %s <conf> <keytab> <user> <doAsUser> <table>\n",
                HBaseProxyClient.class);
            System.exit(-1);
        }

        HBaseProxyClient proxyClient = new HBaseProxyClient(args[0], args[1], args[2]);
        System.out.printf("Scanning as real user: %s\n", args[2]);
        proxyClient.doScanAs(args[2], args[4]);
        System.out.printf("Scanning as proxy user: %s\n", args[3]);
        proxyClient.doScanAs(args[3], args[4]);
    }

}

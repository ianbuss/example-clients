package com.cloudera.fce.exampleclients.hdfs.java;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HdfsProxyClient {

    private Configuration conf;

    public HdfsProxyClient() throws IOException {
        this(null, null);
    }

    public HdfsProxyClient(String keyTab, String user) throws IOException {
        // hdfs-site.xml and core-site.xml need to be on the classpath
        conf = new Configuration();
        if (keyTab != null) {
            // Not including Guava explicitly, but it gets pulled in so many times
            Preconditions.checkNotNull(user);

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(user, keyTab);
        }
    }

    public void lsAs(String user, final String path) throws IOException, InterruptedException {
        UserGroupInformation asUser = UserGroupInformation.createProxyUser(user,
            UserGroupInformation.getCurrentUser());
        asUser.doAs(new PrivilegedExceptionAction<Object>() {
            @Override
            public Object run() throws Exception {
                FileSystem fs = FileSystem.get(conf);
                FileStatus[] fileStatuses = fs.listStatus(new Path(path));
                for (FileStatus fileStatus : fileStatuses) {
                    System.out.printf("%s\t%s:%s\t%s\n",
                        fileStatus.getPermission().toString(),
                        fileStatus.getOwner(),
                        fileStatus.getGroup(),
                        fileStatus.getPath().getName());
                }
                return null;
            }
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length == 3 || args.length == 0) {
            System.err.printf("Usage: %s [<keytab>] [<user>] <path> <asUser>\n", HdfsProxyClient.class.getName());
            System.exit(-1);
        }

        HdfsProxyClient hdfsClient;
        String path;
        String asUser;
        if (args.length > 2) {
            hdfsClient = new HdfsProxyClient(args[0], args[1]);
            path = args[2];
            asUser = args[3];
        } else {
            hdfsClient = new HdfsProxyClient();
            path = args[0];
            asUser = args[1];
        }

        hdfsClient.lsAs(asUser, path);
    }

}

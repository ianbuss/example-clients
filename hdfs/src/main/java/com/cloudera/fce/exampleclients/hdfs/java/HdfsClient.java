package com.cloudera.fce.exampleclients.hdfs.java;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

public class HdfsClient {

    private FileSystem fs;

    public HdfsClient() throws IOException {
        this(null, null);
    }

    public HdfsClient(String keyTab, String user) throws IOException {
        Configuration conf = new Configuration();
        if (keyTab != null) {
            // Not including Guava explicitly, but it gets pulled in so many times
            Preconditions.checkNotNull(user);

            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab(user, keyTab);
        }

        fs = FileSystem.get(conf);
    }

    public void ls(String path) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.printf("%s\t%s:%s\t%s\n",
                fileStatus.getPermission().toString(),
                fileStatus.getOwner(),
                fileStatus.getGroup(),
                fileStatus.getPath().getName());
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 2 || args.length == 0) {
            System.err.printf("Usage: %s [<keytab>] [<user>] <path>\n", HdfsClient.class.getName());
            System.exit(-1);
        }

        HdfsClient hdfsClient;
        String path;
        if (args.length > 1) {
            hdfsClient = new HdfsClient(args[0], args[1]);
            path = args[2];
        } else {
            hdfsClient = new HdfsClient();
            path = args[0];
        }

        hdfsClient.ls(path);
    }

}

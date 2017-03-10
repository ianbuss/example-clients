package com.cloudera.fce.exampleclients.hdfs.java;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;
import java.io.IOException;

public class HdfsTokenClient {

    private Configuration conf;

    public HdfsTokenClient(String tokenFile) throws IOException {
        // hdfs-site.xml and core-site.xml need to be on the classpath
        conf = new Configuration();
        UserGroupInformation.setConfiguration(conf);
        Credentials creds = Credentials.readTokenStorageFile(new File(tokenFile), conf);
        UserGroupInformation.getCurrentUser().addCredentials(creds);
    }

    public void ls(String path) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(path));
        for (FileStatus fileStatus : fileStatuses) {
            System.out.printf("%s\t%s:%s\t%s\n",
                fileStatus.getPermission().toString(),
                fileStatus.getOwner(),
                fileStatus.getGroup(),
                fileStatus.getPath().getName());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.err.printf("Usage: %s <token> <path>\n", HdfsTokenClient.class.getName());
            System.exit(-1);
        }

        HdfsTokenClient hdfsClient;
        hdfsClient = new HdfsTokenClient(args[0]);
        hdfsClient.ls(args[1]);
    }

}

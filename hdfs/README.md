# HDFS Clients

This module has some very simple examples of using HDFS clients against secure clusters using Kerberos.

* `HdfsClient`: this class will use the ambient Kerberos ticket cache or the supplied keytab to do a simple HDFS listing
* `HdfsProxyClient`: this class shows using impersonation/proxying to do the listing as another user (requires the Kerberos user to have proxy rights in the HDFS service)
* `HdfsTokenClient`: demonstrates reading in a delegation token from a file and using that to run a listing

## Delegation tokens at the command line

Tokens can be obtained for HDFS either programmatically or very simply using the command line:

```
$ kinit user1
$ hdfs fetchdt dt.token
$ kdestroy
$ HADOOP_TOKEN_FILE_LOCATION=dt.token hdfs dfs -ls /
```

An programmatic example of fetching an HDFS DT can be found by looking at the `fetchdt` [implementation](https://github.com/cloudera/hadoop-common/blob/cdh5-2.6.0_5.16.1/hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop/hdfs/tools/DelegationTokenFetcher.java)


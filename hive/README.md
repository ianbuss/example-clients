# Example Impala clients

Simple example Hive clients showing how to access secure Hive programmatically via JDBC/ODBC.

## Java

Java clients require either the supplied Apache driver or the Cloudera Simba driver to communicate 
over JDBC with Hive.

### Apache Driver

This example is a simple command line query runner with a sample invocation to a Kerberised 
HiveServer2 as follows. Connection to SSL protected HiveServer2 not supported with the Apache driver
in CDH <= 5.5.

```bash
# Get jars from CDH Hive lib directory
export APACHE_HIVE_CP=$(for j in /opt/cloudera/parcels/CDH/lib/hive/lib/*.jar; do echo -n "$j:"; done | sed 's/:$//')

# Run simple command line client
java -cp $SIMBA_IMPALA_CP:example-clients-common-1.0.0-SNAPSHOT.jar:example-clients-hive-1.0.0-SNAPSHOT.jar \
  com.cloudera.fce.exampleclients.hive.java.HiveJdbcClient \
  -h cs-vanilla.ib.dev \
  -q "select * from sample_08 limit 10" \
  -k -t vagrant.keytab -u vagrant
```

### Simba Driver

This example is a simple command line query runner with a sample invocation to a Kerberised 
and SSL protected HiveServer2 as follows:

```bash
# Get jars from Simba JDBC distribution v2.5.15, here extracted into a HiveJDBC41 directory
export SIMBA_HIVE_CP=$(for j in HiveJDBC41/*.jar; do echo -n "$j:"; done | sed 's/:$//')

# Run simple command line client
java -cp $SIMBA_HIVE_CP:example-clients-common-1.0.0-SNAPSHOT.jar:example-clients-hive-1.0.0-SNAPSHOT.jar \
  com.cloudera.fce.exampleclients.hive.java.HiveJdbcClient \
  -h cs-vanilla.ib.dev \
  -q "select * from sample_08 limit 10" \
  -k -t vagrant.keytab -u vagrant \
  -St /opt/cloudera/security/public/truststore.jks -Sp hadoop
```


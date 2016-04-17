# Example Impala clients

Example JDBC/ODBC Impala clients

## Java

Java clients require the Simba driver to communicate over JDBC with Impala. This example is a simple command line query
runner with a sample invocation to a Kerberised and SSL protected ImpalaD as follows:

```bash
# Get jars from Simba JDBC distribution v2.5.28
export SIMBA_IMPALA_CP=$(for j in ImpalaJDBC41/*.jar; do echo -n "$j:"; done | sed 's/:$//')

# Run simple command line client
java -cp $SIMBA_IMPALA_CP:example-clients-common-1.0.0-SNAPSHOT.jar:example-clients-impala-1.0.0-SNAPSHOT.jar \
  com.cloudera.fce.exampleclients.impala.java.ImpalaJdbcClient \
  -h cs-vanilla.ib.dev \
  -q "select * from sample_08 limit 10" \
  -k -t vagrant.keytab -u vagrant \
  -St /opt/cloudera/security/public/truststore.jks -Sp hadoop
```


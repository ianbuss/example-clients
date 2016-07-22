# Example Impala clients

Example JDBC/ODBC Impala clients

## Java

This example is a simple command line query runner with support for connecting to a TLS protected ImpalaD 
with Kerberos or LDAP authentication. Java clients require the Simba driver to communicate over JDBC with Impala. 

## Usage

```
Usage:com.cloudera.fce.exampleclients.impala.java.ImpalaJdbcClient
	-h HOST
	-q QUERY
	[-d DATABASE] [-p PORT]
	[-s SERVER_PRINC] [-k] [-t KEYTAB] [-u USER_PRINC] [-r REALM]
	[-j JAAS_FILE]
	[-user LDAP_UID] [-pass LDAP_PASSWD]
	[-St SSL_TRUSTSTORE] [-Sp SSL_TRUSTSTORE_PASS]
	[-debug]
```


```bash
# Get jars from Simba JDBC distribution v2.5.28
export SIMBA_IMPALA_CP=$(for j in ImpalaJDBC41/*.jar; do echo -n "$j:"; done | sed 's/:$//')

# Run simple command line client (Kerberos)
java -cp $SIMBA_IMPALA_CP:example-clients-common-1.0.0-SNAPSHOT.jar:example-clients-impala-1.0.0-SNAPSHOT.jar \
  com.cloudera.fce.exampleclients.impala.java.ImpalaJdbcClient \
  -h impala.host.com \
  -q "select * from sample_08 limit 10" \
  -k -t vagrant.keytab -u vagrant \
  -St /opt/cloudera/security/public/truststore.jks -Sp hadoop
  
# Run simple command line client (LDAP)
java -cp $SIMBA_IMPALA_CP:example-clients-common-1.0.0-SNAPSHOT.jar:example-clients-impala-1.0.0-SNAPSHOT.jar \
  com.cloudera.fce.exampleclients.impala.java.ImpalaJdbcClient \
  -h impala.host.com \
  -q "select * from sample_08 limit 10" \
  -user vagrant -pass vagrant \
  -St /opt/cloudera/security/public/truststore.jks -Sp hadoop
```

**Tip:** use the `-debug` option to print the full JDBC connection string used to establish the connection to Impala 


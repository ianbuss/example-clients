package com.cloudera.fce.exampleclients.hive.java;

import com.cloudera.fce.exampleclients.common.AbstractJdbcDriver;
import com.cloudera.fce.exampleclients.common.JdbcDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.UUID;

public class SimbaHiveDriver extends AbstractJdbcDriver {

  private static final Logger LOG = LoggerFactory.getLogger(SimbaHiveDriver.class);

  private String jaasFile = null;

  public String getName() {
    return "com.cloudera.hive.jdbc41.HS2Driver";
  }

  @Override
  public String constructJdbcUrl() {
    // Better error detection for production
    String url = String.format("jdbc:hive://%s:%d", host, port);
    if (isLDAP) {
      url += String.format(";AuthMech=3;transportMode=sasl;UID=%s;PWD=%s",
          userName, password);
    } else if (isKerberos) {
      url += String.format(";AuthMech=1;KrbRealm=%s;KrbHostFQDN=%s;KrbServiceName=%s",
          krbRealm, host, serverPrinc);
    } else {
      // No auth
      url += ";AuthMech=0";
    }
    if (isSSL) {
      url += String.format(";SSL=1;SSLTrustStore=%s;SSLTrustStorePwd=%s",
          trustStore, trustStorePassword);
    }
    if (doAsUser != null) {
      url += String.format(";DelegationUID=%s", doAsUser);
    }
    LOG.debug(url);
    return url;
  }

  @Override
  public JdbcDriver withImpersonation(String doAsUser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void loginViaJaas(String configFile) {
    System.setProperty("java.security.auth.login.config", configFile);
    System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
  }

  @Override
  public void loginViaKeytab(String userPrinc, String keyTab) throws IOException {
    jaasFile = "/tmp/" + UUID.randomUUID().toString();
    System.out.println("Writing JAAS config to: " + jaasFile);
    PrintWriter out = new PrintWriter(new BufferedOutputStream(new FileOutputStream(jaasFile)));
    out.write("Client {\n");
    out.write(" com.sun.security.auth.module.Krb5LoginModule required\n");
    out.write(" useKeyTab=true\n");
    out.write(" keyTab=" + keyTab + "\n");
    out.write(" doNotPrompt=true\n");
    out.write(" principal=" + userPrinc + ";\n");
    out.write("};\n");
    out.close();
    loginViaJaas(jaasFile);
  }

  @Override
  public void cleanup() {
    if (null != jaasFile) {
      new File(jaasFile).delete();
    }
  }
}

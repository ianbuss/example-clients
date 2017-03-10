package com.cloudera.fce.exampleclients.common;

public abstract class AbstractJdbcDriver implements JdbcDriver {

    // Connection parameters
    protected String host;
    protected int port;
    protected String db = "default";
    protected String serverPrinc;
    protected String krbRealm;
    protected String userName;
    protected String password;
    protected String trustStore;
    protected String trustStorePassword;
    protected String doAsUser;

    protected boolean isKerberos = false;
    protected boolean isLDAP = false;
    protected boolean isSSL = false;

    @Override
    public JdbcDriver withHostPort(String host, int port) {
        this.host = host;
        this.port = port;
        return this;
    }

    @Override
    public JdbcDriver withDatabase(String database) {
        this.db = database;
        return this;
    }

    @Override
    public JdbcDriver withKerberos(String serverPrincipal, String kerberosRealm) {
        this.serverPrinc = serverPrincipal;
        this.krbRealm = kerberosRealm;
        this.isKerberos = true;
        return this;
    }

    @Override
    public JdbcDriver withLDAP(String user, String pass) {
        this.userName = user;
        this.password = pass;
        this.isLDAP = true;
        return this;
    }

    @Override
    public JdbcDriver withSSL(String trustStore, String trustStorePassword) {
        this.trustStore = trustStore;
        this.trustStorePassword = trustStorePassword;
        this.isSSL = true;
        return this;
    }

    @Override
    public JdbcDriver withImpersonation(String doAsUser) {
        this.doAsUser = doAsUser;
        return this;
    }

}

package net.rubywillow.ora2pgdmp;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.FileReader;

public class Config {

  private static final String OUTFILE = "outfile";
  private static final String SESSIONS = "sessions";
  private static final String TRUNCATE = "truncate";
  private static final String ORADB = "oradb";
  private static final String ORAUSERNAME = "orausername";
  private static final String ORAPASSWORD = "orapassword";
  private static final String PGHOST = "pghost";
  private static final String PGDB = "pgdb";
  private static final String PGUSERNAME = "pgusername";
  private static final String PGPASSWORD = "pgpassword";
  private static final String PGPORT = "pgport";
  private static final String WORK = "work";
  private static final String QUERY = "query";
  private static final String TARGET = "target";

  private String goutFile = "work";
  private String gpghost;
  private String gpgport;
  private String gpgdb;
  private String gpgusername;
  private String gpgpassword;

  private int numSessions = 7;

  public int getNumSessions() {
    return numSessions;
  }

  class Individual {
    private String outFile;
    private boolean truncate;
    private String oradb;
    private String orausername;
    private String orapassword;
    private String pghost;
    private String pgport;
    private String pgdb;
    private String pgusername;
    private String pgpassword;
    private String query;
    private String target;

    public String getOutFile() {
      return outFile;
    }

    public boolean isTruncate() {
      return truncate;
    }

    public String getOradb() {
      return oradb;
    }

    public String getOrausername() {
      return orausername;
    }

    public String getOrapassword() {
      return orapassword;
    }

    public String getPghost() {
      return pghost;
    }

    public String getPgport() {
      return pgport;
    }

    public String getPgdb() {
      return pgdb;
    }

    public String getPgusername() {
      return pgusername;
    }

    public String getPgpassword() {
      return pgpassword;
    }

    public String getQuery() {
      return query;
    }

    public String getTarget() {
      return target;
    }

  }

  private Individual[] indcfgs;

  public Individual[] getIndcfgs() {
    return indcfgs;
  }

  public Config( String configFile ) throws Exception {
    super();

    JsonObject j = (JsonObject) new JsonParser().parse( new FileReader( configFile ) );

    if ( !j.has( OUTFILE ) ||
        !j.has( ORADB ) ||
        !j.has( ORAUSERNAME ) ||
        !j.has( ORAPASSWORD ) ||
        !j.has( WORK ) )
      throw new Exception( "Must provide \"outfile\", \"oradb\", \"orausername\", \"orapassword\", and \"work\" parameters in your config file" );

    this.goutFile = j.getAsJsonPrimitive( OUTFILE ).getAsString();
    final String goradb = j.getAsJsonPrimitive( ORADB ).getAsString();
    final String gorausername = j.getAsJsonPrimitive( ORAUSERNAME ).getAsString();
    final String gorapassword = j.getAsJsonPrimitive( ORAPASSWORD ).getAsString();

    boolean gtruncate = false;
    if ( j.has( TRUNCATE ) )
      gtruncate = j.getAsJsonPrimitive( TRUNCATE ).getAsBoolean();

    if ( j.has( PGHOST ) )
      this.gpghost = j.getAsJsonPrimitive( PGHOST ).getAsString();

    if ( j.has( PGDB ) )
      this.gpgdb = j.getAsJsonPrimitive( PGDB ).getAsString();

    if ( j.has( PGPORT ) )
      this.gpgport = j.getAsJsonPrimitive( PGPORT ).getAsString();

    if ( j.has( PGUSERNAME ) )
      this.gpgusername = j.getAsJsonPrimitive( PGUSERNAME ).getAsString();

    if ( j.has( PGPASSWORD ) )
      this.gpgpassword = j.getAsJsonPrimitive( PGPASSWORD ).getAsString();

    if (j.has( SESSIONS ))
      this.numSessions = j.getAsJsonPrimitive( SESSIONS ).getAsInt();

    JsonArray works = j.getAsJsonArray( WORK );
    indcfgs = new Individual[ works.size() ];
    int ix = 0;

    for ( final JsonElement work1 : works ) {
      JsonObject work = work1.getAsJsonObject();
      Individual ind = new Individual();
      ind.truncate = work.has( TRUNCATE ) ? work.getAsJsonPrimitive( TRUNCATE ).getAsBoolean() : gtruncate;
      ind.oradb = work.has( ORADB ) ? work.getAsJsonPrimitive( ORADB ).getAsString() : goradb;
      ind.orausername = work.has( ORAUSERNAME ) ? work.getAsJsonPrimitive( ORAUSERNAME ).getAsString() : gorausername;
      ind.orapassword = work.has( ORAPASSWORD ) ? work.getAsJsonPrimitive( ORAPASSWORD ).getAsString() : gorapassword;
      ind.pghost = work.has( PGHOST ) ? work.getAsJsonPrimitive( PGHOST ).getAsString() : this.gpghost;
      ind.pgport = work.has( PGPORT ) ? work.getAsJsonPrimitive( PGPORT ).getAsString() : this.gpgport;
      ind.pgdb = work.has( PGDB ) ? work.getAsJsonPrimitive( PGDB ).getAsString() : this.gpgdb;
      ind.pgusername = work.has( PGUSERNAME ) ? work.getAsJsonPrimitive( PGUSERNAME ).getAsString() : this.gpgusername;
      ind.pgpassword = work.has( PGPASSWORD ) ? work.getAsJsonPrimitive( PGPASSWORD ).getAsString() : this.gpgpassword;
      ind.query = work.getAsJsonPrimitive( QUERY ).getAsString();
      ind.target = work.getAsJsonPrimitive( TARGET ).getAsString();
      ind.outFile = work.getAsJsonPrimitive( OUTFILE ).getAsString();
      indcfgs[ix++] = ind;
    }

  }

  public String getOutFile() {
    return goutFile;
  }

}

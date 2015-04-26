package net.rubywillow.ora2pgdmp;


import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleStatement;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.io.*;
import java.sql.*;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

public class Job implements Runnable {

  /*
      this class is instantiated for each table export. It is essentially
      a stand-alone program that connects to the database, runs the query,
      formats the output as a PostgreSQL script, and writes the output
      to a compressed (gzip) file.
  */

  private final static String fmt = "%02d %02d:%02d:%02d.%03d";
  private OracleConnection conn;
  DateTime startTime;
  DateTime endTime;
  private int rowCount = 0;
  private BlockingQueue<Future<Integer>> results;
  final private ExecutorService fmtThreadPool = Executors.newFixedThreadPool( 8 );
  private Config.Individual cfg;
  private Writer writer;
  private ReentrantLock lock = new ReentrantLock();
  private boolean stopFlag = false;

  public Job( final Config.Individual jobConfig ) {
    // individual configuration for this job
    cfg = jobConfig;
  }

  @Override
  public void run() {
    try {
      final StringBuilder sb = new StringBuilder();
      Properties props = new Properties();
      props.setProperty( "user", cfg.getOrausername() );
      props.setProperty( "password", cfg.getOrapassword() );
      
      conn = (OracleConnection) DriverManager.getConnection( "jdbc:oracle:thin:@" + cfg.getOradb(), props );
      try {
        conn.setAutoCommit( false );

        // set output string formats for dates and timestamps
        conn.createStatement().execute( "begin\n" +
            "  execute immediate q'[alter session set nls_date_format='YYYY-MM-DD HH24:MI:SS']';\n" +
            "  execute immediate q'[alter session set nls_timestamp_format='YYYY-MM-DD HH24:MI:SS.FF6']';\n" +
            "  execute immediate q'[alter session set nls_timestamp_tz_format='YYYY-MM-DD HH24:MI:SS.FF6TZH:TZM']';\n" +
            "end;" );
        System.out.println();
        System.out.println( "Starting job for: " + cfg.getTarget() );

        // the bulk of the work is here
        step2();

        // let user know we finished this job
        sb.setLength( 0 );
        sb.append( "Finished job for: " );
        sb.append( cfg.getTarget() );
        sb.append( "  :: " );
        sb.append( rowCount );
        sb.append( " records in " );
        Duration d = new Duration(startTime, endTime);
        Period p = d.toPeriod();
        sb.append( String.format( fmt, p.getDays(), p.getHours(), p.getMinutes(), p.getSeconds(), p.getMillis() ) );

        long ms = d.getMillis();
        if ( ms == 0 )
          ms = 1;
        sb.append( "  :: " );
        sb.append( ( (long) rowCount * 1000L ) / ms );
        sb.append( " records per second" );

        System.out.println();
        System.out.println( sb.toString() );
      } finally {
        conn.close();
        fmtThreadPool.shutdown();
        fmtThreadPool.awaitTermination( Long.MAX_VALUE, TimeUnit.MILLISECONDS );
      }

    } catch ( Exception e ) {
      e.printStackTrace();
    }
  }

  private void step2() throws Exception {

    // setup the output (work) file
    final File workFile = new File( cfg.getOutFile() + ".sql.gz.work" );
    if ( workFile.exists() )
      workFile.delete();

    // buffer > writer > gzip > buffer > file (double buffering does make a difference)
    writer = new BufferedWriter( new OutputStreamWriter( new GZIPOutputStream( new BufferedOutputStream( new FileOutputStream( workFile ) ) ) ) );

    // assemble some of the file header (continued on step3)
    writer.write( "\n-- output file from OracleExp-PostgresImp --\n\n" );
    writer.write( "SET statement_timeout = 0;\nSET lock_timeout = 0;\nSET client_encoding = 'UTF8';\nSET standard_conforming_strings = on;\n\n" );
    writer.write( "\\echo -n Loading " );
    writer.write( cfg.getTarget() );
    writer.write( " ...\nbegin;\n" );
    if ( cfg.isTruncate() ) {
      writer.write( "TRUNCATE TABLE " );
      writer.write( cfg.getTarget() );
      writer.write( ";\n" );
    }

    // run the query...
    step3();

    lock.lock();
    try {
      writer.write( "\\.\ncommit;\n\\echo . done\n" );
      writer.flush();
      writer.close();
    } finally {
      lock.unlock();
    }

    endTime = new DateTime();

    // rename work file to final filename
    final File outFile = new File( cfg.getOutFile() + ".sql.gz" );

    if ( outFile.exists() )
      outFile.delete();

    workFile.renameTo( outFile );

  }

  private void step3() throws Exception {
    OracleStatement stmt = (OracleStatement) conn.createStatement();
    // get a large chunk of LOB data at a time
    stmt.setLobPrefetchSize( 32768 );
    // get a good chunk of row data at a time
    stmt.setFetchSize( 250 );
    // maybe not necessary, but...
    stmt.setRowPrefetch( 250 );

    startTime = new DateTime();
    ResultSet rs = stmt.executeQuery( cfg.getQuery() );

    // metadata used to specify the column names
    // for the copy command
    ResultSetMetaData rsmd = rs.getMetaData();
    int colCount = rsmd.getColumnCount();
    String comma = null;

    writer.write( "COPY " );
    writer.write( cfg.getTarget() );
    writer.write( " (" );
    for ( int i = 1; i <= colCount; i++ ) {
      if ( comma == null )
        comma = ", ";
      else
        writer.write( comma );

      writer.write( rsmd.getColumnLabel( i ) );
    }

    writer.write( ") FROM stdin;\n" );

    // 25k queued messages is probably quite sufficient.
    results = new LinkedBlockingQueue<Future<Integer>>( 25000 );

    // fire off a thread to consume the results queue
    // the only thing this does is indicate when the queue is
    // finished processing. (It gets the number 1)
    final Timer tmr = new Timer();
    tmr.schedule( new TimerTask() {
      @Override
      public void run() {

        int y = 0;
        do {
          try {
            // take() blocks until a message is available
            y = results.take().get();
          } catch ( InterruptedException e ) {
            e.printStackTrace();
          } catch ( ExecutionException e ) {
            e.printStackTrace();
          }
        } while ( y != 1 );
        stopFlag = true;
      }
    }, 100 );

    // process rows; gather the column values into
    // the column array, and pass the writer, array, and semaphore
    // to the formatter-thread-pool. The only reason for the
    // results queue is to determine that we are done.
    while ( rs.next() ) {
      rowCount++;
      Object[] cols = new Object[colCount];
      for ( int i = 1, j = 0; j < colCount; i++, j++ ) {
        cols[j] = rs.getObject( i );
      }
      results.put( fmtThreadPool.submit( new Formatter( writer, cols, lock ) ) );
    }

    // indicate we are done with NULL values
    results.put( fmtThreadPool.submit( new Formatter( null, null, null ) ) );

    // wait for the results to be consumed.
    while ( !stopFlag ) {
      Thread.sleep( 100 );
    }

    tmr.cancel();

  }

}

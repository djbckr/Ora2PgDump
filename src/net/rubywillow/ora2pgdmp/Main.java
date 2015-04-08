package net.rubywillow.ora2pgdmp;
import java.io.File;
import java.io.FileWriter;
import java.io.OutputStreamWriter;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Main {

    public static AtomicLong recCount = new AtomicLong(  );
    private long lastRecCount = 0;
    private static final String format = "%,12d";

    public Main() {
        Timer t = new Timer();
        t.schedule( new TimerTask() {
            @Override
            public void run() {
                long tmp = recCount.get();
                System.out.print( String.format( format, tmp-lastRecCount )  + "\r");
                lastRecCount = tmp;
            }
        }, 1000, 1000 );
    }

    public static void main( String[] args ) throws Exception {
        new Main().run( args );
    }

    private void run( String[] args ) throws Exception {
        // load configuration
        Config cfg = new Config( args[0] );

        // start thread pools
        final ExecutorService queryThreads = Executors.newFixedThreadPool( cfg.getNumSessions() );

        StringBuilder sb = new StringBuilder();

        sb.append( "#!/usr/bin/env sh\n" );

        for ( int i = 0; i < cfg.getIndcfgs().length; i++ ) {
            Config.Individual icfg = cfg.getIndcfgs()[i];

            // create job and pass it to the thread pool
            queryThreads.execute( new Job( icfg ) );

            // create shell script item for this job
            if ( icfg.getPgpassword() != null ) {
                sb.append( "PGPASSWORD=\"" );
                sb.append( icfg.getPgpassword() );
                sb.append( "\"\n" );
            }
            sb.append( "gunzip -c " );
            sb.append( icfg.getOutFile() );
            sb.append( ".sql.gz | psql --quiet" );
            if ( icfg.getPghost() != null ) {
                sb.append( " --host=" );
                sb.append( icfg.getPghost() );
            }
            if ( icfg.getPgdb() != null ) {
                sb.append( " --dbname=" );
                sb.append( icfg.getPgdb() );
            }
            if ( icfg.getPgport() != null ) {
                sb.append( " --port=" );
                sb.append( icfg.getPgport() );
            }
            if ( icfg.getPgusername() != null ) {
                sb.append( " --username=" );
                sb.append( icfg.getPgusername() );
            }
            sb.append( "\n" );

            Thread.sleep( 250 );
        }

        // write script file
        File f = new File( cfg.getOutFile() + ".sh" );
        OutputStreamWriter osw = new FileWriter( f );
        osw.write( sb.toString() );
        osw.flush();
        osw.close();
        f.setExecutable( true );

        // wait for queries to finish
        queryThreads.shutdown();
        queryThreads.awaitTermination( Long.MAX_VALUE, TimeUnit.MILLISECONDS );

        System.out.println();
        System.out.println( "--- program complete ---" );

        // kill this program, just in case something got mangled
        System.exit( 0 );
    }

}

Oracle to PostgreSQL Dump
=========================

This utility came from a need to convert Oracle data to PostgreSQL data in a reasonable amount
of speed. This project is rather utilitarian, but it's a start.

  - It does not create tables; the target table must already exist.
  - The target column names must match the query column names (column order doesn't matter)
  - The data types of each column must be compatible.

Here are some data type conversions that are allowed (FROM -> to):

  - VARCHAR2 -> text / varchar / char
  - CHAR -> text / varchar / char
  - DATE -> timestamp without time zone / date
  - TIMESTAMP -> timestamp without time zone
  - TIMESTAMP WITH TIME ZONE -> timestamp with time zone
  - NUMBER -> decimal / integer / smallint (provided the values fit)
  - RAW -> bytea
  - CLOB -> text
  - BLOB -> bytea
  - TYPE/OBJECT -> unsupported
  - COLLECTION -> unsupported (for now)

As you can see, this utility does not copy Oracle LOBs to PostgreSQL LOBs, but rather CLOB->text and BLOB->bytea.
If you are copying very large LOB data, you may run into issues. I've tested with moderate sized CLOBs up to about
a megabyte and everything seemed fine.

I spent a great deal of effort to make this utility run as fast as possible. For some tables with narrow rows, I saw a
throughput of over 20k records per second, which is pretty fast given it's not the Oracle export or data-pump utility.
Tables with LOB columns are always slow, but I have managed to get over 1000 records per second on a table with two
CLOB columns. I believe this is even faster than the Oracle export utility. It should be obvious, but the closer
you are to the database, the faster this utility will run. If you can, you can run it directly on the server where the
database is and use the JDBC bequeath connection by specifying an empty string for the "oradb" configuration.
The numbers above are over a fast gigabit network where the server/client were right next to each other.
Running on the server directly produced similar results.

How it works
------------

This utility connects to an Oracle database, SELECTs data from a view/table, and writes the data to a PostgreSQL
database dump file. This file in turn can be read by the psql utility to import the data into Postgres. It is
important to note that the dump file is compressed in gzip format - more on this later. Another file that is generated
is a shell script that can be run on any Posix system that has gunzip and psql.

There are a couple of reasons this utility doesn't directly copy from Oracle to PostgreSQL, but the primary
reason is speed. Importing a dump file using psql is far faster than using anything else I have found. In addition,
the gzip format helps in a couple of ways: Firstly, the file(s) are much smaller, sometimes by as much as 97% compression.
This significantly speeds up delivery over a WAN. Secondly, I have found that the gunzip utility pipes the data faster
than reading uncompressed data from disk.

Configuration
-------------

You specify a configuration file in JSON format. An example configuration is shown below:

    {
      "sessions": 7,
      "outfile": "myoutfile",
      "truncate": true,
      "oradb": "//server-name/database-name",
      "orausername": "theusername",
      "orapassword": "thepassword",
      "pghost": "server-name",
      "pgdb": "database-name",
      "pgusername": "theusername",
      "pgpassword": "thepassword",
      "work": [
        {
          "query": "select field1, field2, field3 from table_or_view1",
          "target": "theschema.table1",
          "outfile": "filename1"
        },
        {
          "query": "select field1, field2, field3 from table_or_view2",
          "target": "theschema.table2",
          "outfile": "filename2"
        }
      ]
    }

  - sessions (optional): The number of parallel queries you want to have running at the same time. Unless you have a
super-powerful server, I would keep this around 7 or less. Default is 7.
  - outfile (required): The name of the shell script that you can run once the export has finished. It automatically
gets a ".sh" extension and is marked executable on a Posix system.
  - truncate (optional): Issues a truncate command to the target table before import. If you can use this, this
_greatly_ speeds up the import process because it bypasses the use of the PostreSQL transaction log. Default is false.
  - oradb (required): This is a JDBC URL. Typically the style shown above is sufficient.
For a bequeath connection, specify the empty string "".
  - orausername (required): The username/schema you are connecting to.
  - orapassword (required): The password of the Oracle username you are connecting to.
  - pghost (optional): The server name where your Postgres database (cluster) is. This is put into your shell script.
  - pgdb (optional): The database name of your Postgres cluster. This is put into your shell script.
  - pgusername (optional): The username used to connect to your Postgres cluster. This is put into your shell script.
  - pgpassword (optional): The password for your pgusername. This is put into your shell script.
  - work (required): An array of objects that defines each job/table to export.
    - query (required): The SQL query to run on your Oracle database. This can be a view or a complex query.
    - target (required): The target table where the data will be loaded in your Postgres database.
    - outfile (required): The name of the file used for output. This file automatically gets a ".sql.gz" extension.
    - All of the main parameters (except sessions and outfile) may be specified for each individual work object if
you need to work with more than one database at a time.

Running the utility
-------------------

To run this utility, execute as follows:

    java -jar Ora2PgDump.jar myConfigFile.json

You will see minimal logging about items that start and finish. The numbers that keep flashing are approximately the
number of records processed per second. If you need more memory, do something like:

    java -Xmx1024m -jar Ora2PgDump.jar myConfigFile.json

This command will allocate 1GB of RAM to the JVM. You likely do not need this much unless you are running many sessions at once.

Importing into PostgreSQL
-------------------------

Simply run the shell script:

    ./myoutfile.sh

Or you can look at the shell commands inside that file to see how to import individual files, or if you have a huge
amount of data, you can split the script into multiple files and execute loads in parallel.

Really huge tables
------------------

I attempted to find ways to retrieve data from a query using multiple threads but this is not easily done. Each table
gets one main thread, and 8 processing threads. (see "One Last Thing" below)

If you still find this utility is too slow, or the output files are too large, you can make multiple work items
in your configuration that selects partial data from a single table (`where pri_key between x and y`) and outputs
that into multiple files. Be certain you specify a different "outfile" for each work item.
If not, data *will* get overwritten.

Building your own version
-------------------------

This project is mostly complete, but you'll need to build it for your environment. This requires the following libraries:

  - gson 2.2.4 (chances are likely that more recent versions work fine)
  - joda-time 2.4 (chances are likely that more recent versions work fine)
  - ojdbc6 (Oracle JDBC 6)

Once you get your IDE setup, it's just a matter of building your own JAR to execute.

One last thing
--------------

Each record that is read by this utility is processed in a thread, and streamed to the dump file as the formatting
is completed. This means that the data in the dump file could be in a different order than it was read. This
should not be an issue whatsoever, but it's just something to know in case you think the data should be in some
particular order. While each job runs in its own main thread, each job generates 8 threads (seems to be the sweet spot)
for record processing and streaming.

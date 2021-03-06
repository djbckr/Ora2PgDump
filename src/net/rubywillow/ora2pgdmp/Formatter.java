package net.rubywillow.ora2pgdmp;

import oracle.sql.ANYDATA;
import oracle.sql.BLOB;

import java.io.*;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/*
    This class is instantiated once per record. Once it acquires the file lock,
    it must keep the lock until the record is finished processing. For short-ish records,
    the lock will only be acquired at the time the record is flushed, then immediately
    released. For longer records, we may acquire the lock before we're done with the record.
    In this case it could block other formatter threads trying to write records,
    but there's nothing to be done about that.
*/
public class Formatter implements Callable<Integer> {

    private static final char tabchar = 9;
    private static final char linefeed = 10;
    private static final char carriagereturn = 13;
    private static final char backslash = 92;

    // if a date comes back as a java.sql.Timestamp, use this date formatter
    private SimpleDateFormat sdf;

    // The file we write to. Writes must be synchronized using lock
    private Writer out;
    private ReentrantLock lock;

    // our main buffer
    private StringBuilder buf = new StringBuilder(1024);

    // the columns we are formatting
    private Object[] cols;

    private final static int TWOMEG = 2097152;
    private final static int ONEMEG = 1048576;

    public Formatter(Writer out_, Object[] cols_, ReentrantLock lock_) {
        out = out_;
        cols = cols_;
        lock = lock_;
    }

    @Override
    public Integer call() throws Exception {

        if (cols == null)
            // this is our indication that we aren't processing any more
            return 1;

        try {
            String tab = null;
            // iterate through the columns, putting a tab
            // character between them. (no tab at beginning or end)
            for (Object obj : cols) {

                if (tab != null)
                    buf.append(tab);
                else
                    tab = "\t";

                processObject(obj);
            }
            // new-line indicates end of record
            buf.append("\n");
            // push to file
            flushBuffer(true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        Main.recCount.incrementAndGet();
        return 0;
    }

    private void doFlush() throws Exception {
        out.write(buf.toString());
        buf.setLength(0);
    }

    private void flushBuffer(boolean force) throws Exception {

        // We *must* write now. Wait indefinitely for a lock.
        if (force) {
            if (!lock.isHeldByCurrentThread()) {
                lock.lock();
            }
            doFlush();
            return;
        }

        // If we aren't forcing, we'll attempt to get a lock but not try too hard.
        // 1: if we already have the lock OR
        // 2: get a lock if nothing else is holding the lock OR
        // 3: try to get the lock but only for 1 second if something else is holding.
        if (lock.isHeldByCurrentThread() || lock.tryLock() || lock.tryLock(1, TimeUnit.SECONDS)) {
            doFlush();
        }

    }

    private void checkFlush() throws Exception {

        int len = buf.length();

        if (len < ONEMEG) // don't need to flush yet
            return;

        if (len < TWOMEG) {
            // try to flush, but it's not critical yet
            flushBuffer(false);
            return;
        }

        // must flush; buffer getting too big now
        flushBuffer(true);
    }

    private void doWrite(final String str) throws Exception {
        buf.append(str);
        checkFlush();
    }

    private void doWrite(final char[] charArray) throws Exception {
        buf.append(charArray);
        checkFlush();
    }

    // this goes through all of the data types that we'll support. There
    // are more (like objects and collections) and maybe we'll support
    // collections in the future.
    private void processObject(Object obj) throws Exception {

        if (obj == null) {
            doWrite("\\N");
        } else if (obj instanceof java.lang.String) {
            escape((String) obj);
        } else if (obj instanceof oracle.sql.NUMBER) {
            doWrite(((oracle.sql.NUMBER) obj).stringValue());
        } else if (obj instanceof java.math.BigDecimal) {
            doWrite(((BigDecimal) obj).toPlainString());
        } else if (obj instanceof oracle.sql.CHAR) {
            escape(((oracle.sql.CHAR) obj).stringValue());
        } else if (obj instanceof oracle.sql.DATE) {
            doWrite(((oracle.sql.DATE) obj).stringValue());
        } else if (obj instanceof java.sql.Timestamp) {
            if (sdf == null)
                sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            doWrite(sdf.format((Timestamp) obj));
        } else if (obj instanceof oracle.sql.TIMESTAMP) {
            doWrite(((oracle.sql.TIMESTAMP) obj).stringValue());
        } else if (obj instanceof oracle.sql.TIMESTAMPTZ) {
            doWrite(((oracle.sql.TIMESTAMPTZ) obj).stringValue());
        } else if (obj instanceof oracle.sql.TIMESTAMPLTZ) {
            doWrite(((oracle.sql.TIMESTAMPLTZ) obj).stringValue());
        } else if (obj instanceof oracle.sql.INTERVALYM) {
            doWrite(((oracle.sql.INTERVALYM) obj).stringValue());
        } else if (obj instanceof oracle.sql.INTERVALDS) {
            doWrite(((oracle.sql.INTERVALDS) obj).stringValue());
        } else if (obj instanceof byte[]) {
            bytesToHex((byte[]) obj, true);
        } else if (obj instanceof oracle.sql.RAW) {
            bytesToHex(((oracle.sql.RAW) obj).getBytes(), true);
        } else if (obj instanceof java.lang.Float) {
            doWrite(new BigDecimal((Float) obj).toPlainString());
        } else if (obj instanceof java.lang.Double) {
            doWrite(new BigDecimal((Double) obj).toPlainString());
        } else if (obj instanceof oracle.sql.BINARY_DOUBLE) {
            doWrite(((oracle.sql.BINARY_DOUBLE) obj).bigDecimalValue().toPlainString());
        } else if (obj instanceof oracle.sql.BINARY_FLOAT) {
            doWrite(((oracle.sql.BINARY_FLOAT) obj).bigDecimalValue().toPlainString());
        } else if (obj instanceof oracle.sql.ANYDATA) {
            processObject(((ANYDATA) obj).accessDatum());
        } else if (obj instanceof oracle.sql.CLOB) {
            final Reader r = ((oracle.sql.CLOB) obj).getCharacterStream();

            char[] buf = new char[16384];
            int readAmount = r.read(buf);

            while (readAmount > 0) {
                escape(new String(buf, 0, readAmount));
                readAmount = r.read(buf);
            }
        } else if (obj instanceof oracle.sql.BLOB) {
            final InputStream r = ((BLOB) obj).getBinaryStream();

            byte[] buf = new byte[16384];
            int readAmount = r.read(buf);

            boolean head = true;
            while (readAmount > 0) {
                bytesToHex(buf, 0, readAmount, head);
                if (head)
                    head = false;
                readAmount = r.read(buf);
            }
        } else {
            throw new SQLException("An unsupported datatype was encountered: " + obj.getClass().getCanonicalName());
        }
    }

    private void escape(final String str) throws Exception {

        if (str.isEmpty())
            return;

        // PostgreSQL COPY data cannot have carriage returns, line-feeds, tab characters,
        // or single backslashes. These are all control characters.
        // replace these with "\r", "\n", "\t", "\\" in the final output

        int strLen = str.length();
        int dstLen = 0;
        char chr;

        // Test string for the special characters.
        // When one is found, add another character to dstLen 
        for (int z = 0; z < strLen; z++) {
            chr = str.charAt(z);
            dstLen++;
            if (chr == tabchar || chr == linefeed || chr == carriagereturn || chr == backslash) {
                dstLen++;
            }
        }

        // if no special chars were found, dstLen should == strLen
        if (dstLen > strLen) {
            // dstLen indicates how many characters are needed for our new string
            char[] chars = new char[dstLen];
            int y = 0;
            for (int z = 0; z < strLen; z++) {
                chr = str.charAt(z);
                if (chr == tabchar || chr == linefeed || chr == carriagereturn || chr == backslash) {
                    chars[y++] = '\\';
                    switch (chr) {
                        case tabchar:
                            chars[y++] = 't';
                            break;
                        case linefeed:
                            chars[y++] = 'n';
                            break;
                        case carriagereturn:
                            chars[y++] = 'r';
                            break;
                        case backslash:
                            chars[y++] = '\\';
                            break;
                        default:
                            break;
                    }
                } else {
                    chars[y++] = chr;
                }
            }
            doWrite(chars);
        } else {
            doWrite(str);
        }
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

    private void bytesToHex(final byte[] bytes, final int offset, final int length, final boolean header) throws Exception {
        int offs = offset;
        int i = 0;
        int j = 0;
        int v;

        char[] hexChars = new char[length * 2];
        while (i++ < length) {
            v = bytes[offs++] & 0xFF;
            hexChars[j++] = hexArray[v >>> 4];
            hexChars[j++] = hexArray[v & 0x0F];
        }

        if (header)
            doWrite("\\x");

        doWrite(hexChars);
    }

    private void bytesToHex(final byte[] bytes, final boolean header) throws Exception {
        bytesToHex(bytes, 0, bytes.length, header);
    }

}

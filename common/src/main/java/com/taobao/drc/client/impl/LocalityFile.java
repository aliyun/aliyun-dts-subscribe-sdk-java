package com.taobao.drc.client.impl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class LocalityFile is in charge of handling inner-state files including
 * binary log offsets, bad daemon servers ever found and other possible
 * persistent information required by the DRCClient.
 * 
 * @author erbai.qzc <erbai.qzc@taobao.com>
 *
 */
public class LocalityFile {

    private String basename;
    private long sizeLimit = 0;
    private long size = 0;
    private FileOutputStream os;
    private BufferedReader is;

    private static final SimpleDateFormat DATE_FORMAT =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * Construct a new LocalityFile, currently the limit = 0;
     * @param name is the base name of the file. 
     * @param rotateLimit is the rotating limit of the files.
     * @param fileSize is the size limit of one file.
     */
    public LocalityFile(final String name,
                 final int rotateLimit,
                 final long fileSize)
        throws FileNotFoundException {
        size = 0;
        basename = name;
        sizeLimit = fileSize;
    }

    public void writeLine(final String data) throws IOException {
        if (os == null)
            os = new FileOutputStream(basename, false);
        final String date = DATE_FORMAT.format(new Date()) + " "; 
        long writtenLength = (long)data.getBytes().length +
            date.getBytes().length + System.getProperty("line.separator").getBytes().length;
        if (size + writtenLength >= sizeLimit) {
            os.close();
            os = new FileOutputStream(basename);
        }
        os.write(date.getBytes());
        os.write(data.getBytes());
        os.write(System.getProperty("line.separator").getBytes());
        os.flush();
        size += writtenLength;
    }
    
    public String readLine() throws IOException
    {
        if (is == null)
            is = new BufferedReader(new InputStreamReader(new FileInputStream(basename)));
        return is.readLine();
    }

    public  void close() throws IOException {
        size = 0;
        if (os != null)
            os.close();
        if (is != null)
            is.close();
        os = null;
    }
}

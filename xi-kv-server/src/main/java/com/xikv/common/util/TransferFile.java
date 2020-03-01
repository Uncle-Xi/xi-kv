package com.xikv.common.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;

/**
 * @description: TransferFile
 * ...
 * @author: Uncle.Xi 2020
 * @since: 1.0
 * @Environment: JDK1.8 + CentOS7.x + ?
 */
public abstract class TransferFile {

    public static File getTransferFile(File source, String location) throws Exception{
        File tempFile;
        Enumeration<URL> urlEnumeration = Thread.currentThread().getContextClassLoader()
                .getResources(location);
        while (urlEnumeration.hasMoreElements()) {
            URL url = urlEnumeration.nextElement();
            if (url != null) {
                tempFile = new File(StringUtils.NONE_SPACE);
                if (!tempFile.exists()) {
                    tempFile.mkdirs();
                }
                tempFile = new File(tempFile.getAbsolutePath() + File.separator
                        + System.currentTimeMillis() + source.getName());
                copyFile(url, tempFile);
                return tempFile;
            }
        }
        return source;
    }

    public static void deleteFile(File source){
        if (source != null && source.exists()) {
            source.delete();
        }
    }

    public static void copyFile(URL url, File target) throws Exception{
        int index;
        byte[] bytes = new byte[1024];
        InputStream inputStream = url.openStream();
        FileOutputStream downloadFile = new FileOutputStream(target);
        while ((index = inputStream.read(bytes)) != -1) {
            downloadFile.write(bytes, 0, index);
            downloadFile.flush();
        }
        inputStream.close();
        downloadFile.close();
    }
}

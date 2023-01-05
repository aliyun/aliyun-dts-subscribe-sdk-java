package com.aliyun.dts.subscribe.clients.utils;

import java.io.Closeable;
import java.io.File;

public class FileUtil {
    public static boolean checkFileExists(String fileName) {
        File metaFile = new File(fileName);
        return metaFile.exists();
    }

    public static void deleteFile(String fileName) {
        File maybeAbsolutePath = new File(fileName);
        if (!maybeAbsolutePath.exists()) {
            return;
        }
        File metaFile = null;
        if (maybeAbsolutePath.isAbsolute()) {
            metaFile = maybeAbsolutePath;
        } else {
            File currentPath = new File(".");
            metaFile = new File(currentPath.getAbsolutePath() + File.separator + fileName);
        }
        boolean deleted = metaFile.delete();
        if (!deleted) {
            throw new RuntimeException(metaFile.getAbsolutePath() + " should be cleaned anyway");
        }
    }

    public static  void swallowErrorClose(Closeable target) {
        try {
            if (null != target) {
                target.close();
            }
        } catch (Exception e) {
        }
    }
}

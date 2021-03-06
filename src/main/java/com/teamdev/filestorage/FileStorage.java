package com.teamdev.filestorage;

import com.teamdev.filestorage.exception.DuplicateFileException;
import com.teamdev.filestorage.exception.OutOfMemoryException;

import java.io.InputStream;

public interface FileStorage {
    boolean saveFile(String key, InputStream input) throws DuplicateFileException, OutOfMemoryException;
    boolean saveFile(String key, InputStream input, long timeToLiveMillis)
            throws DuplicateFileException, OutOfMemoryException;

    boolean deleteFile(String key);

    void purge(float percent);
    void purge(long bytes);
    InputStream readFile(String key);

    long getFreeSpace();
    long getUsedSpace();
}

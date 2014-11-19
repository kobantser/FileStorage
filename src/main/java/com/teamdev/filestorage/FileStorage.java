package com.teamdev.filestorage;

import java.io.InputStream;

public interface FileStorage {
    boolean saveFile(String key, InputStream input) throws StorageException;
    boolean saveFile(String key, InputStream input, long timeToLive) throws StorageException;
    boolean deleteFile(String key);

    void purge(float percent);
    void purge(long bytes);
    InputStream readFile(String key);

    long getUsedSpace();
}

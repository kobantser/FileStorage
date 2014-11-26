package com.teamdev.filestorage.impl;

import com.teamdev.filestorage.FileStorage;
import com.teamdev.filestorage.exception.DuplicateFileException;
import com.teamdev.filestorage.exception.OutOfMemoryException;

import java.io.InputStream;

public class FileStorageImpl implements FileStorage {
    private final HashFile hashFile;


    public FileStorageImpl(long maxSpace, String rootFolder) {
        final FileService service = new FileService(maxSpace, rootFolder + "/.system/");
        hashFile = new HashFile(service, new ExpiredFileCollector(service));
    }

    @Override
    public boolean saveFile(String key, InputStream input)
            throws DuplicateFileException, OutOfMemoryException {
        return hashFile.put(key, input);
    }

    @Override
    public boolean saveFile(String key, InputStream input, long timeToLive)
            throws DuplicateFileException, OutOfMemoryException {
        return hashFile.putExpiredFile(key, input, timeToLive);
    }

    @Override
    public boolean deleteFile(String key) {
        return hashFile.remove(key);
    }

    @Override
    public void purge(float percent) {
        hashFile.purgeOldFiles(percent);
    }

    @Override
    public void purge(long bytes) {
        hashFile.purgeOldFiles(bytes);
    }

    @Override
    public InputStream readFile(String key) {
        return hashFile.openStream(key);
    }

    @Override
    public long getFreeSpace() {
        return hashFile.getFreeSpace();
    }

    @Override
    public long getUsedSpace() {
        return hashFile.getUsedSpace();
    }
}

package com.teamdev.filestorage.impl;

import com.teamdev.filestorage.FileStorage;
import com.teamdev.filestorage.StorageException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * FileStorageImpl based on implementation of the <tt>FileStorage</tt> interface.
 * This implementation provides: save file, save temporary file, delete file, read file
 * and purge specified percent of the storage space or purge specified bytes in the storage.
 *
 * An instance of FileStorageImpl has two parameters that affect it's performance:
 * <i>maxSpace</i> and <i>root folder</i>. The <i>maxSpace</i> is the space that
 * has been allocated for storage. The <i>rootFolder</i> is simply the path in which
 * all files will be stored.
 */
public class FileStorageImpl implements FileStorage {
    private final HashFile hashFile;

    public FileStorageImpl(long maxSpace, String rootFolder) {
        FileService service = new FileService(maxSpace, rootFolder + "/.system/");
        hashFile = new HashFile(service, new ExpiredFileCollector(service));
    }

    /**
     * Save file in the storage and write specified data in it.
     * @param key the name of the file
     * @param input data to be written in the specified file
     * @return <code>true</code> if and only if the directory was created;
     * <code>false</code> otherwise
     * @throws StorageException
     */
    @Override
    public boolean saveFile(String key, InputStream input) throws StorageException {
        return hashFile.put(key, input);
    }

    /**
     * Save temporary file in the storage and write specified data in it.
     * @param key the name of the file
     * @param input data to be written in the specified file
     * @param timeToLive living time of file
     * @return <code>true</code> if and only if the directory was created;
     * <code>false</code> otherwise
     * @throws StorageException
     */
    @Override
    public boolean saveFile(String key, InputStream input, long timeToLive) throws StorageException {
        return hashFile.putTempFile(key, input, timeToLive);
    }

    /**
     * Delete file from the storage.
     * @param key the name of the file.
     * @return <code>true</code> if and only if the directory was created;
     * <code>false</code> otherwise
     */
    @Override
    public boolean deleteFile(String key) {
        return hashFile.remove(key);
    }

    /**
     * Purge specified percent of storage space file. Primarily removed the oldest files.
     * @param percent percent of max storage to be deleted
     */
    @Override
    public void purge(float percent) {
        hashFile.purgeOldFiles(percent);
    }

    /**
     * Delete specified count of bytes in the storage. Primarily removed the oldest files.
     * @param bytes to be deleted
     */
    @Override
    public void purge(long bytes) {
        hashFile.purgeOldFiles(bytes);
    }

    /**
     * Returns count of used space in bytes.
     * @return used bytes in the storage.
     */
    @Override
    public long getUsedSpace() {
        return hashFile.getUsedSpace();
    }

    /**
     * Find file in the storage with specified key and returns InputStream with data that
     * has written in the file.
     * @param key the name of the file
     * @return InputStream with data that was written in the file.
     */
    @Override
    public InputStream readFile(String key) {
        return hashFile.openStream(key);
    }

    public static void main(String[] args) throws InterruptedException, StorageException {
        FileStorage storage = new FileStorageImpl(1000, "C:/Testing");

        storage.saveFile("anton", new ByteArrayInputStream("anton".getBytes()), 100);
        Thread.sleep(500);

    }
}

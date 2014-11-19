package com.teamdev.filestorage.impl;

import com.teamdev.filestorage.StorageException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;

public class HashFile {
    private static final Logger LOGGER = LoggerFactory.getLogger(HashFile.class);

    private static final long DELAY = 500;
    private static final long PERIOD = 100;

    /**
     * The default initial capacity.
     */
    private static final int INIT_CAPACITY = 1 << 2;

    /**
     * The maximum capacity
     * Must be a power of two <= 2<<14
     */
    private static final int MAXIMUM_CAPACITY = 2 << (15 - 1);

    /**
     * Number of permanent files.
     */
    private volatile int N;

    /**
     * Number of folders.
     */
    private volatile int M;

    private final FileService service;
    private final ExpiredFileCollector collector;

    public HashFile(FileService service, ExpiredFileCollector collector) {
        this(INIT_CAPACITY, service, collector);
    }

    public HashFile(int M, FileService service, ExpiredFileCollector collector) {
        this.M = M;
        this.service = service;
        this.collector = collector;

        service.createFolders(0, M);

        checkForRecovery();

        Timer timer = new Timer(true);
        timer.schedule(collector, DELAY, PERIOD);
    }

    /**
     * Receive String argument and return hash code of this object.
     * The method returns only code with positive value.
     */
    private int hash(String key) {
        return (key.hashCode() & 0x7fffffff) % M;
    }

    /**
     * Rehashes the contents and move all files in necessary positions.
     * @param size size the new capacity
     * @throws IOException
     */
    private synchronized void resize(int size) {
        LOGGER.info("Resize to " + size);
        service.setResizeState(true);

        service.createFolders(M, size);
        this.M = size;

        Set<File> files = new HashSet<>();

        for (File folder : service.getRootFolder().listFiles()) {
            for (File file : folder.listFiles()) {
                files.add(file);
            }
        }

        String fileName;
        for (File file : files) {
            fileName = file.getName();
            service.moveFile(file, service.getFile(
                    fileName, Integer.toString(hash(fileName))));
        }

        service.setResizeState(false);
    }

    /**
     * Create file in the storage and put in map 'files'.
     * @param key the name of the file
     * @param input InputStream with some data
     * @return <code>true</code> if and only if the file was created;
     * <code>false</code> otherwise
     * @throws StorageException
     */
    public boolean put(String key, InputStream input) throws StorageException {
        LOGGER.info("Put: " + key);
        checkForResize();
        boolean success;
        synchronized (this) {
            String folderName = Integer.toString(hash(key));
            success = service.createFile(key, folderName, input);
            if (success) {
                N++;
            }
        }

        return success;
    }

    /**
     * Create temporary file in the storage and transfer to 'ExpiredFileCollector' in
     * which this file will be deleted when it's living time finished.
     * @param key key with key with which the specified file is to be associated
     * @param input input stream to be associated with specified key
     * @param timeToLive define living time in millis for file
     * @return <code>true</code> if and only if the file was created;
     * <code>false</code> otherwise
     * @throws StorageException
     */
    public boolean putTempFile(String key, InputStream input, long timeToLive) throws StorageException {
        LOGGER.info("Put temp file: " + key + " with time: " + timeToLive);
        checkForResize();
        boolean success;
        synchronized (this) {
            String folderName = Integer.toString(hash(key));
            File file = service.getFile(key, folderName);

            success = service.createFile(key, folderName, input);
            if (success) {
                collector.push(file, timeToLive);
            }
        }

        return success;
    }

    /**
     * Delete file with specified name from the storage.
     * @param key the name of the file
     * @return <code>true</code> if and only if the file was deleted;
     * <code>false</code> otherwise
     */
    public synchronized boolean remove(String key) {
        LOGGER.info("Remove file: " + key);
        String folderName = Integer.toString(hash(key));
        File file = service.getFile(key, folderName);

        boolean success;
        success = service.deleteFile(file);
        if (success) {
            collector.deleteIfExist(key);
            N--;
        }

        return success;
    }

    /**
     * Delete oldest files from storage.
     * @param toRelease bytes to release
     */
    public synchronized void purgeOldFiles(long toRelease) {
        LOGGER.info("Purge " + toRelease + " bytes");
        File rootFolder = service.getRootFolder();

        class FileWithTime {
            private File file;
            private FileTime creationTime;

            public FileWithTime(File file, FileTime creationTime) {
                this.file = file;
                this.creationTime = creationTime;
            }
        }

        List<FileWithTime> files = new ArrayList<>();
        BasicFileAttributes attributes;
        Path path;

        try {
            for (File folder : rootFolder.listFiles()) {
                for (File file : folder.listFiles()) {
                    path = Paths.get(file.toString());
                    attributes = Files.readAttributes(path, BasicFileAttributes.class);
                    files.add(new FileWithTime(file, attributes.creationTime()));
                }
            }
        } catch (IOException e) {
            LOGGER.error("Failed to purge old files");
        }

        Collections.sort(files, new Comparator<FileWithTime>() {
            @Override
            public int compare(FileWithTime o1, FileWithTime o2) {
                return o1.creationTime.compareTo(o2.creationTime);
            }
        });

        File file;
        long fileSize;
        int index = 0;
        while (toRelease > 0 && index < files.size()) {
            file = files.get(index).file;
            fileSize = file.length();

            service.deleteFile(file);
            collector.deleteIfExist(file.getName());

            toRelease -= fileSize;
            service.setUsedSpace(service.getUsedSpace() - fileSize);

            index++;
        }
    }

    /**
     * Delete specified percent of oldest files.
     * @param percent of files to release
     */
    public void purgeOldFiles(float percent) {
        LOGGER.info("Purge " + percent + " of the storage space");
        long maxSpace = service.getMaxSpace();
        long toRelease = (long) (Math.ceil(maxSpace * percent / 100));

        purgeOldFiles(toRelease);
    }

    /**
     * Open stream of specified file and returns InputStream.
     * @param key the name of the file
     * @return InputStream with data that was written in the file
     */
    public synchronized InputStream openStream(String key) {
        return service.readFile(key, Integer.toString(hash(key)));
    }

    private synchronized void checkForResize() {
        int tempFileNumber = collector.getTempFilesNumber();
        if ((N + tempFileNumber) >= 50 * M && 2 * M < MAXIMUM_CAPACITY) {
            resize(2 * M);
        }
    }

    private void checkForRecovery() {
        LOGGER.info("Check for recovery");
        Properties tempFilesProperties = service.getTempFilesProperty();

        File rootFolder = service.getRootFolder();
        Path path = Paths.get(rootFolder.toString());

        if (Files.exists(path)) {
            int fileNumber = 0;
            long usedSpace = 0;

            for (File folder : rootFolder.listFiles()) {
                for (File file : folder.listFiles()) {
                    usedSpace += file.length();
                    fileNumber++;
                }
            }

            try {
                tempFilesProperties.load(
                        new FileInputStream("src/main/resources/tempFiles.properties"));
            } catch (IOException e) {
                LOGGER.error("Failed to load temp file properties");
            }

            for (String key : tempFilesProperties.stringPropertyNames()) {
                collector.pushFromProperty(new File(key),
                        Long.valueOf(tempFilesProperties.getProperty(key)));
            }

            collector.purgeExpiredFiles();

            this.N = fileNumber - collector.getTempFilesNumber();
            service.setUsedSpace(usedSpace);

            if (service.getResizeState()) {
                resize(2 * fileNumber);
            }
        }
    }

    public long getUsedSpace() {
        return service.getUsedSpace();
    }
}

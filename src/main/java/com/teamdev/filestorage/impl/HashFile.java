package com.teamdev.filestorage.impl;

import com.teamdev.filestorage.exception.DuplicateFileException;
import com.teamdev.filestorage.exception.OutOfMemoryException;
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

    private final FileService service;
    private final ExpiredFileCollector collector;

    private static final long DELAY = 500;
    private static final long PERIOD = 100;

    public HashFile(FileService service, ExpiredFileCollector collector) {
        this.service = service;
        this.collector = collector;

        checkForRecovery();

        final Timer timer = new Timer(true);
        timer.schedule(collector, DELAY, PERIOD);
    }

    private int hash(String key) {
        return key.hashCode() & 0x7fffffff;
    }

    private String createFolderStructure(String key) {
        int hashCode = hash(key) % (int) Math.pow(2, 30);
        int firstLevel = hashCode / (int) Math.pow(2, 15);
        int secondLevel = hashCode % (int) Math.pow(2, 15);

        return firstLevel + File.separator + secondLevel;
    }

    public boolean put(String key, InputStream input) throws DuplicateFileException, OutOfMemoryException {
        LOGGER.info("Put file: " + key);
        final String path = createFolderStructure(key);
        boolean success = service.createFile(key, path, input);
        return success;
    }

    /**
     * Create temporary file in the storage and transfer to 'ExpiredFileCollector' in
     * which this file will be deleted when it's living time finished.
     * @param key key with key with which the specified file is to be associated
     * @param input input stream to be associated with specified key
     * @param timeToLiveMillis define living time in millis for file
     * @return <code>true</code> if and only if the file was created;
     * <code>false</code> otherwise
     * @throws DuplicateFileException
     * @throws OutOfMemoryException
     */
    public boolean putExpiredFile(String key, InputStream input, long timeToLiveMillis) throws DuplicateFileException, OutOfMemoryException {
        LOGGER.info("Put expired file: " + key + ", living time: " + timeToLiveMillis);

        final String path = createFolderStructure(key);
        final File file = service.getFile(key, path);

        boolean success;
        synchronized (this) {
            success = service.createFile(key, path, input);
            if (success) {
                collector.push(file, timeToLiveMillis);
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
    public boolean remove(String key) {
        LOGGER.info("Remove file: " + key);

        final String path = createFolderStructure(key);
        final File file = service.getFile(key, path);

        boolean success;
        synchronized (this) {
            success = service.deleteFile(file);
            if (success) {
                collector.deleteIfExist(key);
            }
        }

        return success;
    }

    /**
     * Delete oldest files from storage.
     * @param byteToRelease bytes to release
     */
    public synchronized void purgeOldFiles(long byteToRelease) {
        LOGGER.info("Purge " + byteToRelease + " bytes");

        final File rootFolder = new File(service.getRootFolder());

        class FileWithTime {
            private File file;
            private FileTime creationTime;

            public FileWithTime(File file, FileTime creationTime) {
                this.file = file;
                this.creationTime = creationTime;
            }
        }

        final List<FileWithTime> files = new ArrayList<>();
        BasicFileAttributes attributes;
        Path path;

        if (service.getUsedSpace() > 0) {
            try {
                for (File firstFolder : rootFolder.listFiles()) {
                    for (File secondFolder : firstFolder.listFiles()) {
                        for (File file : secondFolder.listFiles()) {
                            path = Paths.get(file.toString());
                            attributes = Files.readAttributes(path, BasicFileAttributes.class);
                            files.add(new FileWithTime(file, attributes.creationTime()));
                        }
                    }
                }

            } catch (IOException e) {
                LOGGER.error("Failed to purge old files");
            }
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
        while (byteToRelease > 0 && index < files.size()) {
            file = files.get(index).file;
            fileSize = file.length();

            service.deleteFile(file);
            collector.deleteIfExist(file.getName());

            byteToRelease -= fileSize;
            service.setUsedSpace(service.getUsedSpace() - fileSize);

            index++;
        }
    }

    private void checkForRecovery() {
        LOGGER.info("Check for recovery");

        Properties tempFilesProperties = service.getExpiredFilesProperty();

        File rootFolder = new File(service.getRootFolder());
        Path path = Paths.get(rootFolder.toString());

        if (Files.exists(path)) {
            long usedSpace = 0;

            for (File firstFolder : rootFolder.listFiles()) {
                for (File secondFolder : firstFolder.listFiles()) {
                    for (File file : secondFolder.listFiles())
                        usedSpace += file.length();
                }
            }

            try (InputStream input = new FileInputStream("src/main/resources/tempFiles.properties")) {
                tempFilesProperties.load(input);
            } catch (IOException e) {
                LOGGER.error("Failed to load temp file properties");
            }

            for (String key : tempFilesProperties.stringPropertyNames()) {
                collector.pushFromProperty(new File(key),
                        Long.valueOf(tempFilesProperties.getProperty(key)));
            }

            collector.purgeExpiredFiles();

            service.setUsedSpace(usedSpace);
        }
    }

    public void purgeOldFiles(float percent) {
        LOGGER.info("Purge the oldest files, percent: " + percent);

        long maxSpace = service.getMaxSpace();
        long byteToRelease = (long) (Math.ceil(maxSpace * percent / 100));

        purgeOldFiles(byteToRelease);
    }

    public InputStream openStream(String key) {
        LOGGER.info("Open stream of file: " + key);

        return service.readFile(key, createFolderStructure(key));
    }

    public long getUsedSpace() {
        return service.getUsedSpace();
    }

    public long getFreeSpace() {
        return service.getMaxSpace() - getUsedSpace();
    }
}

package com.teamdev.filestorage.impl;

import com.teamdev.filestorage.StorageException;
import com.teamdev.filestorage.exception.DuplicateFileException;
import com.teamdev.filestorage.exception.OutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * An instance of FileService has two parameters that affect it's performance:
 * <i>max space</i> and <i>root folder</i>. The <i>max space</i> is the space
 * that has been allocated for the storage. The <i>root folder</i> is simply the path
 * in which files stored.
 */
public class FileService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileService.class);

    private final String rootFolder;
    private final long maxSpace;

    private final Properties tempFilesProperty;
    private final Properties storageDataProperty;

    private volatile long usedSpace;

    public FileService(long maxSpace, String rootFolder) {
        this.maxSpace = maxSpace;
        this.rootFolder = rootFolder;

        tempFilesProperty = new Properties();
        storageDataProperty = new Properties();
    }

    /**
     * Creates file in the storage with specified name in specified folder and write
     * the data that InputStream has.
     * @param name the name of the file
     * @param folder folder to be added specified file
     * @param input InputStream with some data
     * @return <code>true</code> if and only if the file was created;
     * <code>false</code> otherwise
     * @throws StorageException
     */
    public boolean createFile(String name, String folder, InputStream input) throws StorageException {
        LOGGER.info("Create file: " + name + " in folder: " + folder);
        Path path = Paths.get(rootFolder + folder + File.separator + name);

        if (!Files.exists(path)) {
            try {
                Files.createFile(path);

                try (OutputStream output = new BufferedOutputStream(
                        new FileOutputStream(path.toString()))) {

                    while (true) {
                        int b = input.read();
                        if (b == -1) {
                            break;
                        }

                        if (!hasSpace()) {
                            throw new OutOfMemoryException("Out of memory.");
                        }

                        output.write(b);
                        usedSpace++;
                    }
                }
            } catch (IOException e) {
                return false;
            }
        } else {
            throw new DuplicateFileException("File is already exist.");
        }

        return true;
    }

    /**
     * Creates folder with specified name.
     * @param name the name of the folder
     * @return <code>true</code> if and only if the directory was created;
     * <code>false</code> otherwise
     */
    public boolean createFolder(String name) {
        LOGGER.info("Create folder: " + name);
        File path = new File(rootFolder + File.separator + name);
        return path.mkdirs();
    }

    public void createFolders(int from, int to) {
        LOGGER.info("Create folders from: " + from + " to: " + to);
        for (int i = from; i < to; i++) {
            createFolder(Integer.toString(i));
        }
    }

    /**
     * Deletes file with specified name from the storage.
     * @param file path of the file
     * @return <code>true</code> if and only if the file was deleted;
     * <code>false</code> otherwise
     */
    public boolean deleteFile(File file) {
        LOGGER.info("Delete file: " + file);
        Path path = Paths.get(file.toString());
        try {
            Files.delete(path);
            usedSpace -= file.length();
        } catch (IOException e) {
            return false;
        }

        return true;
    }

    public InputStream readFile(String name, String folder) {
        LOGGER.info("Read file: " + name + " in folder: " + folder);
        BufferedInputStream input;
        try {
            input = new BufferedInputStream(new FileInputStream(
                    rootFolder + folder + File.separator + name));
        } catch (IOException e) {
            return null;
        }

        return input;
    }

    /**
     * Move file from one path to another.
     * @param from from path
     * @param to to path
     * @return <code>true</code> if and only if the file was moved;
     * <code>false</code> otherwise
     */
    public boolean moveFile(File from, File to) {
        LOGGER.info("Move file: "  + from + " to " + to);
        return from.renameTo(to);
    }

    public File getFile(String name, String folder) {
        return new File(rootFolder + folder + File.separator + name);
    }

    public File getRootFolder() {
        return new File(rootFolder);
    }

    /**
     * @return code>true</code> if and only if the file storage has space;
     * <code>false</code> otherwise
     */
    public boolean hasSpace() {
        return (maxSpace - usedSpace) > 0;
    }

    /**
     * Add information about temporary file in the property file.
     * @param key the name of the file
     * @param value time file to be deleted
     */
    public void addTempFilesProperty(String key, String value) {
        LOGGER.info("Add temp file: " + key + " value: " + value + " to property file");
        try {
            tempFilesProperty.setProperty(key, value);
            tempFilesProperty.store(
                    new FileOutputStream("src/main/resources/tempFiles.properties"), null);
        } catch (IOException e) {
            LOGGER.error("Failed to add temp file property");
        }
    }

    /**
     * Delete information about temporary file from the property file.
     * @param key the name of the file
     */
    public void deleteTempFilesProperty(String key) {
        LOGGER.info("Delete temp file: " + key + " from property file");
        try {
            tempFilesProperty.remove(key);
            tempFilesProperty.store(
                    new FileOutputStream("src/main/resources/tempFiles.properties"), null);
        } catch (IOException e) {
            LOGGER.error("Failed to delete temp files property");
        }
    }

    public void setResizeState(boolean isResize) {
        String key = "isResize";
        try {
            storageDataProperty.setProperty(key, Boolean.toString(isResize));
            storageDataProperty.store(
                    new FileOutputStream("src/main/resources/storageData.properties"), null);
        } catch (IOException e) {
            LOGGER.error("Failed to set resize state in the property file");
        }
    }

    public boolean getResizeState() {
        String key = "isResize";
        return Boolean.parseBoolean(storageDataProperty.getProperty(key));
    }

    public long getMaxSpace() {
        return maxSpace;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }

    public Properties getTempFilesProperty() {
        return tempFilesProperty;
    }
}

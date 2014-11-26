package com.teamdev.filestorage.impl;

import com.teamdev.filestorage.exception.DuplicateFileException;
import com.teamdev.filestorage.exception.OutOfMemoryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class FileService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileService.class);

    private final String rootFolder;
    private final long maxSpace;

    private final Properties expiredFilesProperty;

    private volatile long usedSpace;

    public FileService(long maxSpace, String rootFolder) {
        this.maxSpace = maxSpace;
        this.rootFolder = rootFolder;

        expiredFilesProperty = new Properties();
    }

    public boolean createFile(String name, String folderStructure, InputStream input)
            throws OutOfMemoryException, DuplicateFileException {
        LOGGER.info("Create file: " + name);

        final Path path = Paths.get(rootFolder, folderStructure, name);

        if (!Files.exists(path)) {
            if (!Files.exists(path.getParent())) {
                createFolders(folderStructure);
            }

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

    public boolean deleteFile(File file) {
        LOGGER.info("Delete file: " + file.getName());

        final Path path = Paths.get(file.toString());
        try {
            Files.delete(path);
            usedSpace -= file.length();
        } catch(IOException e) {
            return false;
        }

        return true;
    }

    public InputStream readFile(String name, String folderStructure) {
        LOGGER.info("Read file: " + name);

        final BufferedInputStream input;
        try {
            input = new BufferedInputStream(new FileInputStream(
                    rootFolder + folderStructure + File.separator + name));
        } catch (IOException e) {
            return null;
        }

        return input;
    }


    private boolean createFolders(String folderStructure) {
        LOGGER.info("Create folders");

        File file = new File(rootFolder + File.separator + folderStructure);
        return file.mkdirs();
    }

    public boolean hasSpace() {
        return (maxSpace - usedSpace) > 0;
    }

    public File getFile(String name, String folderStructure) {
        return new File(rootFolder + folderStructure + File.separator + name);
    }

    public void addExpiredFilesProperty(String key, String value) {
        LOGGER.info("Add expired file: " + key + " value: " + value + " to property file");

        try(OutputStream output = new FileOutputStream(
                "src/main/resources/tempFiles.properties")) {
            expiredFilesProperty.setProperty(key, value);
            expiredFilesProperty.store(output, null);
        } catch(IOException e) {
            LOGGER.error("Failed to add expired file property");
        }
    }

    public void deleteExpiredFilesProperty(String key) {
        LOGGER.info("Delete temp file: " + key + " from property file");

        try(OutputStream output = new FileOutputStream("src/main/resources/tempFiles.properties")) {
            expiredFilesProperty.remove(key);
            expiredFilesProperty.store(output, null);
        } catch (IOException e) {
            LOGGER.error("Failed to delete temp files property");
        }
    }

    public Properties getExpiredFilesProperty() {
        return expiredFilesProperty;
    }

    public long getMaxSpace() {
        return maxSpace;
    }

    public String getRootFolder() {
        return rootFolder;
    }

    public long getUsedSpace() {
        return usedSpace;
    }

    public void setUsedSpace(long usedSpace) {
        this.usedSpace = usedSpace;
    }
}

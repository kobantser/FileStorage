package com.teamdev.filestorage.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class ExpiredFileCollector extends TimerTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExpiredFileCollector.class);

    /**
     * Map of the temporary files to be deleted in specified time.
     */
    private final SortedMap<Date, File> tempFiles = new ConcurrentSkipListMap<>();
    private final FileService service;

    public ExpiredFileCollector(FileService service) {
        this.service = service;
    }

    /**
     * Delete expired files from the storage.
     */
    public void purgeExpiredFiles() {
        Date date = new Date();
        for (Iterator<Map.Entry<Date, File>> it = tempFiles.entrySet().iterator(); it.hasNext(); ) {
            Map.Entry<Date, File> entry = it.next();

            if (entry.getKey().compareTo(date) <= 0) {
                service.deleteFile(tempFiles.get(tempFiles.firstKey()));
                service.deleteTempFilesProperty(entry.getValue().toString());
                it.remove();
            } else {
                break;
            }
        }
    }

    /**
     * Push temp file to the 'tempFiles' map.
     * @param file to be added to the map
     * @param timeToLive living time of the file
     */
    public void push(File file, long timeToLive) {
        Date date = new Date();
        while (tempFiles.containsKey(date)) {
            date = new Date();
        }

        service.addTempFilesProperty(file.toString(), Long.toString(timeToLive + date.getTime()));
        tempFiles.put(new Date(date.getTime() + timeToLive), file);
    }

    /**
     * Push specified file from property file to the 'tempFiles' map.
     * @param file the name of the file
     * @param timeToRemove the time to delete file
     */
    public void pushFromProperty(File file, long timeToRemove) {
        tempFiles.put(new Date(timeToRemove), file);
    }

    /**
     * Delete file from the map if the specified file exist.
     * @param key name of the file
     */
    public void deleteIfExist(String key) {
        LOGGER.info("Delete file if exist: " + key);
        for (Date date : tempFiles.keySet()) {
            File file = tempFiles.get(date);
            String fileName = file.getName();

            if (fileName.equals(key)) {
                tempFiles.remove(date);
                service.deleteTempFilesProperty(key);
                break;
            }
        }
    }

    public int getTempFilesNumber() {
        return tempFiles.size();
    }

    @Override
    public void run() {
       purgeExpiredFiles();
    }
}

package com.teamdev.filestorage;

import com.teamdev.filestorage.exception.DuplicateFileException;
import com.teamdev.filestorage.impl.FileStorageImpl;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FileStorageTest {
    private FileStorage storage;
    private final long maxStorageSpace = 10000;
    private final String rootFolder = "C:/Testing";
    private final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ12345674890";
    private final Random random = new Random();

    @Test
    public void testSaveFiles() throws StorageException {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);
        String[] names = new String[100];

        for (int i = 0; i < 100; i++) {
            names[i] = randomName();
            try {
                storage.saveFile(names[i], new ByteArrayInputStream(names[i].getBytes()));
            } catch (DuplicateFileException e) {
                i--;
            }
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(hasStorageFile(names[i], rootFolder), true);
        }
    }

    @Test
    public void testDeleteFiles() throws StorageException {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);
        String[] names = new String[100];

        for (int i = 0; i < 50; i++) {
            names[i] = randomName();
            try {
                storage.saveFile(names[i], new ByteArrayInputStream(names[i].getBytes()));
            } catch (DuplicateFileException e) {
                i--;
            }
        }

        for (int i = 50; i < 100; i++) {
            names[i] = randomName();
            try {
                storage.saveFile(names[i], new ByteArrayInputStream(names[i].getBytes()), 10000);
            } catch (DuplicateFileException e) {
                i--;
            }
        }

        for (int i = 0; i < 100; i++) {
            storage.deleteFile(names[i]);
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(hasStorageFile(names[i], rootFolder), false);
        }
    }

    @Test
    public void testSaveTempFiles() throws StorageException {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);
        String[] names = new String[100];

        for (int i = 0; i < 100; i++) {
            names[i] = randomName();
            try {
                storage.saveFile(names[i],
                        new ByteArrayInputStream(names[i].getBytes()), 10000);
            } catch (DuplicateFileException e) {
                i--;
            }
        }

        for (int i = 0; i < 100; i++) {
            assertEquals(hasStorageFile(names[i], rootFolder), true);
        }
    }

    @Test
    public void testPurgeExpiredFiles() throws StorageException, InterruptedException {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);
        String[] names = new String[50];

        for (int i = 0; i < 50; i++) {
            names[i] = randomName();
            try {
                storage.saveFile(names[i],
                        new ByteArrayInputStream(names[i].getBytes()), 50);
            } catch (DuplicateFileException e) {
                i--;
            }
        }

        Thread.sleep(1000);

        for (int i = 0; i < 50; i++) {
            assertEquals(hasStorageFile(names[i], rootFolder), false);
        }
    }

    @Test
    public void testPurge() throws StorageException {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);
        String[] names = new String[100];

        long beforeUsedSpace = storage.getUsedSpace();

        long usedSpace = 0;
        for (int i = 0; i < 50; i++) {
            names[i] = randomName();
            try {
                storage.saveFile(names[i], new ByteArrayInputStream(names[i].getBytes()));
                usedSpace += names[i].getBytes().length;
            } catch (DuplicateFileException e) {
                i--;
            }
        }

        storage.purge(usedSpace);
        assertTrue(storage.getUsedSpace() <= beforeUsedSpace);
    }

    @Test
    public void testPurgeWithPercent() {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);
        storage.purge(100f);
        assertEquals(0, storage.getUsedSpace());
    }

    @Test
    public void testReadFile() throws StorageException, IOException {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);
        String name = randomName();

        boolean flag = true;
        while (flag) {
            try {
                storage.saveFile(name, new ByteArrayInputStream(name.getBytes()));
                flag = false;
            } catch (DuplicateFileException e) { }
        }

        InputStream input = storage.readFile(name);

        String result = "";
        int b;
        while (true) {
            b = input.read();
            if (b == -1) {
                break;
            }
            result += b;
        }

        String expected = "";
        for (int i : name.getBytes()) {
            expected += i;
        }

        assertEquals(expected, result);
    }

    @Test(expected=DuplicateFileException.class)
    public void testDuplicateFile() throws StorageException {
        storage = new FileStorageImpl(maxStorageSpace, rootFolder);

        storage.saveFile("abc", new ByteArrayInputStream("abc".getBytes()));
        storage.saveFile("abc", new ByteArrayInputStream("abc".getBytes()));
    }

    private String randomName() {
        String name = "";

        int length = random.nextInt(5) + 5;

        for (int i = 0; i < length; i++) {
            name += characters.charAt(random.nextInt(characters.length()));
        }

        return name;
    }

    private boolean hasStorageFile(String key, String root) {
        File storageFolder = new File(root + "/.system/");

        for (File folder : storageFolder.listFiles()) {
            for (String file : folder.list()) {
                if (file.equals(key)) {
                    return true;
                }
            }
        }

        return false;
    }
}

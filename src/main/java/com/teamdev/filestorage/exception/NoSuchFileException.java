package com.teamdev.filestorage.exception;

import com.teamdev.filestorage.StorageException;

public class NoSuchFileException extends StorageException {
    public NoSuchFileException(String message) {
        super(message);
    }
}

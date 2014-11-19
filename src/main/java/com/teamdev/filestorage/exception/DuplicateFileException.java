package com.teamdev.filestorage.exception;

import com.teamdev.filestorage.StorageException;

public class DuplicateFileException extends StorageException {
    public DuplicateFileException(String message) {
        super(message);
    }
}

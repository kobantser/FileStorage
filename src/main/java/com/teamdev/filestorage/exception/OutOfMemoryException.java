package com.teamdev.filestorage.exception;

import com.teamdev.filestorage.StorageException;

public class OutOfMemoryException extends StorageException {
    public OutOfMemoryException(String message) {
        super(message);
    }
}

package com.practice.config;

import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.batch.item.file.transform.FlatFileFormatException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.DuplicateKeyException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.practice.entity.Person;

public class PersonSkipListener implements SkipListener<Person, Person> {

    private static final String FAILED_DIR = "logs";
    private static final String FAILED_FILE = "failed_records.csv";
    private final File errorFile;

    public PersonSkipListener() {
        File dir = new File(FAILED_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        this.errorFile = new File(dir, FAILED_FILE);

        // Write header if new file
        if (!errorFile.exists()) {
            try (FileWriter fw = new FileWriter(errorFile, true)) {
                fw.write("id,firstName,lastName,email,userStatus,createTs,errorMessage\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void onSkipInRead(Throwable t) {
        String reason = "Unknown read error";
        if (t instanceof FlatFileParseException fpe) {
            reason = "Flat file parse error at line " + fpe.getLineNumber() + ": " + fpe.getInput();
        } else if (t instanceof FlatFileFormatException) {
            reason = "Flat file format error: " + t.getMessage();
        } else if (t instanceof NumberFormatException) {
            reason = "Number format error: " + t.getMessage();
        }
        logError(null, reason);
    }

    @Override
    public void onSkipInProcess(Person item, Throwable t) {
        logError(item, "PROCESS_ERROR: " + t.getMessage());
    }

    @Override
    public void onSkipInWrite(Person item, Throwable t) {
        String reason = "Unknown write error";
        if (t instanceof DuplicateKeyException) reason = "Duplicate key";
        else if (t instanceof DataIntegrityViolationException) reason = "Data integrity violation";
        logError(item, reason);
    }

    private void logError(Person item, String reason) {
        try (FileWriter fw = new FileWriter(errorFile, true)) {
            fw.write((item != null ? item.getId() : "READ_ERROR") + "," +
                     (item != null ? item.getFirstName() : "") + "," +
                     (item != null ? item.getLastName() : "") + "," +
                     (item != null ? item.getEmail() : "") + "," +
                     (item != null ? item.getUserStatus() : "") + "," +
                     (item != null && item.getCreateTs() != null ? item.getCreateTs() : "") + "," +
                     reason.replace(",", " ") + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.err.println("Skipped record: " + (item != null ? item : "NULL") + " | Reason: " + reason);
    }
}

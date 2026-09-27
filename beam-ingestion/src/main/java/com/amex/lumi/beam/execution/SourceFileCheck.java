package com.amex.lumi.beam.execution;

import com.amex.lumi.beam.model.IngestionControl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Checks the data file matches its control file and did not change since the API accepted it.
 */
public final class SourceFileCheck {

    private SourceFileCheck() {
    }

    /** expectedSha256 and expectedRecordCount come from the API; null skips that check. */
    public static void verify(IngestionControl control, Path dataFile, String expectedSha256,
                              Long expectedRecordCount) {
        if (expectedRecordCount != null && expectedRecordCount != control.expectedRecordCount()) {
            throw new ControlFileException("Control file changed after the request: record_count is "
                    + control.expectedRecordCount() + " but was " + expectedRecordCount);
        }
        String actualName = dataFile.getFileName().toString();
        if (control.fileName() != null && !control.fileName().equals(actualName)) {
            throw new ControlFileException("Control file is for " + control.fileName()
                    + " but the data file is " + actualName);
        }
        if (control.sha256() == null && expectedSha256 == null) {
            return;
        }
        String actualSha256 = sha256(dataFile);
        if (control.sha256() != null && !control.sha256().equals(actualSha256)) {
            throw new ControlFileException("Data file content does not match the sha256 in the control file");
        }
        if (expectedSha256 != null && !expectedSha256.equals(actualSha256)) {
            throw new ControlFileException("Data file changed after the request (its sha256 is different)");
        }
    }

    /** Lowercase hex, the same format as the control file and the sha256sum tool. */
    public static String sha256(Path file) {
        try (InputStream input = Files.newInputStream(file)) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[64 * 1024];
            int read;
            while ((read = input.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (IOException exception) {
            throw new ControlFileException("Unable to read data file " + file, exception);
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is not available", exception);
        }
    }
}

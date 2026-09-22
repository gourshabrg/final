package com.amex.lumi.beam;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;

public class BeamFileSystemTest {

    public static void main(String[] args) {

        String windowsPath =
                "C:/Users/Ravindra/Desktop/Assignment/amex-lumi-ingestion-ravindra/data/error/test-output";

        String fileUri =
                java.nio.file.Paths
                        .get(windowsPath)
                        .toAbsolutePath()
                        .normalize()
                        .toUri()
                        .toString();

        System.out.println("Windows path:");
        System.out.println(windowsPath);

        System.out.println("Converted URI:");
        System.out.println(fileUri);

        ResourceId resourceId =
                FileSystems.matchNewResource(
                        fileUri,
                        false
                );

        System.out.println("Beam ResourceId:");
        System.out.println(resourceId);

        System.out.println("SUCCESS");
    }
}

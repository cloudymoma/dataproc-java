package com.example;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class App {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: App <gcs_bucket_path>");
            System.err.println("Example: App gs://your-bucket-name/path/to/list");
            System.exit(1);
        }

        String gcsPath = args[0];

        SparkConf sparkConf = new SparkConf()
                .setAppName("GCS File Lister");

        // If running locally, uncomment and set master
        // sparkConf.setMaster("local[*]");

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();

        // GCS connector specific configurations if needed
        hadoopConf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        hadoopConf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        // Auth
        // hadoopConf.set("google.cloud.auth.service.account.enable", "true");
        // hadoopConf.set("google.cloud.auth.service.account.json.keyfile", "gs://dingoproc/sa/service-account.json");

        System.out.println("Listing files recursively in: " + gcsPath);

        try {
            FileSystem fs = FileSystem.get(new URI(gcsPath), hadoopConf);
            List<String> filePaths = listRecursive(fs, new Path(gcsPath));
            System.out.println("Found " + filePaths.size() + " files:");
            for (String filePath : filePaths) {
                System.out.println(filePath);
            }
        } catch (IOException | java.net.URISyntaxException e) {
            System.err.println("Error listing files from GCS: " + e.getMessage());
            e.printStackTrace();
        }

        spark.stop();
    }

    private static List<String> listRecursive(FileSystem fs, Path path) throws IOException {
        List<String> filePaths = new ArrayList<>();
        FileStatus[] fileStatuses = fs.listStatus(path);
        for (FileStatus fileStatus : fileStatuses) {
            filePaths.add(fileStatus.getPath().toString()); // Add path for both files and directories
            if (fileStatus.isDirectory()) {
                filePaths.addAll(listRecursive(fs, fileStatus.getPath()));
            }
        }
        return filePaths;
    }
}

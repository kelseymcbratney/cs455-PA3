package csx55.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.Scanner;

public class JobRunner {
    public static void main(String[] args) {
        System.out.printf("Select Which HDFS Job to Run");

        Scanner scanner = new Scanner(System.in);
        switch (scanner.nextInt()) {
            case 1:
                runJob1();
                break;
            case 2:
                runJob2();
                break;
            case 3:
                runJob3();
                break;
            default:
                System.out.println("Invalid Job Number");

        }
    }
}
package com.manu.kafka.utils;


import java.io.*;
import java.util.List;

public class MyFileWriter {


    public static void write(List<String> values) throws IOException {

        java.io.FileWriter fw = null;
        try {
            fw = new java.io.FileWriter("/manu/output.txt");
            for(String value: values) {
                fw.write(value);
            }
        } catch (IOException e) {

        } finally {
            if(null != fw) {
                    fw.close();
            }
        }



    }
}

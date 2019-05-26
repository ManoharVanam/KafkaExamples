package com.manu.kafka.largemessages;

import com.manu.kafka.utils.MyFileWriter;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;


/**
 *  File Utilities
 */
public class FileUtils {

//    public static void main(String s[]) throws IOException {
//        FileUtils fu = new FileUtils();
//       String ss =readFile("/Users/mvanam/Documents/kafkaTest/mano.log");
//       List<String> list = new ArrayList<String>();
//       list.add(ss);
//        MyFileWriter.write(list);
//    }

    public static void writeFile(List<byte[]> messages, String filePath) {
        FileOutputStream stream = null;
        try {
            stream = new FileOutputStream(filePath);
            for (byte[] bytes : messages) {
                stream.write(bytes);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                stream = null;
            }
        }
    }

    public static void writeFile(byte[] messages, String filePath) {
        FileOutputStream stream = null;
        try {
            stream = new FileOutputStream(filePath);
                stream.write(messages);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                stream.close();
            } catch (IOException e) {
                stream = null;
            }
        }
    }


    public static byte[] readFileInList(String fileName) {
        try {
            byte[] lines = Files.readAllBytes(Paths.get(fileName));
//            System.out.println(lines.toString());
            return lines;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }


    public static String readFile(String fileName) {
        try {
            return new String(Files.readAllBytes(Paths.get(fileName)), StandardCharsets.UTF_8);
//            byte[] lines = Files.readAllBytes(Paths.get(fileName));
//            System.out.println(lines.toString());
//            return lines.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

}

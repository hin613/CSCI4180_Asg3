import java.io.*;
import java.util.*;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

public class MyDedup {
  // CUHK Network
  // static {
  //       System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
  //       System.setProperty("https.proxyPort", "8000");
  //       System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
  //       System.setProperty("http.proxyPort", "8000");
  // }

  public static void usage() {
    System.out.println("Usage: ");
    System.out.println("    java MyDedup upload <min_chunk> <avg_chunk> <max_chunk> <d> <file_to_upload> <local|azure>");
    System.out.println("    java MyDedup download <file_to_download> <local_file_name> <local|azure>");
    System.out.println("    java MyDedup delete <file_to_delete> <local|azure>");
  }

  public static void main(String[] args) {
    if (args.length < 1) {
      usage();
      System.exit(1);
    }
    String fileToProcess, storageType, pathName;
    Local local = new Local();

    switch (args[0]) {
      case "upload":
        if (args.length != 7) {
          usage();
          System.exit(1);
        } else {
          int minChunk = 0, avgChunk = 0, maxChunk = 0, d = 0;
          try {
            minChunk = Integer.parseInt(args[1]);
            avgChunk = Integer.parseInt(args[2]);
            maxChunk = Integer.parseInt(args[3]);
            d = Integer.parseInt(args[4]);
          } catch (NumberFormatException e) {
            usage();
            System.exit(1);
          }
          fileToProcess = args[5];
          storageType = args[6];
          if (storageType.equals("local")) {
            try {
              local.upload(minChunk, avgChunk, maxChunk, d, fileToProcess);
            } catch (Exception e) {
                e.printStackTrace();
            }
          } else if (storageType.equals("azure")) {
            try {
              Azure azure = new Azure();
              azure.upload(minChunk, avgChunk, maxChunk, d, fileToProcess);
            } catch (Exception e) {
                e.printStackTrace();
            }
          } else {
            usage();
            System.exit(1);
          }
        }
        break;
        case "download":
          if (args.length != 4) {
            usage();
            System.exit(1);
          } else {
            fileToProcess = args[1];
            pathName = args[2];
            storageType = args[3];
            if (storageType.equals("local")) {
              try {
                local.download(fileToProcess, pathName);
              } catch (Exception e) {
                  e.printStackTrace();
              }
            }
            else if (storageType.equals("azure")) {
              try {
                Azure azure = new Azure();
                azure.download(fileToProcess, pathName);
              } catch (Exception e) {
                  e.printStackTrace();
              }
            } else {
              usage();
              System.exit(1);
            }
          }
          break;
          case "delete":
            if (args.length != 3) {
              usage();
              System.exit(1);
            } else {
              fileToProcess = args[1];
              storageType = args[2];
              if (storageType.equals("local")) {
                try {
                  local.delete(fileToProcess);
                } catch (Exception e) {
                    e.printStackTrace();
                }
              } else if (storageType.equals("azure")) {
                try {
                  Azure azure = new Azure();
                  azure.delete(fileToProcess);
                } catch (Exception e) {
                    e.printStackTrace();
                }
              } else {
                usage();
                System.exit(1);
              }
            }
            break;
            default:
              usage();
              System.exit(1);
        }
    }
}

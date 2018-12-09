import java.io.*;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.InvalidKeyException;

import java.util.*;
import java.math.BigInteger;
import java.lang.ClassNotFoundException;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import java.net.URISyntaxException;

public class Azure{
  public static final String storageConnectionString = "DefaultEndpointsProtocol=https;AccountName=csci4180group28;AccountKey=AL9Qxr3OnckMBPgKe2g1QHcpzvCRg3Y+4yMekGOmFcJq9YPLoPe7FbwydK0IzpF//haaxV/gcq7fVd05dH+bDA==;EndpointSuffix=core.windows.net";

  public static CloudStorageAccount storageAccount;
  public static CloudBlobClient blobClient;
  public static CloudBlobContainer blobContainer;
  public static String containerName = "mycontainer";

  public static final String indexFileName = "mydedup.index";
  public static final String recipesFileName = "fileRecipes.index";

  // CUHK Network
  // static {
  //       System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
  //       System.setProperty("https.proxyPort", "8000");
  //       System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
  //       System.setProperty("http.proxyPort", "8000");
  // }

  public Azure(){
    try {
        storageAccount = CloudStorageAccount.parse(storageConnectionString);
        blobClient = storageAccount.createCloudBlobClient();
        blobContainer = blobClient.getContainerReference(containerName);
        blobContainer.createIfNotExists();
    } catch (Exception ae) {
        System.err.println(ae.getMessage());
        ae.printStackTrace();
    }
	}

  public static int baseMod(int base, int exp, int modulus) {
    base %= modulus;
    int result = 1;
    while (exp > 0) {
      if ((exp & 1) != 0) result = (result * base) % modulus;
      base = (base * base) % modulus;
      exp >>= 1;
    }
    return result;
  }

  public static void deleteDir(File file) {
      File[] contents = file.listFiles();
      if (contents != null) {
          for (File f : contents) {
              deleteDir(f);
          }
      }
      file.delete();
  }

  public void upload(int minChunk, int avgChunk, int maxChunk, int d, String fileToUpload, String storageType) throws IOException, NoSuchAlgorithmException, ClassNotFoundException, URISyntaxException{
    File file = new File(fileToUpload);
    try {
      long fileSize = file.length();

      FileInputStream fis = new FileInputStream(file);
      FileRecipeList FileRecipeList = new FileRecipeList();
      List<String> fileRecipe = new ArrayList<>();
      IndexList IndexList = new IndexList();

      //for local
      File dir = null;

      FileOutputStream indexFileOut = null;
      ObjectOutputStream indexObjOut = null;
      FileOutputStream recipesFileOut = null;
      ObjectOutputStream recipesObjOut = null;

      int maxBufferSize = (int) Runtime.getRuntime().freeMemory();
      int chunkIterations = (int) Math.ceil((1.0 * fileSize / maxBufferSize));
      int lastChunkSize = (int) (fileSize % maxBufferSize);
      long s2 = 0;
      long s1 = 0;
      long totalChunks = 0;
      long dedupChunks = 0;
      double spaceSaving = 0;

      dir = new File("store");

      // error handle
      if (!dir.exists()) {
        if (!dir.mkdir()) {
          System.err.println("Failed to create directory \"store\"");
          return;
        }
      }
      if (!dir.isDirectory()) {
        System.err.println("\"store\" is not a directory");
        return;
      }

      CloudBlockBlob blockBlobReference = blobContainer.getBlockBlobReference(indexFileName);

      if (blockBlobReference.exists()) {
        blockBlobReference.downloadToFile(dir.getName() + "/" + indexFileName);
      }

      FileInputStream fileIn;
      ObjectInputStream objIn;

      // index file

      File indexFile = new File(dir.getName() + "/" + indexFileName);
      if (indexFile.exists()){
        //System.out.println("index file exisits");
        fileIn = new FileInputStream(indexFile.getAbsolutePath());
        objIn = new ObjectInputStream(fileIn);
        IndexList = (IndexList) objIn.readObject();
        objIn.close();
        fileIn.close();
      } else {
        indexFile.createNewFile();
      }
      indexFileOut = new FileOutputStream(indexFile.getAbsolutePath());
      indexObjOut = new ObjectOutputStream(indexFileOut);

      // receipes file

      File recipesFile = new File(dir.getName() + "/" + recipesFileName);
      if (recipesFile.exists()){
        //System.out.println("recipes file exisits");
        fileIn = new FileInputStream(recipesFile.getAbsolutePath());
        objIn = new ObjectInputStream(fileIn);
        FileRecipeList = (FileRecipeList) objIn.readObject();
        objIn.close();
        fileIn.close();
      } else {
        recipesFile.createNewFile();
      }

      if (FileRecipeList.fileRecipes.containsKey(fileToUpload)) {
        System.err.println("Error, file already exists!");
      }
      recipesFileOut = new FileOutputStream(recipesFile.getAbsolutePath());
      recipesObjOut = new ObjectOutputStream(recipesFileOut);

      for (int iteration = 0; iteration < chunkIterations; iteration++) {
        byte[] fileBytes;
        // initilize chunk size
        if (iteration == chunkIterations - 1) {
          fileBytes = new byte[lastChunkSize];
        } else {
          fileBytes = new byte[maxBufferSize];
        }
        fis.read(fileBytes);
        int fileChunkSize = fileBytes.length;
        int m = minChunk;
        int q = avgChunk;
        int s = 0;
        int mask = (int)Math.pow(2, m) - 1;

        int currSize = 0;
        int cursor = 0;
        int rfp = 0;

        List<Integer> checkSum = new ArrayList<>();
        List<Integer> chunkSize = new ArrayList<>();

        while ((cursor + currSize <= fileChunkSize) && (cursor + m <= fileChunkSize)) {
          if (s == 0) {
            rfp = 0;
            for (int i = 1; i <= m; i++) {
              rfp = (rfp + ((int) (fileBytes[cursor + i - 1] & mask) * baseMod(d, m - i, q))) % q;
            }
            currSize = m;
          } else {
            rfp = (d * (rfp - baseMod(d, m - 1, q) * (int) (fileBytes[cursor + s - 1] & mask)) + fileBytes[cursor + s + m - 1]) % q;
            while (rfp < 0) {
              rfp += q;
            }
            currSize++;
          }
          if ((rfp & mask) == 0 || currSize == maxChunk || cursor + currSize >= fileChunkSize) {
            checkSum.add(cursor);
            chunkSize.add(currSize);
            cursor += currSize;
            s = 0;
            currSize = 0;
          } else {
            s++;
          }
        }

        if (cursor < fileChunkSize) {
          checkSum.add(cursor);
          chunkSize.add(fileChunkSize - cursor);
        }
        if (checkSum.size() == chunkSize.size() && checkSum.size() > 0) {
          int numOfChunks = checkSum.size();

          // Do some file settings here
          for (int i = 0; i < numOfChunks; i++) {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(Arrays.copyOfRange(fileBytes, checkSum.get(i), checkSum.get(i) + chunkSize.get(i)));

            byte[] messageDigest = md.digest();

            BigInteger no = new BigInteger(1, messageDigest);

            String hashtext = no.toString(16);

            while (hashtext.length() < 32) {
              hashtext = "0" + hashtext;
            }

            if (IndexList.index.containsKey(hashtext)) {
              IndexList.index.get(hashtext).refCount += 1;
              fileRecipe.add(hashtext);
            } else {
              Index index = new Index();

              index.chunkSize = chunkSize.get(i);

              CloudBlockBlob blob = blobContainer.getBlockBlobReference(hashtext);

              blob.uploadFromByteArray(fileBytes, checkSum.get(i), chunkSize.get(i));
              IndexList.index.put(hashtext, new Index(chunkSize.get(i), 1));
              fileRecipe.add(hashtext);
            }
          }
        }
      }
      // statistics:
      for (String key : IndexList.index.keySet()) {
        int refCount = IndexList.index.get(key).refCount;
        int chunkSize = IndexList.index.get(key).chunkSize;
        s2 += refCount;
        s1++;
        totalChunks += refCount * chunkSize;
        dedupChunks += chunkSize;
      }
      spaceSaving = 1 - 1.0 * s1 / s2;
      System.out.println("Total number of chunks in storage: " + s2);
      System.out.println("Number of unique chunks in storage: " + s1);
      System.out.println("Number of bytes in storage with deduplication: " + dedupChunks);
      System.out.println("Number of bytes in storage without deduplication: " + totalChunks);
      System.out.println("Space saving: " + spaceSaving);

      FileRecipeList.fileRecipes.put(fileToUpload, fileRecipe);

      indexObjOut.writeObject(IndexList);
      indexObjOut.close();
      indexFileOut.close();

      recipesObjOut.writeObject(FileRecipeList);
      recipesObjOut.close();
      recipesFileOut.close();

      File source = new File(dir.getName() + "/" + indexFileName);

      blockBlobReference.uploadFromFile(source.getAbsolutePath());

      CloudBlockBlob recipeBlockBlobReference = blobContainer.getBlockBlobReference(recipesFileName);
      File recipe = new File(dir.getName() + "/" + recipesFileName);

      recipeBlockBlobReference.uploadFromFile(recipe.getAbsolutePath());
      deleteDir(dir);
    } catch (Exception e) {
        e.printStackTrace();
    }

  }

  public void download(String fileToDownload, String storageType)throws IOException{
    String downloadedFileName = fileToDownload + ".download";
    try {
        FileRecipeList FileRecipeList = new FileRecipeList();
        List<String> fileRecipe = new ArrayList<>();
        IndexList IndexList = new IndexList();
        //for local
        File dir;

        dir = new File("store");
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                System.err.println("Failed to create directory \"store\"");
                System.err.println("Program terminated");
                return;
            }
        }
        if (!dir.isDirectory()) {
            System.err.println("\"store\" is not a directory!");
            return;
        }
        FileInputStream fileIn;
        ObjectInputStream objIn;

        CloudBlockBlob blockBlobReference = blobContainer.getBlockBlobReference(indexFileName);

        if (blockBlobReference.exists()) {
            blockBlobReference.download(new FileOutputStream(dir.getName() + "/" + indexFileName));
        }
        // index file

        File indexFile = new File(dir.getName() + "/" + indexFileName);
        if (indexFile.exists()){
          //System.out.println("index file exisits");
          fileIn = new FileInputStream(indexFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          IndexList = (IndexList) objIn.readObject();
          objIn.close();
          fileIn.close();
        } else {
          indexFile.createNewFile();
        }

        // receipes file

        File recipesFile = new File(dir.getName() + "/" + recipesFileName);
        if (recipesFile.exists()){
          //System.out.println("recipes file exisits");
          fileIn = new FileInputStream(recipesFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          FileRecipeList = (FileRecipeList) objIn.readObject();
          objIn.close();
          fileIn.close();
        } else {
          recipesFile.createNewFile();
        }

        CloudBlockBlob recipeBlockBlobReference = blobContainer.getBlockBlobReference(recipesFileName);

        if (recipeBlockBlobReference.exists()) {
            recipeBlockBlobReference.download(new FileOutputStream(dir.getName() + "/" + recipesFileName));
        }

        FileInputStream fis;
        byte[] fileBytes;
        int bytesRead = 0;
        FileOutputStream fos = new FileOutputStream(new File(downloadedFileName));
        List<String> recipeList = FileRecipeList.fileRecipes.get(fileToDownload);
        for (String chunkName : recipeList) {
            CloudBlockBlob blob = blobContainer.getBlockBlobReference(chunkName);

            File file = new File(dir.getName() + "/" + chunkName);
            blob.downloadToFile(file.getAbsolutePath());
            fis = new FileInputStream(file);
            fileBytes = new byte[(int) file.length()];
            bytesRead = fis.read(fileBytes, 0, (int) file.length());
            fos.write(fileBytes);
            fos.flush();
            fileBytes = null;
            fis.close();
            fis = null;
        }
        fos.close();
        fos = null;
        deleteDir(dir);
    } catch (Exception e) {
        System.err.println(e.getMessage());
    }
  }

  public void delete(String fileToDelete, String storageType) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{
    try {
        FileRecipeList FileRecipeList = new FileRecipeList();
        List<String> fileRecipe = new ArrayList<>();
        IndexList IndexList = new IndexList();
        //for local
        File dir;

        dir = new File("store");
        if (!dir.exists()) {
            if (!dir.mkdir()) {
                System.err.println("Failed to create directory \"store\"");
                System.err.println("Program terminated");
                return;
            }
        }
        if (!dir.isDirectory()) {
            System.err.println("\"store\" is not a directory!");
            return;
        }
        FileInputStream fileIn;
        ObjectInputStream objIn;

        CloudBlockBlob blockBlobReference = blobContainer.getBlockBlobReference(indexFileName);

        if (blockBlobReference.exists()) {
            blockBlobReference.downloadToFile(dir.getName() + "/" + indexFileName);
        }
        File indexFile = new File(dir.getName() + "/" + indexFileName);
        Boolean isNewIndexFile = indexFile.createNewFile();

        FileOutputStream indexFileOut = null;
        ObjectOutputStream indexObjOut = null;
        FileOutputStream recipesFileOut = null;
        ObjectOutputStream recipesObjOut = null;

        if (indexFile.exists()){
          System.out.println("index file exisits");
          fileIn = new FileInputStream(indexFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          IndexList = (IndexList) objIn.readObject();
          objIn.close();
          fileIn.close();
        } else {
          indexFile.createNewFile();
        }
        indexFileOut = new FileOutputStream(indexFile.getAbsolutePath());
        indexObjOut = new ObjectOutputStream(indexFileOut);

        CloudBlockBlob recipeBlockBlobReference = blobContainer.getBlockBlobReference(recipesFileName);

        if (recipeBlockBlobReference.exists()) {
            recipeBlockBlobReference.downloadToFile(dir.getName() + "/" + recipesFileName);
        }
        File recipesFile = new File(dir.getName() + "/" + recipesFileName);
        if (recipesFile.exists()){
          //System.out.println("recipes file exisits");
          fileIn = new FileInputStream(recipesFile.getAbsolutePath());
          objIn = new ObjectInputStream(fileIn);
          FileRecipeList = (FileRecipeList) objIn.readObject();
          objIn.close();
          fileIn.close();
        } else {
          recipesFile.createNewFile();
        }

        recipesFileOut = new FileOutputStream(recipesFile.getAbsolutePath());
        recipesObjOut = new ObjectOutputStream(recipesFileOut);

        List<String> recipeList = FileRecipeList.fileRecipes.get(fileToDelete);

        for (String sha1Hex : recipeList) {
            IndexList.index.get(sha1Hex).refCount -= 1;
            if (IndexList.index.get(sha1Hex).refCount == 0) {
                IndexList.index.remove(sha1Hex);
                CloudBlockBlob blob = blobContainer.getBlockBlobReference(sha1Hex);
                FileRecipeList.fileRecipes.remove(fileToDelete);
                blob.deleteIfExists();
            }
        }

        indexObjOut.writeObject(IndexList);
        indexObjOut.close();
        indexFileOut.close();

        recipesObjOut.writeObject(FileRecipeList);
        recipesObjOut.close();
        recipesFileOut.close();

        CloudBlockBlob indexFileNameBlobReference = blobContainer.getBlockBlobReference(indexFileName);
        File source = new File(dir.getName() + "/" + indexFileName);
        indexFileNameBlobReference.uploadFromFile(source.getAbsolutePath());

        CloudBlockBlob recipesFileNameBlobReference = blobContainer.getBlockBlobReference(recipesFileName);
        File recipe = new File(dir.getName() + "/" + recipesFileName);
        recipesFileNameBlobReference.uploadFromFile(recipe.getAbsolutePath());

        deleteDir(dir);

    } catch (Exception e) {
        System.err.println(e.getMessage());
    }
  }
}

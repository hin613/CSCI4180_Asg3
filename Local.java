import java.io.*;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import java.util.*;
import java.math.BigInteger;
import java.lang.ClassNotFoundException;

public class Local{

  public static final String indexFileName = "mydedup.index";
  public static final String recipesFileName = "fileRecipes.index";

    public Local(){
        File f = new File("store");
        if (!f.exists()){
            f.mkdir();
        }
    }

    public static int exp_mod(int base, int exp, int modulus) {
      base %= modulus;
      int result = 1;
      while (exp > 0) {
        if ((exp & 1) != 0) result = (result * base) % modulus;
        base = (base * base) % modulus;
        exp >>= 1;
      }
      return result;
    }

    public void upload(int minChunk, int avgChunk, int maxChunk, int d, String fileToUpload, String storageType) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{
      File file = new File(fileToUpload);
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

      switch (storageType) {
          case "local": {
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

              FileInputStream fileIn;
              ObjectInputStream objIn;

              // index file

              File indexFile = new File(dir.getName() + "/" + indexFileName);
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

              // receipes file

              File recipesFile = new File(dir.getName() + "/" + recipesFileName);
              if (recipesFile.exists()){
                System.out.println("recipes file exisits");
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
              break;
          }
      }
      int maxBufferSize = (int) Runtime.getRuntime().freeMemory();
      int chunkIterations = (int) Math.ceil((1.0 * fileSize / maxBufferSize));      // count times to chunk large files
      int lastChunkSize = (int) (fileSize % maxBufferSize);
      long totalLogicChunks = 0;
      long totalUniqueChunks = 0;
      long totalLogicFileBytes = 0;
      long totalUniqueFileBytes = 0;
      double spaceSaving = 0;

      for (int iteration = 0; iteration < chunkIterations; iteration++) {
          byte[] fileBytes;
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
          int currentChunkSize = 0;
          int currentPos = 0;
          int fp = 0;
          List<Integer> offset = new ArrayList<>();
          List<Integer> chunkSize = new ArrayList<>();

          while ((currentPos + currentChunkSize <= fileChunkSize) && (currentPos + m <= fileChunkSize)) {
              if (s == 0) {
                  fp = 0;
                  for (int i = 1; i <= m; i++) {
                      fp = (fp + ((int) (fileBytes[currentPos + i - 1] & 0xff) * exp_mod(d, m - i, q))) % q;
                  }
                  currentChunkSize = m;
              } else {
                  fp = (d * (fp - exp_mod(d, m - 1, q) * (int) (fileBytes[currentPos + s - 1] & 0xff)) + fileBytes[currentPos + s + m - 1]) % q;
                  while (fp < 0) {
                      fp += q;
                  }
                  currentChunkSize++;
              }
              if ((fp & 0xFF) == 0 || currentChunkSize == maxChunk || currentPos + currentChunkSize >= fileChunkSize) {
                  offset.add(currentPos);
                  chunkSize.add(currentChunkSize);
                  currentPos += currentChunkSize;
                  s = 0;
                  currentChunkSize = 0;
              } else {
                  s++;
              }
          }
          // handle special case: chunk smaller than m left behind
          if (currentPos < fileChunkSize) {
              offset.add(currentPos);
              chunkSize.add(fileChunkSize - currentPos);
          }
          if (offset.size() == chunkSize.size() && offset.size() > 0) {
              int numOfChunks = offset.size();
              // Do some file settings here
              switch (storageType) {
                  case "local":
                      for (int i = 0; i < numOfChunks; i++) {
                          MessageDigest md = MessageDigest.getInstance("SHA-256");
                          md.update(Arrays.copyOfRange(fileBytes, offset.get(i), offset.get(i) + chunkSize.get(i)));

                          byte[] messageDigest = md.digest();

                          BigInteger no = new BigInteger(1, messageDigest);

                          String hashtext = no.toString(16);

                          while (hashtext.length() < 32) {
                              hashtext = "0" + hashtext;
                          }

                          if (IndexList.index.containsKey(hashtext)) { // already have that chunk, reuse it
                              IndexList.index.get(hashtext).refCount += 1;
                              fileRecipe.add(hashtext);
                          } else {         // if no entry of this chunk in indexTable, do update, create new chunk
                              Index index = new Index();

                              index.chunkSize = chunkSize.get(i);

                              File newChunk = new File(dir.getAbsolutePath() + "/" + hashtext);
                              if (newChunk.createNewFile()) {
                                  // new chunk file created
                                  FileOutputStream tmpOs = new FileOutputStream(newChunk);
                                  tmpOs.write(fileBytes, offset.get(i), chunkSize.get(i));
                                  tmpOs.close();

                                  IndexList.index.put(hashtext, new Index(chunkSize.get(i), 1));
                                  fileRecipe.add(hashtext);
                              } else {
                                  System.err.println("Cannot create chunk file");
                                  return;
                              }
                          }
                      }
                      break;
              }
          }
      }
      // report
      for (String key : IndexList.index.keySet()) {
          int refCount = IndexList.index.get(key).refCount;
          int chunkSize = IndexList.index.get(key).chunkSize;
          totalLogicChunks += refCount;
          totalUniqueChunks++;
          totalLogicFileBytes += refCount * chunkSize;
          totalUniqueFileBytes += chunkSize;
      }
      spaceSaving = 1 - (1.0 * totalUniqueChunks / totalLogicChunks);
      System.out.println("Total number of chunks in storage: " + totalLogicChunks);
      System.out.println("Number of unique chunks in storage: " + totalUniqueChunks);
      System.out.println("Number of bytes in storage with deduplication: " + totalUniqueFileBytes);
      System.out.println("Number of bytes in storage without deduplication: " + totalLogicFileBytes);
      System.out.println("Space saving: " + spaceSaving);

      FileRecipeList.fileRecipes.put(fileToUpload, fileRecipe);

      indexObjOut.writeObject(IndexList);
      indexObjOut.close();
      indexFileOut.close();

      recipesObjOut.writeObject(FileRecipeList);
      recipesObjOut.close();
      recipesFileOut.close();
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

        File indexFile = new File(dir.getName() + "/" + indexFileName);
        Boolean isNewIndexFile = indexFile.createNewFile();


        if (!isNewIndexFile) {
            fileIn = new FileInputStream(indexFile.getAbsolutePath());
            objIn = new ObjectInputStream(fileIn);
            IndexList = (IndexList) objIn.readObject();
            objIn.close();
            fileIn.close();
        }

        File recipesFile = new File(dir.getName() + "/" + recipesFileName);
        Boolean isNewRecipesFile = recipesFile.createNewFile();
        if (!isNewRecipesFile) {
            fileIn = new FileInputStream(recipesFile.getAbsolutePath());
            objIn = new ObjectInputStream(fileIn);
            FileRecipeList = (FileRecipeList) objIn.readObject();
            objIn.close();
            fileIn.close();
        }

        FileInputStream fis;
        byte[] fileBytes;
        int bytesRead = 0;
        FileOutputStream fos = new FileOutputStream(new File(downloadedFileName));
        List<String> recipeList = FileRecipeList.fileRecipes.get(fileToDownload);
        for (String chunkName : recipeList) {
            File file = new File(dir.getName() + "/" + chunkName);
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
      }
      catch (Exception e) {
          System.err.println(e.getMessage());
      }
    }

    public void delete(String fileToDelete, String storageType) throws IOException, NoSuchAlgorithmException, ClassNotFoundException{
      try {
          FileRecipeList FileRecipeList = new FileRecipeList();
          List<String> fileRecipe = new ArrayList<>();
          IndexList IndexList = new IndexList();

          FileOutputStream indexFileOut = null;
          ObjectOutputStream indexObjOut = null;
          FileOutputStream recipesFileOut = null;
          ObjectOutputStream recipesObjOut = null;


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

          // index file

          File indexFile = new File(dir.getName() + "/" + indexFileName);
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

          // receipes file

          File recipesFile = new File(dir.getName() + "/" + recipesFileName);
          if (recipesFile.exists()){
              System.out.println("recipes file exisits");
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

          for (String hashtext : recipeList) {
              IndexList.index.get(hashtext).refCount -= 1;
              System.out.println(IndexList.index.get(hashtext).refCount);
              System.out.println("refCount -= 1");
              if (IndexList.index.get(hashtext).refCount == 0) {
                  IndexList.index.remove(hashtext);

                  File file = new File(dir.getName() + "/" + hashtext);
                  file.delete();
                  System.out.println("file.delete()");
                  FileRecipeList.fileRecipes.remove(fileToDelete);
                  System.out.println("FileRecipeList.fileRecipes is removed");
              }
          }

          indexObjOut.writeObject(IndexList);
          indexObjOut.close();
          indexFileOut.close();

          recipesObjOut.writeObject(FileRecipeList);
          recipesObjOut.close();
          recipesFileOut.close();

      } catch (Exception e) {
        System.out.println("delete error");
          System.err.println(e.getMessage());
      }
    }

}


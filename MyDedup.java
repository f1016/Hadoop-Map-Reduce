import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.BufferedInputStream;
import java.io.File;

import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.blob.*;

import java.security.MessageDigest;

@SuppressWarnings(value={"unchecked", "rawtypes"})
public class MyDedup {
    private static Map<String, ArrayList<String>> metadata;
    private static Map<String, ArrayList<String[]>> fileRecipe;
    private static ArrayList<String[]> store_chunk_index = new ArrayList<String[]>();
    private static final String PATH = "./data/MyDedup.index";
    private static HashMap<Integer, Integer> mod_hm = new HashMap<Integer, Integer>();
    private static String device;
    //AZURE ---------------------------------------
    public static final String storageConnectionString ="=https;AccountName==////=core.windows.net";
    // --------------------------------------- AZURE
/*
    static {
      System.setProperty("https.proxyHost", "proxy.cse.cuhk.edu.hk");
      System.setProperty("https.proxyPort", "8000");
      System.setProperty("http.proxyHost", "proxy.cse.cuhk.edu.hk");
      System.setProperty("http.proxyPort", "8000");
    }
*/

    private static void storeData(String PATH, String file_to_upload) {
        ArrayList<Map> index = new ArrayList<>();
        try {
            FileOutputStream fop = new FileOutputStream(PATH);
            ObjectOutputStream oos = new ObjectOutputStream(fop);

            fileRecipe.put(file_to_upload, store_chunk_index);

            index.add(metadata);
            index.add(fileRecipe);

            if(device.equals("local")){
                oos.writeObject(index);
                oos.close();
            }else{

                FileInputStream fip = new FileInputStream(oos.toString());
                ObjectInputStream ois = new ObjectInputStream(fip);
                CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

                // Create the blob client.
                CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
          
                // Retrieve reference to a previously created container.
                CloudBlobContainer container = blobClient.getContainerReference("mycontainer");
          
                // Create the container if it does not exist.
                container.createIfNotExists();
                CloudBlockBlob blob = container.getBlockBlobReference("Mydedup.index");
                blob.upload(ois, metadata.size() + fileRecipe.size());

            }

        } catch (Exception e) {
            System.out.println("fail");
        }
    }

    @SuppressWarnings("all")
    private static void restoreMetaData(String PATH) throws FileNotFoundException, IOException, ClassNotFoundException {

        File file = new File("./data");
        file.mkdir();

        file = new File(PATH);
        file.createNewFile(); // if file already exists will do nothing


        try {
            FileInputStream fis = new FileInputStream(PATH);
            ObjectInputStream ois = new ObjectInputStream(fis);

            ArrayList<Map> woi = (ArrayList<Map>) ois.readObject();
            
            
            metadata = (HashMap<String, ArrayList<String>>) woi.get(0);
            fileRecipe = (HashMap<String, ArrayList<String[]>>) woi.get(1);
            
        } catch (Exception e) {
            
        }

        if (metadata == null) {
            
            metadata = new HashMap<String, ArrayList<String>>();
            fileRecipe = new HashMap<String, ArrayList<String[]>>();

            ArrayList<String> largest_index = new ArrayList<String>();
            largest_index.add("0");
            metadata.put("largest_index", largest_index);
        }

    }

    private static void chunk(byte[] bytesToChunk, boolean isZeroChunk, int countOfZero, String file_to_upload) {
        try {

            ArrayList<String> chunk_index = new ArrayList<String>();
            ArrayList<String> largest_index = new ArrayList<String>();

            if (isZeroChunk) {


                store_chunk_index.add(new String[] { "-".concat(Integer.toString(countOfZero)), "0" });

            } else {


                MessageDigest md = MessageDigest.getInstance("MD5");
                md.update(bytesToChunk);

                byte[] hc_bytes = md.digest();
                String hc_string = new String(hc_bytes, "ISO-8859-1");

                if (metadata.get(hc_string) == null) { // unique chunk

                    chunk_index.add("data_".concat(Long.toString(Long.parseLong(metadata.get("largest_index").get(0)) + 1)));
                    chunk_index.add("1");
                    chunk_index.add(Long.toString(bytesToChunk.length));
                    metadata.put(hc_string, chunk_index);

                    largest_index.add(Long.toString(Long.parseLong(metadata.get("largest_index").get(0)) + 1));
                    metadata.put("largest_index", largest_index);

                    store_chunk_index.add(new String[] { "data_".concat(chunk_index.get(0).split("data_")[1]), hc_string });

                    uploadChunk(bytesToChunk, chunk_index.get(0));

                } else {
                    // System.out.println(" -------- duplicated chunk ----------");
                    String cid = metadata.get(hc_string).toArray()[0].toString();
                    chunk_index.add(cid);
                    chunk_index.add(Long.toString(Long.parseLong(metadata.get(hc_string).toArray()[1].toString()) + 1));
                    chunk_index.add(Integer.toString(bytesToChunk.length));

                    store_chunk_index.add(new String[] { cid, hc_string });
                    metadata.put(hc_string, chunk_index);

                }

            }

            return;
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void uploadChunk(byte[] bytesToChunk, String cName) throws IOException {
        try {

            // Initialize a pointer
            // in file using OutputStream

            File file = new File("./data/" + cName);
            file.createNewFile();
            FileOutputStream os = new FileOutputStream(file);

            // Starts writing the bytes in it
            os.write(bytesToChunk);

            // Close the file
            os.flush();
            os.close();
        }

        catch (Exception e) {
            System.out.println("Exception: " + e);
        }
    }

    private static void output() {
        String q = "Total number of files that have been stored:";
        String a = "Total number of pre-deduplicated chunks in storage:";
        String b = "Total number of unique chunks in storage:";
        String c = "Total number of bytes of pre-deduplicated chunks in storage:";
        String d = "Total number of bytes of unique chunks in storage:";
        String e = "Deduplication ratio:";
        long total_unique_bytes = 0;
        long total_bytes = 0;
        long total_chunks = 0;

        for (String key : fileRecipe.keySet()) {
            for (String[] chunk : fileRecipe.get(key)) {
                if (chunk[0].contains("data_")) {
                    total_chunks += 1;
                    total_bytes += Integer.parseInt(metadata.get(chunk[1]).get(2));
                } else {
                    total_chunks += 1;
                    total_bytes += Integer.parseInt(chunk[0].split("-")[1]);
                }
            }
        }

        for (String key : metadata.keySet()) {
            if (key.compareTo("largest_index") == 0) {
                continue;
            }
            total_unique_bytes += Integer.parseInt(metadata.get(key).get(2));

        }

        System.out.print(q + " " + fileRecipe.size() + '\n');
        System.out.print(a + " " + total_chunks + '\n');
        System.out.print(b + " " + Integer.toString(metadata.size() - 1) + '\n');
        System.out.print(c + " " + total_bytes + '\n');
        System.out.print(d + " " + total_unique_bytes + '\n');
        double ratio = (double) total_bytes / total_unique_bytes ;
        System.out.print(e + " ");
        System.out.printf("%.2f\n", ratio);

        return;
    }

    private static int mod(int d, int power,int avg_chunk){
        int ans = 0;

        if(mod_hm.get(power) != null) {

            return mod_hm.get(power);
        }
        if(power >= 2){
            ans = d*d %avg_chunk;
            for(int i = 2; i < power; i++){
                ans = ( ans*d )% avg_chunk;
            }
        }else{
            if(power == 0){
                ans = 1;
            }else if (power == 1){
                ans = d % avg_chunk;
            }
        }
        mod_hm.put(power, ans);
        return ans;
    }

    private static void upload(int min_chunk, int avg_chunk, int max_chunk, int d, String file_to_upload) {
        try {

            FileInputStream input = new FileInputStream(file_to_upload);
            BufferedInputStream bis = new BufferedInputStream(input);			

            List<Byte> bytes_list = new ArrayList<Byte>();
            byte[] bytes_arr = {};
            int curr_byte;
            int byte_count = 0;
            int rfp = 0;
            boolean isZeroChunk= false;
            final int interested_RFP = 0; // avg_chunk
            boolean notZeroDuringFirstWindow = false;

            while((curr_byte = bis.read()) != -1){
                
                // chunk.get(shift in 1 digit)
                byte_count++;
                // calculate the rfp 
                if(isZeroChunk){ // refered to the chunk before this byte // start from 6th element
                    if(curr_byte != 0){

                        chunk(null, isZeroChunk, byte_count - 1,file_to_upload);
                        isZeroChunk = false;
                        byte_count = 1;
                        bytes_list.clear();
                        rfp = 0;
                    }
                    if(curr_byte == 0){
                        continue;
                    }
                }

                if(byte_count <= min_chunk && !isZeroChunk){
                    rfp += (curr_byte % avg_chunk * mod(d, min_chunk - byte_count, avg_chunk)) % avg_chunk;
                    rfp %= avg_chunk;
                    if(curr_byte != 0){
                        notZeroDuringFirstWindow = true;
                    }

                    bytes_list.add((byte) curr_byte);
                    if(byte_count == min_chunk){
                        if(rfp == 0 & !notZeroDuringFirstWindow){
                            isZeroChunk = true;
                            notZeroDuringFirstWindow = false;
                            continue;
                        }
                        rfp = rfp % avg_chunk;
                    }
                    continue;
                }else{
                    rfp =  (int) ((((rfp % avg_chunk - ((bytes_list.get(byte_count - min_chunk - 1) % avg_chunk)
                            *  mod(d, min_chunk - 1, avg_chunk)) % avg_chunk) % avg_chunk * d % avg_chunk)
                            % avg_chunk + curr_byte % avg_chunk) % avg_chunk) % avg_chunk;

                    if(rfp < 0){
                        rfp = avg_chunk + rfp;
                    }
                    //((( rfp- (bytes_list.get(byte_count - min_chunk - 1)  * Math.pow(d, min_chunk - 1) ) ) * d )  + curr_byte) % avg_chunk;
                    // ((a mod n - ((b mod n  *c mod n) mod n *d mod n)mod n )mod n + e mod n) mod n
                    //
                    bytes_list.add((byte) curr_byte);
                }


                if(byte_count >= max_chunk && !isZeroChunk){ // if it is not zero chunk and too large then chunk
                    bytes_arr = new byte[bytes_list.size()];
                    for(int i = 0; i < bytes_list.size(); i++) {
                        bytes_arr[i] = bytes_list.get(i).byteValue();
                    }
                    chunk(bytes_arr,false, byte_count, file_to_upload);
                    byte_count = 0;
                    bytes_list.clear();
                    rfp = 0;
                    continue;
                }

                if((rfp & (min_chunk-1)) == interested_RFP && !isZeroChunk){ //  
                    bytes_arr = new byte[bytes_list.size()];
                    for(int i = 0; i < bytes_list.size(); i++) {
                        bytes_arr[i] = bytes_list.get(i).byteValue();
                    }
                    chunk(bytes_arr,false, byte_count, file_to_upload);
                    byte_count = 0;
                    bytes_list.clear();
                    rfp = 0;
                }

        }

        if(bytes_list.size() != 0 ){ // check if there are remaining bytes to chunk
            if(byte_count <= min_chunk){ // first window
                if(rfp == 0){
                    chunk(null, true, byte_count, file_to_upload);
                }else{

                    bytes_arr = new byte[bytes_list.size()];
                    for(int i = 0; i < bytes_list.size(); i++) {
                        bytes_arr[i] = bytes_list.get(i).byteValue();
                    }

                    chunk(bytes_arr,false, byte_count, file_to_upload);
                }


            }else{ // after first window
                if(isZeroChunk){
                    chunk(null, true, byte_count, file_to_upload);
                }else{
                    bytes_arr = new byte[bytes_list.size()];
                    for(int i = 0; i < bytes_list.size(); i++) {
                        bytes_arr[i] = bytes_list.get(i).byteValue();
                    }

                    chunk(bytes_arr,false, byte_count, file_to_upload);
                }
            }
        }
        
        bis.close();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
    public static void download(String file_to_download, String file_name) throws IOException {
        byte[] temp = new byte[4096];

        try{
            File file = new File(file_name);
            file.getParentFile().mkdir();
            file.createNewFile();
            FileOutputStream output = new FileOutputStream(file_name);

            int size;

            for(String[] val : fileRecipe.get(file_to_download)){
                if(val[0].contains("-")){

                    size = Integer.parseInt(val[0].split("-")[1]);

                    output.write(new byte[size]);
                    continue;
                }

                FileInputStream input = new FileInputStream("./data/"+val[0]);

                BufferedInputStream bis = new BufferedInputStream(input);	

                while((size = bis.read(temp)) != -1){
                    output.write(temp,0,size);
                }
                bis.close();
                
            }

            output.close();
            return;
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public static void delete(String file_to_delete){
        ArrayList<String> chunk_index = new ArrayList<String>();


        ArrayList<String[]> fr = fileRecipe.get(file_to_delete);

        try{
            for(String[] curr_chunk : fr){
                if(curr_chunk[0].contains("data_")){

                    chunk_index.add(curr_chunk[0]);
                    chunk_index.add(Long.toString(Long.parseLong(metadata.get(curr_chunk[1]).get(1).toString()) -1));

                    chunk_index.add(metadata.get(curr_chunk[1]).get(2));

                    metadata.put(curr_chunk[1],chunk_index);

                    if(Long.parseLong(chunk_index.get(1)) == 0) {
                        
                        metadata.remove(curr_chunk[1]);
                        File file = new File("./data/"+curr_chunk[0]);
                        file.delete();
                    }
                    chunk_index = new ArrayList<String>();

                }
            }
            
            fileRecipe.remove(file_to_delete);

            FileOutputStream fop=new FileOutputStream(PATH);
            ObjectOutputStream oos=new ObjectOutputStream(fop);

            ArrayList<Map> index=new ArrayList<>();
            index.add(metadata);
            index.add(fileRecipe);
            oos.writeObject(index);
            oos.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean checkSqaure(int n ){
        if (n == 0){
            return false;
        }
        return (int) (Math.ceil((Math.log(n) / Math.log(2)))) == (int) (Math.floor(((Math.log(n) / Math.log(2)))));

    }

    public static void main(String args[]) {
        try {
            String action = args[0].toString();
            restoreMetaData(PATH);

            if(action.compareTo("upload") == 0){
                int min_chunk = Integer.parseInt(args[1]);
                int avg_chunk = Integer.parseInt(args[2]);
                int max_chunk = Integer.parseInt(args[3]);
                int d = Integer.parseInt(args[4]);
                String file_to_upload = args[5].toString();
                device = args[6].toString();

                for(int i = 1 ; i < 3; i++){
                    if(!checkSqaure(Integer.parseInt(args[i]))){
                        System.out.println("parameter of chunk size is not a power of 2");
                        System.exit(0);
                    }
                }

                if(fileRecipe.get(file_to_upload) != null){
                    System.err.println("it has been uploaded already");
                    System.exit(0);
                }

                upload(min_chunk, avg_chunk, max_chunk, d, file_to_upload);

                storeData(PATH, file_to_upload);
                output();   

            }else if(action.compareTo("download") == 0){
                String file_to_download = args[1].toString();
                String file_name = args[2].toString();
                device = args[3].toString();

                if(fileRecipe.get(file_to_download) == null){
                    System.err.println("file not exist");
                    System.exit(0);
                }

                download(file_to_download, file_name);
    
            }else if (action.compareTo("delete") == 0){
                
                String file_to_delete = args[1].toString();
                device = args[2].toString();

                if(fileRecipe.get(file_to_delete) == null){
                    System.err.println("file not exist");
                    System.exit(0);
                }

                delete(file_to_delete);
            }else{
                System.out.println("Wrong action");
                System.exit(0);
            }

        }catch(Exception e){
            System.out.println(e + "it should be input error");
        }

    }

}
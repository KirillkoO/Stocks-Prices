import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.*;

public class Main {

    static final String[] tickets = {"GOOGL","MSFT","AMZN","FB","AAPL","INTC"};
    static final String apikey = "RC4G0FMRBMGYJONK";

    static final String dir = "C:\\Users\\Lenovo\\AnacondaProjects\\data";

    //D:\Learning\Magistracy\Dissertation\Program\Stock Price Download
    //C:\Users\Lenovo\AnacondaProjects\data
    //https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=MSFT&interval=5min&apikey=demo&outputsize=full&datatype=csv


    public static void main(String[] args) throws IOException {

        creatingFiles(tickets, dir);              //check for existing files
        CSVReader reader;
        CSVReader reader1;
        List<String[]> mergeInfo;
        CSVWriter writer;
        for (int i=0; i<tickets.length; i++) {
            downloadPrices(getUrls(tickets)[i],bufferFileName(tickets, dir)[i]);        //download stock data
            try {
                reader = new CSVReader(new FileReader(basicFileName(tickets,dir)[i]));
                reader1 = new CSVReader(new FileReader(bufferFileName(tickets,dir)[i]));
                mergeInfo = mergetwoCSV(reader.readAll(), reader1.readAll());              // merge two csv in one
                writer = new CSVWriter(new FileWriter(basicFileName(tickets,dir)[i]));
                writer.writeAll(mergeInfo, false);
                writer.close();
                if (i<tickets.length-1) {
                    System.out.println("waiting 12 seconds");
                    Thread.sleep(12000);                          //  may only 5 requests per minute
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.out.println("///////////////////////////////////////////////////////////////////////////////////////////////////////");
            }
        }
    }

    private static void downloadPrices(String urll, String fileName){     //download stock prices in the buffer file
        try{
            URL url = new URL(urll);
            ReadableByteChannel rbc = Channels.newChannel(url.openStream());
            FileOutputStream fos = new FileOutputStream(fileName);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            fos.close();
            rbc.close();
            System.out.println("Done for " + url.toString());
        } catch (MalformedURLException e){
            System.out.println("Url does not exist");
        } catch (IOException e) {
            System.out.println("IO Exception");
        }
    }

    private static String[] getUrls(String[] tickers){       //generate URL for provided stocks
        String[] urls = new String[tickers.length];
        for (int i=0; i<tickers.length; i++){
            urls[i] = String.format("https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=%s&interval=5min&apikey=RC4G0FMRBMGYJONK&outputsize=full&datatype=csv", tickers[i]);
        }
        return urls;
    }

    private static List<String[]> mergetwoCSV(List<String[]> first, List<String[]> second) {    //sort + merge two csv files
        List<String> firstDate = getListStrings(first);
        List<String> secondDate = getListStrings(second);
        for(int i=0; i<second.size(); i++) {
            if(!firstDate.contains(secondDate.get(i))){
                first.add(second.get(i));
            }
        }
        Collections.sort(first, new Comparator<String[]>() {
            public int compare(String[] o1, String[] o2) {
                return o1[0].compareTo(o2[0]);
            }
        });
        first.remove(first.size()-1);
        return first;
    }

    private static List<String> getListStrings(List<String[]> list){  //get date from csv
        List<String> listDates = new ArrayList<>();
        for (int i=0; i<list.size(); i++) {
            listDates.add(list.get(i)[0]);
        }
        return listDates;
    }

    private static String[] bufferFileName(String[] tickers, String directory) {       //get buffer file name for provided stocks
        String[] bufFileNames = new String[tickers.length];
        for (int i=0; i<tickers.length; i++) {
            bufFileNames[i] = directory + "\\intraday_5min_" + tickers[i] + "_buf.csv";
        }
        return bufFileNames;
    }

    private static String[] basicFileName(String[] tickers, String directory) {        //get basic file name for provided stocks
        String[] basicFileName = new String[tickers.length];
        for (int i=0; i<tickers.length; i++) {
            basicFileName[i] = directory + "\\intraday_5min_" + tickers[i] + ".csv";
        }
        return basicFileName;
    }

    private static void creatingFiles(String[] tickers, String directory) {    //create missing files
        File dir = new File(directory);
        File[] files = dir.listFiles();
        List<File> existingFiles = Arrays.asList(files);
        List<String> str = new ArrayList<>();
        for (File file : existingFiles) {
            str.add(file.getAbsolutePath());
        }
        for (int i=0; i<tickers.length; i++) {
            if (!str.contains(basicFileName(tickers,directory)[i])){
                File file = new File(basicFileName(tickers,directory)[i]);
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (!str.contains(bufferFileName(tickers,directory)[i])) {
                File file = new File(bufferFileName(tickers,directory)[i]);
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}


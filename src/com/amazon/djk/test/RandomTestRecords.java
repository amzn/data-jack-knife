package com.amazon.djk.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import com.amazon.djk.source.QueueSource;
import com.amazon.djk.core.RecordSource;
import com.amazon.djk.record.FieldType;
import com.amazon.djk.record.Record;
//import com.amazon.djk.record.UnicodeUtilsTest;

public class RandomTestRecords {
    public static final String ASCII_SAMP = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    public static final String NON_ASCII_SAMP = "எண்களுடன்தான் 本質的には数字しか扱うこと υπολογιστές 兩個不同的字元 基本上，计算机只是处理数字。它们指定一المختلفة لتخصيص هذه الأرقام، و لم يكن";
    public static final String SURROGATES_SAMP = "\u24B62\u10437";
    static Random rnd = new Random();

    private final Random rand = new Random();
    private final float maxDepth;
    private final int maxFields;
    private final FieldType[] requiredTypes;
    
    public enum RandomStringType {
        ASCII(ASCII_SAMP), 
        NON_ASCII(NON_ASCII_SAMP),
        SURROGATES(SURROGATES_SAMP);
        
        final String sample;
        private RandomStringType(String sample) {
            this.sample = sample;
        }
        
        public String getSample() {
            return sample;
        }
    }
    
    /**
     * returns a random string of a given kind
     * @param len
     * @param kind
     * @return
     */
    public static String randomString(int len, RandomStringType kind) {
        String sample = kind.getSample();
        StringBuilder sb = new StringBuilder( len );
        for( int i = 0; i < len; i++ ) 
            sb.append( sample.charAt( rnd.nextInt(sample.length()) ) );
        return sb.toString();
    }

    public RandomTestRecords(int maxDepth, FieldType[] requiredTypes, int maxFields) {
        this.maxDepth = maxDepth;
        this.requiredTypes = requiredTypes;
        this.maxFields = maxFields;
    }

    // 0 --> 1.0
    // 1 --> 0.5
    // 2 --> 0.25
    // 3 --> 0.125
    // 4 --> ...
    private float levelProb(int level) {
        return (float)(1.0 / Math.pow(2, (double)level));
    }
    
    public RecordSource getRandomSource(int numRecords) throws IOException {
        QueueSource queue = new QueueSource();
        for (int i = 0; i < numRecords; i++) {
            Record rec = getRandomRecord();
            queue.add(rec, false);
        }
        
        return queue;
    }
    
    public List<Record> getRandomRecords(int numRecords) throws IOException {
        List<Record> recs = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            Record rec = getRandomRecord();
            recs.add(rec);
        }
        
        return recs;
    }
    
    
    /**
     * returns records with random fields of random types (String,Double,Long,Record)
     * of random names.  Also includes requiredFieldTypes, where the names of those
     * fields are a,b,c.  The random field names are guaranteed not intersect with
     * the requiredFieldType field names.
     * 
     * @param requiredTypes
     * @return
     * @throws IOException 
     */
    public Record getRandomRecord() throws IOException {
        return getRandomRecord(0);
    }
    
    private Record getRandomRecord(int level) throws IOException {
        Record rec = new Record();

        // do mandatory fields
        for (int i = 0; i < requiredTypes.length; i++) {
            String name = String.format("%c", (char)((int)'a' + i));
            addRandomField(rec, requiredTypes[i], name); // names starting with 'a'
        } 
        
        // do random fields now
        int numFields = maxFields > 0 ? rand.nextInt(maxFields) : 0;
        for (int i = 0; i < numFields; i++) {
            byte fbyte = (byte)rand.nextInt(5);
            FieldType type = FieldType.getType(fbyte);

            if (type == FieldType.RECORD) {
                if (level < maxDepth && rand.nextFloat() < levelProb(level)) {
                    Record sub = getRandomRecord(level+1);
                    rec.addField(randomName(), sub);
                } // else skip
            } 
            
            else {
                addRandomField(rec, type);                    
            }
        }
        
        return rec;
    }
    
    public void addRandomField(Record rec, FieldType type, String fieldName) throws IOException {
        switch (type) {
        case STRING:
            String svalue = randomString(25, RandomStringType.ASCII);
            rec.addField(fieldName, svalue);
            break;


        case LONG:
            long lvalue = rand.nextLong();
            rec.addField(fieldName, lvalue);
            break;

        case DOUBLE:
            // I don't know why rand.nextDouble() makes this fail on the byte wise equals comparison
            //double dvalue = (double)(value / ((double)Integer.MAX_VALUE/10000.0) ); // but this works
            double dvalue = rand.nextDouble();
            rec.addField(fieldName, dvalue);
            break;
            
        case RECORD:
            throw new RuntimeException("don't call with type = RECORD");
            
        case BOOLEAN:
            int i = rand.nextInt();
            rec.addField(fieldName, i % 2 == 0);
            break;
            
        default:
            throw new RuntimeException("untested type for this test = " + type);
        }
    }
    
    /**
     * limit number of field names to 26 * 26
     * @return
     */
    private String randomName() {
            String name = String.format("%c%c", randomLetter(), randomLetter());
            if (name.contains(",")) {
                System.out.println();
            }
            return name;
    }
    
    private char randomLetter() {
        int i = rand.nextInt(26);
        return (char)((int)'a' + i);
    }
    
    public void addRandomField(Record rec, FieldType type) throws IOException {
        addRandomField(rec, type, randomName());
    }
}

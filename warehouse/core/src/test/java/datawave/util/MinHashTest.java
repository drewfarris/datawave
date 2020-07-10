package datawave.util;

import org.apache.commons.math3.analysis.function.Min;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class MinHashTest {
    
    @Test
    public void testSimilarity() {
        Random r = new Random();
        MinHash a = new MinHash(5000);
        MinHash b = new MinHash(5000);
        for (int i = 0; i < 10000; i++) {
            long rv = r.nextLong();
            switch (i % 3) {
                case 0:
                    a.add(rv, "");
                    break;
                case 1:
                    b.add(rv, "");
                    break;
                case 2:
                    a.add(rv, "");
                    b.add(rv, "");
                    break;
            }
        }
        
        assertEquals(1.0, MinHash.similarity(a, a), 0.0);
        assertEquals(1.0, MinHash.similarity(b, b), 0.0);
        
        // Actual similarity should be 0.33 - but allow a broader bounds becasue we're estimating.
        assertEquals(0.33, MinHash.similarity(a, b), 0.1);
    }
    
    @Test
    public void testSerialization() throws IOException {
        Random r = new Random();
        MinHash a = new MinHash(5000);
        for (int i = 0; i < 10000; i++) {
            a.add(r.nextLong(), "");
        }
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        a.writeTo(bos);
        byte[] ab = bos.toByteArray();
        
        MinHash b = MinHash.readFrom(new ByteArrayInputStream(ab));
        assertEquals(a, b);
    }
}

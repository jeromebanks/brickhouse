package brickhouse.udf.bloom;

import org.apache.hadoop.util.bloom.Filter;
import org.apache.hadoop.util.bloom.Key;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.UUID;

public class BloomTest {


    ///@Test
    public void testBloom() {

        int numElems = 10 * 1000000;
        double pct = 0.01;
        Filter bloom = BloomFactory.NewBloomInstance(numElems, pct);

        for (int i = 0; i < numElems; ++i) {
            UUID uuid = UUID.randomUUID();

            Key key = new Key(uuid.toString().getBytes());
            bloom.add(key);

            Assert.assertTrue(bloom.membershipTest(key));
            if ((i % 10000) == 0) {
                System.out.println(" Added " + i + " elements.");
            }
        }


        int numHits = 0;
        for (int i = 0; i < numElems; ++i) {
            UUID uuid = UUID.randomUUID();
            Key key = new Key(uuid.toString().getBytes());
            if (bloom.membershipTest(key)) {
                numHits++;
            }
        }
        System.out.print("Number of hits = " + numHits + " out of " + numElems + " or " + ((double) numHits / (double) numElems) * 100.0 + " %");
        Assert.assertTrue(numHits / numElems <= pct);
    }

    @Test
    public void testBloomUnion() {

        int numElems = 100000;
        double pct = 0.01;
        HashSet<String> unionMap = new HashSet<String>();

        Filter bloom1 = BloomFactory.NewBloomInstance(numElems, pct);
        for (int i = 0; i < numElems / 2; ++i) {
            UUID uuid = UUID.randomUUID();

            Key key = new Key(uuid.toString().getBytes());
            bloom1.add(key);

            Assert.assertTrue(bloom1.membershipTest(key));
            (unionMap).add(uuid.toString());

            if ((i % 10000) == 0) {
                System.out.println(" Added " + i + " elements.");
            }
        }
        Filter bloom2 = BloomFactory.NewBloomInstance(numElems, pct);

        for (int i = 0; i < numElems / 2; ++i) {
            UUID uuid = UUID.randomUUID();

            Key key = new Key(uuid.toString().getBytes());
            bloom2.add(key);

            Assert.assertTrue(bloom2.membershipTest(key));
            (unionMap).add(uuid.toString());

            if ((i % 10000) == 0) {
                System.out.println(" Added " + i + " elements.");
            }
        }

        bloom1.or(bloom2);

        for (String uuid : unionMap) {
            Assert.assertTrue(bloom1.membershipTest(new Key(uuid.getBytes())));
        }

    }

}

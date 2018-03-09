package brickhouse.udf.xunit;

import org.junit.Assert;
import org.junit.Test;

/**
 * Created by christopherleung on 1/31/18.
 */
public class XunitUDFTest {

    @Test
    public void testGetYPathvalue(){

        GetYPathValueUDF udf = new GetYPathValueUDF();
        Assert.assertEquals("'47", udf.evaluate("brand", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com"));
        Assert.assertEquals("nordstrom.com", udf.evaluate("domain", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com"));
        Assert.assertEquals("Women's Clothing > Socks & Hosiery", udf.evaluate("category", "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com"));

        Assert.assertEquals("AMMO CAN MAN, LLC", udf.evaluate("brand", "/brand/brand=AMMO CAN MAN_ LLC,/category/top_level=Sports_ Fitness_ & Outdoors/level1=Hunting/level2=Knives & Tools,/domain/domain=amazon.com"));
        Assert.assertEquals("Sports, Fitness, & Outdoors > Hunting > Knives & Tools", udf.evaluate("category", "/brand/brand=AMMO CAN MAN_ LLC,/category/top_level=Sports_ Fitness_ & Outdoors/level1=Hunting/level2=Knives & Tools,/domain/domain=amazon.com"));

    }

    @Test
    public void testGetAllYPaths(){
        GetAllYPathsUDF udf = new GetAllYPathsUDF();
        Assert.assertEquals("brand,category,domain", udf.evaluate( "/brand/brand='47,/category/top_level=Women's Clothing/level1=Socks & Hosiery,/domain/domain=nordstrom.com"));


    }

    @Test
    public void testParseXUnit() {
        String xunitStr =  "/age/bucket=18-24,/domain/domain=mercadolibre.cl,/product/id=452597691";

        XUnitDesc xunit = XUnitDesc.ParseXUnit( xunitStr);

        for(YPathDesc yp : xunit.getYPaths()) {
           System.out.println(" YP = " + yp) ;
           System.out.println(" YP DIM = " + yp.getDimName() + " NUM ATTR "  + yp.getAttributeNames().length);
           for(int i = 0; i< yp.getAttributeNames().length; ++i) {
              System.out.println(" ATTR " +yp.getAttributeNames()[i] + " = " + yp.getAttributeValues()[i]) ;
           }
        }


        System.out.println(" XUNIT = " + xunit);
    }

}

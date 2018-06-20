package test;

import com.ming.common.utils.PropertiesUtils;
import com.ming.common.utils.StringUtils;
import org.junit.Test;

/**
 * Created by ming on 18-6-19.
 */
public class TestUtils {
    @Test
    public void testProperties(){
        String mysql = PropertiesUtils.getProperties("mysql", "resource.properties");
        StringUtils.isEmpty(mysql);
        System.out.println(mysql);
    }
}

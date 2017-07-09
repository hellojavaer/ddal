package org.hellojavaer.ddal.spring.scan;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * 
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 21/06/2017.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/spring.xml" })
public class EnableShardRouteAnnotationTest {

    @Autowired
    private ShardRouteTestComponent shardRouteTestComponent;

    @Test
    public void test() {
        for (int i = 0; i < 100; i++) {
            shardRouteTestComponent.route((long) i);
        }
    }

}

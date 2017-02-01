package org.hellojavaer.ddal.ddr.expression.formate;

import org.hellojavaer.ddal.ddr.expression.format.ast.token.FeToken;
import org.hellojavaer.ddal.ddr.expression.format.ast.token.FeTokenParser;
import org.hellojavaer.ddal.ddr.expression.format.ast.token.FeTokenType;
import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 19/11/2016.
 */
public class FeTokenParserTest {

    @Test
    public void test(){
        FeTokenParser parser =new FeTokenParser("tab_{$0:'%4s'}_name");
        for(;;){
            FeToken token =parser.next();
            if(token.getType() == FeTokenType.NULL){
                break;
            }else {
                System.out.println(token.getType()+"&"+token.getData());
            }
        }
    }
}

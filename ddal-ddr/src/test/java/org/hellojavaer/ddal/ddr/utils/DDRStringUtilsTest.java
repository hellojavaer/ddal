package org.hellojavaer.ddal.ddr.utils;

import org.junit.Test;

/**
 *
 * @author <a href="mailto:hellojavaer@gmail.com">Kaiming Zou</a>,created on 23/12/2016.
 */
public class DDRStringUtilsTest {

    @Test
    public void trim() {
        Assert.isTrue(DDRStringUtils.trimToNull(null) == null);
        Assert.isTrue(DDRStringUtils.trimToNull("") == null);
        Assert.isTrue(DDRStringUtils.trimToNull(" ") == null);
        Assert.isTrue(DDRStringUtils.trimToNull("   ") == null);
        Assert.isTrue(DDRStringUtils.trimToNull("   ") == null);
        Assert.equals(DDRStringUtils.trimToNull("  ab "), "ab");
        Assert.equals(DDRStringUtils.trimToNull("ab"), "ab");
        Assert.isTrue(!DDRStringUtils.trimToNull("  aB ").equals("ab"));
    }

    @Test
    public void toLowerCase() {
        Assert.isTrue(DDRStringUtils.toLowerCase(null) == null);
        Assert.isTrue(DDRStringUtils.toLowerCase("") == null);
        Assert.isTrue(DDRStringUtils.toLowerCase(" ") == null);
        Assert.isTrue(DDRStringUtils.toLowerCase("   ") == null);
        Assert.isTrue(DDRStringUtils.toLowerCase("   ") == null);
        Assert.equals(DDRStringUtils.toLowerCase("  ab "), "ab");
        Assert.equals(DDRStringUtils.toLowerCase("ab"), "ab");
        Assert.equals(DDRStringUtils.toLowerCase("  aB "), "ab");
        Assert.isTrue(!DDRStringUtils.trimToNull("aB").equals("ac"));
    }

}

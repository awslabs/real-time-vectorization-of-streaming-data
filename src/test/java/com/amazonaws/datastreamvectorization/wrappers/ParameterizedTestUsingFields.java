package com.amazonaws.datastreamvectorization.wrappers;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class ParameterizedTestUsingFields {
    // fields used together with @Parameter must be public
    @Parameterized.Parameter(0)
    public int m1;
    @Parameterized.Parameter(1)
    public int m2;
    @Parameterized.Parameter(2)
    public int result;


    // creates the test data
    @Parameterized.Parameters(name = "{index}: Test with m1={0}, m2 ={1}, result is:{2} ")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][]{
                {1, 2, 2},
                {5, 3, 15},
                {121, 4, 484}
        };
        return Arrays.asList(data);
    }

    @Test
    public void testMultiplyException() {
        ToTestClass tester = new ToTestClass();
        assertEquals("Result", result, tester.multiply(m1, m2));
    }


    // class to be tested
    class ToTestClass {
        public int multiply(int i, int j) {
            return i * j;
        }
    }

}

package com.netflix.hollow.test;

import org.junit.jupiter.api.Assertions;

public class AssertShim {

    public static void fail(String message) {
        Assertions.fail(message);
    }
    public static void fail() {
        Assertions.fail();
    }

    public static void assertNull(Object actual) {
        Assertions.assertNull(actual);
    }
    public static void assertNull(String message, Object actual) {
        Assertions.assertNull(actual, message);
    }

    public static void assertNotNull(Object actual) {
        Assertions.assertNotNull(actual);
    }
    public static void assertNotNull(String message, Object actual) {
        Assertions.assertNotNull(actual, message);
    }

    public static void assertTrue(String message, boolean condition) {
        Assertions.assertTrue(condition, message);
    }
    public static void assertTrue(boolean condition) {
        Assertions.assertTrue(condition);
    }

    public static void assertFalse(String message, boolean condition) {
        Assertions.assertFalse(condition, message);
    }
    public static void assertFalse(boolean condition) {
        Assertions.assertFalse(condition);
    }

    public static void assertEquals(String message, Object expected, Object actual) {
        Assertions.assertEquals(expected, actual, message);
    }
    public static void assertEquals(Object expected, Object actual) {
        Assertions.assertEquals(expected, actual);
    }

    public static void assertEquals(String message, int expected, int actual) {
        Assertions.assertEquals(expected, actual, message);
    }
    public static void assertEquals(int expected, int actual) {
        Assertions.assertEquals(expected, actual);
    }

    public static void assertEquals(String message, long expected, long actual) {
        Assertions.assertEquals(expected, actual, message);
    }
    public static void assertEquals(long expected, long actual) {
        Assertions.assertEquals(expected, actual);
    }

    public static void assertEquals(float expected, float actual, float delta) {
        Assertions.assertEquals(expected, actual, delta);
    }

    public static void assertEquals(double expected, double actual, double delta) {
        Assertions.assertEquals(expected, actual, delta);
    }

    public static void assertNotEquals(String message, Object expected, Object actual) {
        Assertions.assertNotEquals(expected, actual, message);
    }
    public static void assertNotEquals(Object expected, Object actual) {
        Assertions.assertNotEquals(expected, actual);
    }

    public static void assertNotEquals(String message, int expected, int actual) {
        Assertions.assertNotEquals(expected, actual, message);
    }
    public static void assertNotEquals(int expected, int actual) {
        Assertions.assertNotEquals(expected, actual);
    }

    public static void assertNotEquals(String message, long expected, long actual) {
        Assertions.assertNotEquals(expected, actual, message);
    }
    public static void assertNotEquals(long expected, long actual) {
        Assertions.assertNotEquals(expected, actual);
    }
    
    
    public static void assertArrayEquals(String message, Object[] expected, Object[] actual) {
        Assertions.assertArrayEquals(expected, actual, message);
    }
    public static void assertArrayEquals(Object[] expected, Object[] actual) {
        Assertions.assertArrayEquals(expected, actual);
    }

    public static void assertArrayEquals(String message, int[] expected, int[] actual) {
        Assertions.assertArrayEquals(expected, actual, message);
    }
    public static void assertArrayEquals(int[] expected, int[] actual) {
        Assertions.assertArrayEquals(expected, actual);
    }

    public static void assertArrayEquals(String message, long[] expected, long[] actual) {
        Assertions.assertArrayEquals(expected, actual, message);
    }
    public static void assertArrayEquals(long[] expected, long[] actual) {
        Assertions.assertArrayEquals(expected, actual);
    }
}

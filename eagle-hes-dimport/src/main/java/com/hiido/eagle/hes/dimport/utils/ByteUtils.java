package com.hiido.eagle.hes.dimport.utils;

import java.nio.charset.Charset;

public class ByteUtils {
	
	private static final String UTF8_ENCODING = "UTF-8";

  private static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);
	
	public static byte[] toBytes(long val) {
	    byte [] b = new byte[8];
	    for (int i = 7; i > 0; i--) {
	      b[i] = (byte) val;
	      val >>>= 8;
	    }
	    b[0] = (byte) val;
	    return b;
	  }
	
	
	public static byte [] toBytes(final double d) {
	    // Encode it as a long
	    return ByteUtils.toBytes(Double.doubleToRawLongBits(d));
	  }
	
	public static byte [] toBytes(final float f) {
	    // Encode it as int
	    return ByteUtils.toBytes(Float.floatToRawIntBits(f));
	  }
	
	public static byte[] toBytes(int val) {
	    byte [] b = new byte[4];
	    for(int i = 3; i > 0; i--) {
	      b[i] = (byte) val;
	      val >>>= 8;
	    }
	    b[0] = (byte) val;
	    return b;
	  }
	
	public static byte[] toBytes(String s) {
	    return s.getBytes(UTF8_CHARSET);
	  }

}

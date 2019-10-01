package com.epam.dataflow.util;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class CryptoUtils {

    private Cipher cipher;

    private CryptoUtils() {}

    public static byte[] encrypt(byte[] input, SecretKeySpec key, String cipherAlgorithm)
            throws Exception {

        Cipher cipher = Cipher.getInstance(cipherAlgorithm);
        cipher.init(Cipher.ENCRYPT_MODE, key);
        return cipher.doFinal(input);
    }

    public static byte[] decrypt(byte[] input, SecretKeySpec key, String cipherAlgorithm)
            throws Exception {

        Cipher cipher = Cipher.getInstance(cipherAlgorithm);
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(input);
    }

    public static byte[] CISEncrypt(byte[] input, SecretKeySpec key) throws Exception {
        Cipher aes = Cipher.getInstance("AES/ECB/PKCS5Padding");
        aes.init(Cipher.ENCRYPT_MODE, key);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CipherOutputStream out = new CipherOutputStream(baos, aes);
        out.write(input);
        out.flush();
        out.close();
        return baos.toByteArray();
    }

    public static byte[] CISDecrypt(byte[] input, SecretKeySpec key) throws Exception {
        Cipher aes = Cipher.getInstance("AES/ECB/PKCS5Padding");
        aes.init(Cipher.DECRYPT_MODE, key);
        ByteArrayInputStream bios = new ByteArrayInputStream(input);
        CipherInputStream in = new CipherInputStream(bios, aes);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] b = new byte[1024];
        int numberOfBytedRead;
        while ((numberOfBytedRead = in.read(b)) >= 0) {
            baos.write(b, 0, numberOfBytedRead);
        }
        in.close();
        return baos.toByteArray();
    }

}

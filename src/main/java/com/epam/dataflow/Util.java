package com.epam.dataflow;


import com.google.cloud.kms.v1.*;
import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

import javax.crypto.Cipher;
import javax.crypto.spec.OAEPParameterSpec;
import javax.crypto.spec.PSource;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.X509EncodedKeySpec;

public class Util {

	private Util() {}

	/**
	 * Encrypts the given plaintext using the specified crypto key.
	 */
	public static byte[] encrypt(
			String projectId, String locationId, String keyRingId, String cryptoKeyId, byte[] plaintext)
			throws IOException {

		// Create the KeyManagementServiceClient using try-with-resources to manage client cleanup.
		try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {

			// The resource name of the cryptoKey
			String resourceName = CryptoKeyName.format(projectId, locationId, keyRingId, cryptoKeyId);

			// Encrypt the plaintext with Cloud KMS.
			EncryptResponse response = client.encrypt(resourceName, ByteString.copyFrom(plaintext));

			// Extract the ciphertext from the response.
			return response.getCiphertext().toByteArray();
		}
	}

	/**
	 * Decrypts the provided ciphertext with the specified crypto key.
	 */
	public static byte[] decrypt(
			String projectId, String locationId, String keyRingId, String cryptoKeyId, byte[] ciphertext)
			throws IOException {

		// Create the KeyManagementServiceClient using try-with-resources to manage client cleanup.
		try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {

			// The resource name of the cryptoKey
			String resourceName = CryptoKeyName.format(projectId, locationId, keyRingId, cryptoKeyId);

			// Decrypt the ciphertext with Cloud KMS.
			DecryptResponse response = client.decrypt(resourceName, ByteString.copyFrom(ciphertext));

			// Extract the plaintext from the response.
			return response.getPlaintext().toByteArray();
		}
	}

	public static byte[] encryptRSA(String keyName, byte[] plaintext)
			throws IOException, GeneralSecurityException {
		// Create the Cloud KMS client.
		try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
			// Get the public key
			com.google.cloud.kms.v1.PublicKey pub = client.getPublicKey(keyName);
			String pemKey = pub.getPem();
			pemKey = pemKey.replaceFirst("-----BEGIN PUBLIC KEY-----", "");
			pemKey = pemKey.replaceFirst("-----END PUBLIC KEY-----", "");
			pemKey = pemKey.replaceAll("\\s", "");
			byte[] derKey = BaseEncoding.base64().decode(pemKey);
			X509EncodedKeySpec keySpec = new X509EncodedKeySpec(derKey);
			PublicKey rsaKey = KeyFactory.getInstance("RSA").generatePublic(keySpec);

			// Encrypt plaintext for the 'RSA_DECRYPT_OAEP_2048_SHA256' key.
			// For other key algorithms:
			// https://docs.oracle.com/javase/7/docs/api/javax/crypto/Cipher.html
			Cipher cipher = Cipher.getInstance("RSA/ECB/OAEPWithSHA-256AndMGF1Padding");
			OAEPParameterSpec oaepParams = new OAEPParameterSpec(
					"SHA-256", "MGF1", MGF1ParameterSpec.SHA256, PSource.PSpecified.DEFAULT);
			cipher.init(Cipher.ENCRYPT_MODE, rsaKey, oaepParams);

			return cipher.doFinal(plaintext);
		}
	}

	public static byte[] decryptRSA(String keyName, byte[] ciphertext) throws IOException {
		// Create the Cloud KMS client.
		try (KeyManagementServiceClient client = KeyManagementServiceClient.create()) {
			AsymmetricDecryptResponse response = client.asymmetricDecrypt(
					keyName, ByteString.copyFrom(ciphertext));
			return response.getPlaintext().toByteArray();
		}
	}

}

package com.howtoprogram.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.kms.AWSKMS;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.kms.model.EncryptRequest;
import com.amazonaws.services.kms.model.ListKeysRequest;
import com.amazonaws.services.kms.model.ListKeysResult;
import com.amazonaws.util.Base64;

public class ProducerTest {
	
	
 
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "l5222t.sss.se.scania.com:9092,l5223t.sss.se.scania.com:9092,l5224t.sss.se.scania.com:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    Producer<String, String> producer = null;
    try {
      producer = new KafkaProducer<>(props);
      for (int i = 0; i < 100; i++) {
        String msg = "Message " + i;
        
        System.out.println("Sent:" + msg);
        String encryptedMsg = encrypt(msg);
        producer.send(new ProducerRecord<String, String>("HelloKafkaTopic", encryptedMsg));
        System.out.println("encryptedMsg:" + encryptedMsg);
      }
    } catch (Exception e) {
      e.printStackTrace();

    } finally {
      producer.close();
    }

  }
  
 
  public static String encrypt(String msg) { 
	   String accessKey = "ASIAIAMRWTOBXJ76TUMA"; 
	     String secretKey = "lmUG0Fu5mcL91DEh5EG/41FrLmZq/9ALttkFfjPp"; 
	     String endpoint = "https://kms.eu-west-1.amazonaws.com"; 
	     String masterKeyId = "arn:aws:kms:eu-west-1:758105706512:key/0bd130c5-5f28-4e04-85ec-c7b90f30afb9"; 
	     EndpointConfiguration endpointConfiguration = new EndpointConfiguration(endpoint,"eu-west-1");
	     BasicAWSCredentials creds = new BasicAWSCredentials(accessKey, secretKey);
	    // AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).build();
	     
	     Integer limit = 10;
	     String marker = null;

	     ListKeysRequest req1 = new ListKeysRequest().withMarker(marker).withLimit(limit);
	     
	     
      AWSKMS kms = AWSKMSClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(creds)).withEndpointConfiguration(endpointConfiguration).build();// defaultClient();  
      //ListKeysResult result = kms.listKeys(req1);
     // System.out.println(".................."+result);
      ByteBuffer plaintext = ByteBuffer.wrap(msg.getBytes()); 
       
      EncryptRequest req = new EncryptRequest().withKeyId(masterKeyId).withPlaintext(plaintext); 
      //req.setKeyId(masterKeyId); 
      ByteBuffer encryptedKey = kms.encrypt(req).getCiphertextBlob(); 
       
      byte[] key = new byte[encryptedKey.remaining()]; 
      encryptedKey.get(key); 
       byte[] encodeVal = Base64.encode(key);
       String valString = null;
      try {
    	  valString = new String(encodeVal, "UTF-8");
	} catch (UnsupportedEncodingException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}; 
	return valString ;
  } 
/*
  public static String KMSEncrypt() {
	  

String keyId = "arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab";
ByteBuffer plaintext = ByteBuffer.wrap(new byte[]{1,2,3,4,5,6,7,8,9,0});

EncryptRequest req = new EncryptRequest().withKeyId(keyId).withPlaintext(plaintext);
ByteBuffer ciphertext = kms.encrypt(req).getCiphertextBlob();
  }*/
}

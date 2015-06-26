package com.regus.mail.processor;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import javax.mail.*;
import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class MailProcessor {
    public static void main(String[] args) {

        try {
            Properties props = new Properties();
            props.setProperty("mail.store.protocol", "imap");
            props.setProperty("mail.imap.ssl.enable", "true");
            props.setProperty("mail.imap.port", "993");
            props.setProperty("mail.imap.socketFactory.port", "993");
            props.setProperty("mail.imap.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
            props.setProperty("mail.imap.socketFactory.fallback", "false");

            Session session = Session.getInstance(props, null);
            Store store = session.getStore("imap");
            Properties mailCredentials = new Properties();
            mailCredentials.load(new FileInputStream(System.getProperty("user.home") + File.separator + "mail.credentials.txt"));
            store.connect(mailCredentials.getProperty("host"), mailCredentials.getProperty("user"), mailCredentials.getProperty("password"));
            Folder formFolder  = store.getFolder("INBOX/Form Fills");
            formFolder.open(Folder.READ_WRITE);
            Folder processedFolder =  store.getFolder("INBOX/Form Fills/Processed");

            System.out.println(formFolder.getMessageCount());

            MongoClient mongo = new MongoClient("localhost", 27017);
            DB regus_analytics_db = mongo.getDB("regus_analytics");

            DBCollection regusMail = regus_analytics_db.getCollection("mail");

            Message msg;
            String subject, country, product;
            Date sentDate;

            Map<String, String> formValues = new HashMap<String, String>();

            for (int msgNum = 1; msgNum <= formFolder.getMessageCount(); msgNum++) {
                msg = formFolder.getMessage(msgNum);
                subject = msg.getSubject();
                sentDate = msg.getSentDate();
                String cont = msg.getContent().toString();

                if (subject.contains("Canada")) {
                    country = "Canada";
                } else if (subject.contains("United States of America")) {
                    country = "US";
                } else {
                    country = "Unknown";
                }


                if (subject.contains("SEM VO")) {
                    product = "Virtual Office";
                } else if (subject.contains("SEM OF")) {
                    product = "Office";
                } else {
                    product = "Unknown";
                }

                formValues.put("product", product);
                formValues.put("country", country);
                final String lines[] = cont.split("\n");

                for (String line : lines) {
                    if (line.length() > 0 && line.contains(":") && !line.startsWith("NOTE TO")) {
                        if (line.startsWith("The following inquiry")) {
                            int httpIndex = line.indexOf("http");
                            String[] split = line.substring(httpIndex).split("\\?");
                            String URL = split[0];
                            String reqParameters = split[1];
                            formValues.put("url", URL);
                            formValues.put("urlParameters", reqParameters);
                        } else {
                            String nameValue[] = line.split(":");
                            formValues.put(nameValue[0], nameValue[1]);
                        }
                    }
                }

                formValues.put("sentDate", new SimpleDateFormat("yyyy/MM/dd").format(sentDate));
                formValues.put("sentTime", new SimpleDateFormat("hh:mm:ss").format(sentDate));
                BasicDBObject objectToInsert = new BasicDBObject(formValues);
                regusMail.insert(objectToInsert);

                Message[] msgs = {msg};
                formFolder.copyMessages(msgs, processedFolder);
                msg.setFlag(Flags.Flag.DELETED, true);
            }

            formFolder.close(true);
            store.close();
        } catch (Exception mex) {
            mex.printStackTrace();
        }
    }

}

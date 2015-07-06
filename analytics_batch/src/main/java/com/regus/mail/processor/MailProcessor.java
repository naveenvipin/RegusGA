package com.regus.mail.processor;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

import javax.mail.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class MailProcessor {
    public static void main(String[] args) throws MessagingException {

        Store store = null;
        Folder formFolder = null;
        Folder processedFolder = null;
        try {
            Session session = getSession();
            store = session.getStore("imap");
            Properties mailCredentials = new Properties();
            mailCredentials.load(new FileInputStream(System.getProperty("user.home") + File.separator + "mail.credentials.txt"));
            store.connect(mailCredentials.getProperty("host"), mailCredentials.getProperty("user"), mailCredentials.getProperty("password"));
            formFolder = store.getFolder("INBOX/Form Fills/To Process");
            formFolder.open(Folder.READ_WRITE);
            processedFolder = store.getFolder("INBOX/Form Fills/Processed");
            System.out.println(formFolder.getMessageCount());
            processMessages(formFolder, processedFolder);

        } catch (Exception mex) {
            mex.printStackTrace();
        } finally {
            if (formFolder != null) {
                formFolder.close(true);
            }
            if (store != null) {
                store.close();
            }
            if (processedFolder != null) {
                processedFolder.close(true);
            }
        }
    }

    private static Session getSession() {
        Properties props = new Properties();
        props.setProperty("mail.store.protocol", "imap");
        props.setProperty("mail.imap.ssl.enable", "true");
        props.setProperty("mail.imap.port", "993");
        props.setProperty("mail.imap.socketFactory.port", "993");
        props.setProperty("mail.imap.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
        props.setProperty("mail.imap.socketFactory.fallback", "false");

        return Session.getInstance(props, null);
    }

    private static void processMessages(Folder formFolder, Folder processedFolder) throws MessagingException, IOException {
        MongoClient mongo = new MongoClient("localhost", 27017);
        DB regus_analytics_db = mongo.getDB("regus_analytics");
        DBCollection regusMail = regus_analytics_db.getCollection("mail");

        for (int msgNum = 1; msgNum <= formFolder.getMessageCount(); msgNum++) {
            Map<String, String> formValues = new HashMap<String, String>();
            Message msg = formFolder.getMessage(msgNum);
            formValues.putAll(getSentDateAndTime(msg));
            formValues.putAll(getCountryAndProductFromMsgSubject(msg.getSubject()));
            formValues.putAll(processMessageContent(msg));


            BasicDBObject objectToInsert = new BasicDBObject(formValues);
            regusMail.insert(objectToInsert);

            Message[] msgs = {msg};
            formFolder.copyMessages(msgs, processedFolder);
            msg.setFlag(Flags.Flag.DELETED, true);
        }
    }

    private static Map<String, String> getSentDateAndTime(Message msg) throws MessagingException {
        Map<String, String> formValues = new HashMap<String, String>();
        Date sentDate = msg.getSentDate();

        formValues.put("sentDate", new SimpleDateFormat("yyyy/MM/dd").format(sentDate));
        formValues.put("sentTime", new SimpleDateFormat("hh:mm:ss").format(sentDate));
        return formValues;
    }

    private static Map<String, String> processMessageContent(Message msg) {
        String[] lines;
        boolean isQuestionBody = false;
        StringBuilder questionBody = new StringBuilder();
        Map<String, String> formValues = new HashMap<String, String>();

        try {
            String cont = msg.getContent().toString();
            lines = cont.split("\n");

        } catch (Exception e) {
            System.out.println("Error reading message content");
            e.printStackTrace();
            return formValues;
        }

        for (String line : lines) {
            try {
                if (isQuestionBody) {
                    if (line.startsWith("How did you hear about Regus")) {
                        // reached the end of question body
                        isQuestionBody = false;
                        formValues.put("Question", questionBody.toString());
                    } else {
                        questionBody.append(line);
                        continue;
                    }
                }
                if (line.length() > 0 && line.contains(":") && !line.startsWith("NOTE TO")) {
                    if (line.startsWith("The following inquiry")) {
                        int httpIndex = line.indexOf("http");
                        String[] split = line.substring(httpIndex).split("\\?");
                        String URL = split[0];
                        String reqParameters = "No Parameter";
                        if (split.length > 1) {
                            reqParameters = split[1];
                        }
                        formValues.put("url", URL);
                        formValues.put("urlParameters", reqParameters);

                    } else if (line.startsWith("Question")) {
                        // this line and following lines until 'How did you hear' belong to question body
                        isQuestionBody = true;
                        String[] split = line.split(":", 2);
                        if (split.length > 1) {
                            questionBody.append(split[1]);
                        }
                    } else {
                        String nameValue[] = line.split(":");
                        formValues.put(nameValue[0], nameValue[1]);
                    }
                }
            } catch (Exception e) {
                System.out.println("Error processing message lines .. continuing");
                e.printStackTrace();
            }
        }
        return formValues;
    }

    private static Map<String, String> getCountryAndProductFromMsgSubject(String subject) {
        String country, product;
        Map<String, String> formValues = new HashMap<String, String>();
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
        return formValues;
    }

}

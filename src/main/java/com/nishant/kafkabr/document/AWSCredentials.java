package com.nishant.kafkabr.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "AWSCredentials")
public class AWSCredentials {
    @Id
    private String id;
    private String ACCESS_ID;
    private String SECURITY_KEY;

    public AWSCredentials(String ACCESS_ID, String SECURITY_KEY) {
        this.ACCESS_ID = ACCESS_ID;
        this.SECURITY_KEY = SECURITY_KEY;
    }

    public void setACCESS_ID(String ACCESS_ID) {
        this.ACCESS_ID = ACCESS_ID;
    }

    public void setSECURITY_KEY(String SECURITY_KEY) {
        this.SECURITY_KEY = SECURITY_KEY;
    }

    public String getACCESS_ID() {
        return ACCESS_ID;
    }

    public String getSECURITY_KEY() {
        return SECURITY_KEY;
    }

    @Override
    public String toString() {
        return "AWSCredentials{" +
                "id='" + id + '\'' +
                ", ACCESS_ID='" + ACCESS_ID + '\'' +
                ", SECURITY_KEY='" + SECURITY_KEY + '\'' +
                '}';
    }
}

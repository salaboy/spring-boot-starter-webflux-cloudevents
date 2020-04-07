package com.salaboy.cloudevents.helper;

import io.cloudevents.CloudEvent;
import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Map;

public class CloudEventsHelper {

    public static final String CE_ID = "Ce-Id";
    public static final String CE_TYPE = "Ce-Type";
    public static final String CE_SOURCE = "Ce-Source";
    public static final String CE_SPECVERSION = "Ce-Specversion";
    public static final String CE_TIME = "Ce-Time";
    public static final String CE_SUBJECT = "Ce-Subject";

    public static final String APPLICATION_JSON = "application/json";
    public static final String CONTENT_TYPE = "Content-Type";


    public static CloudEvent<AttributesImpl, String> parseFromRequest(Map<String, String> headers, Object body) throws IllegalStateException {
        return parseFromRequestWithExtension(headers, body, null);
    }


    public static CloudEvent<AttributesImpl, String> parseFromRequestWithExtension(Map<String, String> headers, Object body, ExtensionFormat ef){
        if (headers.get(CE_ID) == null || (headers.get(CE_SOURCE) == null || headers.get(CE_TYPE) == null)) {
            throw new IllegalStateException("Cloud Event required fields are not present.");
        }

        CloudEventBuilder<String> builder = CloudEventBuilder.<String>builder()
                .withId(headers.get(CE_ID))
                .withType(headers.get(CE_TYPE))
                .withSource((headers.get(CE_SOURCE) != null) ? URI.create(headers.get(CE_SOURCE)) : null)
                .withTime((headers.get(CE_TIME) != null) ? ZonedDateTime.parse(headers.get(CE_TIME)) : null)
                .withData((body != null) ? body.toString() : "")
                .withSubject(headers.get(CE_SUBJECT))
                .withDatacontenttype((headers.get(CONTENT_TYPE) != null) ? headers.get(CONTENT_TYPE) : APPLICATION_JSON);

        if(ef != null){
            builder = builder.withExtension(ef);
        }
        return  builder.build();
    }

    public static WebClient.ResponseSpec createPostCloudEvent(WebClient webClient, CloudEvent<AttributesImpl, String> cloudEvent) {
        return createPostCloudEvent(webClient,"", cloudEvent);
    }

    public static WebClient.ResponseSpec createPostCloudEvent(WebClient webClient, String uriString, CloudEvent<AttributesImpl, String> cloudEvent) {
        WebClient.RequestBodySpec uri = webClient.post().uri(uriString);
        WebClient.RequestHeadersSpec<?> headersSpec = uri.body(BodyInserters.fromValue(cloudEvent.getData()));
        AttributesImpl attributes = cloudEvent.getAttributes();
        WebClient.RequestHeadersSpec<?> header = headersSpec
                .header(CE_ID, attributes.getId())
                .header(CE_SPECVERSION, attributes.getSpecversion())
                .header(CONTENT_TYPE, APPLICATION_JSON)
                .header(CE_TYPE, attributes.getType())
                .header(CE_TIME, (attributes.getTime().isPresent()) ? attributes.getTime().get().toString() : "")
                .header(CE_SOURCE, (attributes.getSource() != null) ? attributes.getSource().toString() : "")
                .header(CE_SUBJECT, (attributes.getSubject() != null) ? attributes.getSubject().get() : "");

        return header.retrieve();
    }





    //@TODO: create a print CLOUD EVENT helper

}

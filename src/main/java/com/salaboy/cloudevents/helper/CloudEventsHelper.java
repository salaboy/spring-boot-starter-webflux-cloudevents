package com.salaboy.cloudevents.helper;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.net.URI;
import java.util.Map;

public class CloudEventsHelper {

    public static CloudEvent<AttributesImpl, String> parseFromRequest(Map<String, String> headers, Object body) {
        return CloudEventBuilder.<String>builder()
                .withId(headers.get("Ce-Id"))
                .withType(headers.get("Ce-Type"))
                .withSource((headers.get("Ce-Source") != null) ? URI.create(headers.get("Ce-Source")) : null)
                .withData((body != null) ? body.toString() : "")
                .withDatacontenttype((headers.get("Content-Type") != null) ? headers.get("Content-Type") : "application/json")
                .build();
    }

    public static WebClient.ResponseSpec createPostCloudEvent(WebClient webClient, CloudEvent<AttributesImpl, String> cloudEvent) {
        WebClient.RequestBodySpec uri = webClient.post().uri("");
        WebClient.RequestHeadersSpec<?> headersSpec = uri.body(BodyInserters.fromValue(cloudEvent.getData()));
        AttributesImpl attributes = cloudEvent.getAttributes();
        WebClient.RequestHeadersSpec<?> header = headersSpec
                .header("Ce-Id", attributes.getId())
                .header("Ce-Specversion", attributes.getSpecversion())
                .header("Content-Type", "application/json")
                .header("Ce-Type", attributes.getType())
                .header("Ce-Source", (attributes.getSource() != null) ? attributes.getSource().toString() : "");

        return header.retrieve();
    }

}

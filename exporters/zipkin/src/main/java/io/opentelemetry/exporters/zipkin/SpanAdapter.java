package io.opentelemetry.exporters.zipkin;

import io.opentelemetry.common.Timestamp;
import io.opentelemetry.sdk.trace.SpanData;
import io.opentelemetry.trace.AttributeValue;
import zipkin2.Endpoint;
import zipkin2.Span;

import javax.annotation.concurrent.ThreadSafe;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@ThreadSafe
final class SpanAdapter {
    private static final long MICRO_SECONDS_PER_SECOND = 1_000_000L;
    private static final long NANO_SECONDS_PER_MICRO_SECOND = 1_000L;

    private SpanAdapter() {}

    static List<Span> toZipkin(Endpoint localEndpoint, List<SpanData> spans) {
        List<Span> zipkinSpans = new ArrayList<>(spans.size());
        for (SpanData span : spans) {
            zipkinSpans.add(toZipkin(localEndpoint, span));
        }
        return zipkinSpans;
    }

    private static Span toZipkin(Endpoint localEndpoint, SpanData spanData) {
        Span.Builder builder = Span.newBuilder();

        builder.traceId(spanData.getTraceId().toLowerBase16());
        if (spanData.getParentSpanId().isValid()) {
            builder.parentId(spanData.getParentSpanId().toLowerBase16());
        }
        builder.id(spanData.getSpanId().toLowerBase16());

        switch (spanData.getKind()) {
            case CLIENT:
                builder.kind(Span.Kind.CLIENT);
                break;
            case SERVER:
                builder.kind(Span.Kind.SERVER);
                break;
            case CONSUMER:
                builder.kind(Span.Kind.CONSUMER);
                break;
            case PRODUCER:
                builder.kind(Span.Kind.PRODUCER);
                break;
            default:
                // No equivalent zipkin kind, should be null
                break;
        }

        builder.name(spanData.getName());

        long startMicros = toMicroSeconds(spanData.getStartTimestamp());
        builder.timestamp(startMicros);
        long endMicros = toMicroSeconds(spanData.getEndTimestamp());
        builder.duration(endMicros - startMicros);

        builder.localEndpoint(localEndpoint);

        for (SpanData.TimedEvent timedEvent : spanData.getTimedEvents()) {
            builder.addAnnotation(toMicroSeconds(timedEvent.getTimestamp()), toAnnotation(timedEvent.getName(), timedEvent.getAttributes()));
        }

        for (Map.Entry<String, AttributeValue> attribute : spanData.getAttributes().entrySet()) {
            builder.putTag(attribute.getKey(), toTag(attribute.getValue()));
        }
        builder.putTag("otel.span_status", spanData.getStatus().getCanonicalCode().name());
        if (spanData.getStatus().getDescription() != null) {
            builder.putTag("otel.status_description", spanData.getStatus().getDescription());
        }

        return builder.build();
    }

    private static long toMicroSeconds(Timestamp timestamp) {
        long microSeconds = timestamp.getSeconds() * MICRO_SECONDS_PER_SECOND;
        microSeconds += timestamp.getNanos() / NANO_SECONDS_PER_MICRO_SECOND;

        return microSeconds;
    }

    private static String toAnnotation(String name, Map<String, AttributeValue> attributes) {
        StringBuilder annotation = new StringBuilder(name);
        if (!attributes.isEmpty()) {
            annotation.append(": ");
        }

        Iterator<Map.Entry<String, AttributeValue>> iterator = attributes.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, AttributeValue> attribute = iterator.next();
            annotation.append(attribute.getValue()).append('=').append(attribute.getValue().toString());
            if (iterator.hasNext()) {
                annotation.append(", ");
            }
        }

        return annotation.toString();
    }

    private static String toTag(AttributeValue attributeValue) {
        switch (attributeValue.getType()) {
            case BOOLEAN:
                return String.valueOf(attributeValue.getBooleanValue());
            case DOUBLE:
                return String.valueOf(attributeValue.getDoubleValue());
            case LONG:
                return String.valueOf(attributeValue.getLongValue());
            case STRING:
                return String.valueOf(attributeValue.getStringValue());
            default:
                return "";
        }
    }
}

package io.opentelemetry.exporters.zipkin;

import io.opentelemetry.sdk.trace.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import zipkin2.Endpoint;
import zipkin2.Span;

import javax.annotation.concurrent.ThreadSafe;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

@ThreadSafe
public final class ZipkinSpanExporter implements SpanExporter {
  private final Endpoint localEndpoint;

  private ZipkinSpanExporter() {
    Endpoint.Builder endpointBuilder = Endpoint.newBuilder();
    endpointBuilder.serviceName("unknown");
    endpointBuilder.port(0);

    try {
      String ip = InetAddress.getLocalHost().getHostAddress();
      endpointBuilder.ip(ip);
    } catch (UnknownHostException ignored) {
    }

    localEndpoint = endpointBuilder.build();
  }

  @Override
  public ResultCode export(List<SpanData> spanDataList) {
    List<Span> spans = SpanAdapter.toZipkin(localEndpoint, spanDataList);
    return null;
  }

  @Override
  public void shutdown() {

  }
}

package io.opentelemetry.exporters.zipkin;

import io.opentelemetry.sdk.trace.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.codec.SpanBytesEncoder;
import zipkin2.reporter.Sender;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

@ThreadSafe
public final class ZipkinSpanExporter implements SpanExporter {
  private final Sender sender;
  private final SpanBytesEncoder encoder;

  private final Endpoint localEndpoint;

  private ZipkinSpanExporter(String serviceName, Integer servicePort, Sender sender, SpanBytesEncoder encoder) {
    this.sender = sender;
    this.encoder = encoder;

    Endpoint.Builder endpointBuilder = Endpoint.newBuilder()
            .serviceName(serviceName);
    if (servicePort != null) {
      endpointBuilder.port(servicePort);
    }

    try {
      endpointBuilder.ip(InetAddress.getLocalHost().getHostAddress());
    } catch (UnknownHostException ignored) {
    }

    localEndpoint = endpointBuilder.build();
  }

  @Override
  public ResultCode export(List<SpanData> spanDataList) {
    List<Span> spans = SpanAdapter.toZipkin(localEndpoint, spanDataList);

    try {
      List<byte[]> batch = new ArrayList<>();
      int currentSize = 0;
      for (Span span : spans) {
        byte[] single = encoder.encode(span);
        int singleSize = sender.messageSizeInBytes(single.length);
        if (currentSize + singleSize < sender.messageMaxBytes()) {
          batch.add(single);
          currentSize += singleSize;
        } else {
          sender.sendSpans(batch).execute();
          batch.clear();
          batch.add(single);
          currentSize = singleSize;
        }
      }
      if (!batch.isEmpty()) {
        sender.sendSpans(batch).execute();
      }
    } catch (IOException e) {
      return ResultCode.FAILED_RETRYABLE;
    }

    return ResultCode.SUCCESS;
  }

  @Override
  public void shutdown() {
    // Nothing to shutdown
  }
}

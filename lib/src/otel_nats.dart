// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_nats_suppression.dart';

const _tracerName = 'dartastic_otel_libs_nats';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Wraps a NATS publish in a PRODUCER span.
Future<T> tracedNatsPublish<T>({
  required String subject,
  required Future<T> Function() invoke,
  String? replyTo,
  int? payloadSize,
  String? streamName,
}) async {
  if (natsInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': 'nats',
    'messaging.operation': 'publish',
    'messaging.destination.name': subject,
  };
  if (replyTo != null) m['messaging.nats.reply_to'] = replyTo;
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (streamName != null) m['messaging.nats.stream'] = streamName;

  final span = _tracer().startSpan(
    'publish $subject',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps a NATS subscribe-callback message handler in a CONSUMER span.
Future<T> tracedNatsConsume<T>({
  required String subject,
  required Future<T> Function() invoke,
  String? queueGroup,
  String? streamName,
  String? consumerName,
  int? sequence,
}) async {
  if (natsInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': 'nats',
    'messaging.operation': 'receive',
    'messaging.destination.name': subject,
  };
  if (queueGroup != null) m['messaging.nats.queue_group'] = queueGroup;
  if (streamName != null) m['messaging.nats.stream'] = streamName;
  if (consumerName != null) m['messaging.nats.consumer'] = consumerName;
  if (sequence != null) m['messaging.nats.sequence'] = sequence;

  final span = _tracer().startSpan(
    'receive $subject',
    kind: SpanKind.consumer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps a NATS request/reply call in a CLIENT span.
Future<T> tracedNatsRequest<T>({
  required String subject,
  required Future<T> Function() invoke,
  int? timeoutMs,
}) async {
  if (natsInstrumentationSuppressed()) {
    return invoke();
  }
  final m = <String, Object>{
    'messaging.system': 'nats',
    'messaging.operation': 'request',
    'messaging.destination.name': subject,
  };
  if (timeoutMs != null) m['messaging.nats.timeout_ms'] = timeoutMs;

  final span = _tracer().startSpan(
    'request $subject',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(OTel.attributes([
      OTel.attributeString(
        ErrorResource.errorType.key,
        e.runtimeType.toString(),
      ),
    ]));
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_nats/otel_nats.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_nats-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('publish records PRODUCER span', () async {
    await tracedNatsPublish<String>(
      subject: 'orders.new',
      replyTo: '_INBOX.x',
      payloadSize: 64,
      streamName: 'ORDERS',
      invoke: () async => 'ok',
    );
    final a = attrsOf(spans.findSpanByName('publish orders.new')!);
    expect(a['messaging.system'], 'nats');
    expect(a['messaging.destination.name'], 'orders.new');
    expect(a['messaging.nats.reply_to'], '_INBOX.x');
    expect(a['messaging.nats.stream'], 'ORDERS');
  });

  test('JetStream consume records CONSUMER span', () async {
    await tracedNatsConsume<String>(
      subject: 'orders.new',
      streamName: 'ORDERS',
      consumerName: 'worker-1',
      sequence: 42,
      invoke: () async => 'ok',
    );
    final a = attrsOf(spans.findSpanByName('receive orders.new')!);
    expect(a['messaging.nats.stream'], 'ORDERS');
    expect(a['messaging.nats.consumer'], 'worker-1');
    expect(a['messaging.nats.sequence'], 42);
  });

  test('request records CLIENT span', () async {
    await tracedNatsRequest<String>(
      subject: 'rpc.echo',
      timeoutMs: 5000,
      invoke: () async => 'pong',
    );
    final a = attrsOf(spans.findSpanByName('request rpc.echo')!);
    expect(a['messaging.operation'], 'request');
    expect(a['messaging.nats.timeout_ms'], 5000);
  });

  test('suppression skips spans', () async {
    await runWithoutNatsInstrumentationAsync(() async {
      await tracedNatsPublish<String>(
        subject: 's',
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('publish '), isEmpty);
  });
}

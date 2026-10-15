# Qdrant.zig

Cliente **gRPC** do Qdrant para **Zig 0.16.0**. O transporte usa gRPC Core e
troca mensagens protobuf binárias diretamente com a porta `6334`; a API REST
não é utilizada.

## Requisitos

- Zig 0.16.0;
- biblioteca de desenvolvimento gRPC Core (`libgrpc-dev` em Ubuntu/Debian);
- Qdrant com a porta gRPC 6334 acessível.

```sh
sudo apt-get install libgrpc-dev
zig fetch --save git+https://github.com/SEU_USUARIO/Qdrant.zig
```

## Uso

```zig
const std = @import("std");
const qdrant = @import("qdrant");

test "Qdrant via gRPC" {
    const allocator = std.testing.allocator;
    var client = try qdrant.Client.init(allocator, .{
        .target = "localhost:6334",
        // .api_key = "secret",
        // .tls = true,
    });
    defer client.deinit();

    var created = try client.createCollection(.{
        .name = "documents",
        .vector_size = 384,
        .distance = .cosine,
    });
    defer created.deinit(allocator);
    try std.testing.expect(created.isSuccess());

    const points = [_]qdrant.Point{
        .{ .id = 1, .vector = &.{ 0.1, 0.2, 0.3, 0.4 } },
    };
    var upserted = try client.upsert("documents", &points, true);
    defer upserted.deinit(allocator);
}
```

## API gRPC completa

`call` recebe o nome canônico do método e a mensagem protobuf serializada. Isso
permite chamar qualquer RPC unary atual ou futuro sem esperar uma nova versão
da biblioteca:

```zig
var response = try client.call("/qdrant.Points/Query", protobuf_request);
defer response.deinit(allocator);
```

Também existem `collectionsRaw`, `pointsRaw` e `snapshotsRaw`. `ProtoWriter`
oferece primitivas para varints e campos length-delimited; ele pode ser usado
com os schemas protobuf oficiais do Qdrant. A resposta contém `status`, a
mensagem de status e os bytes protobuf retornados.

## Eventos UbiQ: Memory, NATS e QUIC

O módulo `qdrant.events` integra o cliente com uma versão fixada da biblioteca
[UbiQUIC eXtreme](https://github.com/suissa/UbiQUIC-eXtreme-zero-trust-message-queue).
As operações seguem o prefixo `<action>.<entity/property>` solicitado, como
`create.collection`, `upsert.collection`, `query.point`, `set.payload` e
`create.snapshot`.

Como o envelope canônico UbiQ exige um estado de ciclo de vida, os assuntos no
wire acrescentam `.request`, `.ok` ou `.error`. Assim, o consumidor ouve
`create.collection.request` e publica o mesmo frame de resultado em Memory,
NATS e QUIC sob `create.collection.ok` ou `create.collection.error`.

```zig
const events = qdrant.events;

var hub = events.Hub(256, 1024 * 1024){
    .nats = nats_sink.sink(),
    .quic = my_quic_sink, // envia o frame UbiQ em um stream QUIC
};

const incoming: events.event.Envelope = .{
    .id = "evt-1",
    .event = try events.event.CanonicalEvent.parse("create.collection.request"),
    .correlation_id = "corr-1",
    .causation_id = "",
    .idempotency_key = "create-documents-1",
    .schema_id = "create.collection.request@1",
    .payload = protobuf_request,
    .guarantee = .processed,
    .created_at_ms = 0,
};

var result = try events.handle(allocator, &client, incoming);
defer result.deinit(); // após os consumidores de memória terminarem
_ = try hub.emit(result.envelope);
```

`NatsListener` assina `*.*.request`, valida o envelope e executa a chamada
gRPC. `handleQuicFrame` faz a mesma operação para frames recebidos por QUIC.
`Hub.nextMemory` e `Hub.settleMemory` implementam consumo, lease e settlement
da fila em memória. O projeto UbiQ ainda não implementa sockets QUIC; por isso,
o adapter recebe um `Sink` de um provider QUIC real e preserva integralmente o
formato binário, a identidade e o payload do envelope UbiQ.

## Testes

```sh
zig fmt --check build.zig src tests
zig build test

docker run --rm -p 6333:6333 -p 6334:6334 qdrant/qdrant:v1.15.4
zig build integration-test
```

O GitHub Actions executa os testes com a imagem oficial do Qdrant, publica as
portas REST (health check) e gRPC (cliente), e valida um ciclo real de coleção,
vetores, listagem, consulta de informações e contagem.

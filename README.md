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

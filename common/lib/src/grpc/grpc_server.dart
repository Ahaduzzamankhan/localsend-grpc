import 'dart:async';
import 'dart:io';
import 'package:grpc/grpc.dart';
import 'package:localsend_grpc/localsend.pb.dart';
import 'package:localsend_grpc/localsend.pbgrpc.dart';
import 'package:logging/logging.dart';
import 'package:uuid/uuid.dart';

final _logger = Logger('GrpcServer');
final _uuid = Uuid();

class LocalSendGrpcService extends LocalSendServiceBase {
  final DeviceInfo localInfo;
  final Future<bool> Function(PrepareUploadRequest) onPrepareUpload;
  final Future<void> Function(String fileId, List<int> data) onFileReceived;
  final Future<PrepareDownloadResponse> Function() onPrepareDownload;
  final Future<Stream<FileChunk>> Function(DownloadRequest) onDownload;

  final Map<String, Map<String, String>> _sessions = {};

  LocalSendGrpcService({
    required this.localInfo,
    required this.onPrepareUpload,
    required this.onFileReceived,
    required this.onPrepareDownload,
    required this.onDownload,
  });

  @override
  Future<DeviceInfo> getInfo(ServiceCall call, Empty request) async {
    return localInfo;
  }

  @override
  Future<DeviceInfo> register(ServiceCall call, DeviceInfo request) async {
    _logger.info('[REGISTER] ${request.alias}');
    return localInfo;
  }

  @override
  Future<PrepareUploadResponse> prepareUpload(
    ServiceCall call,
    PrepareUploadRequest request,
  ) async {
    final accepted = await onPrepareUpload(request);
    if (!accepted) {
      throw GrpcError.permissionDenied('Upload rejected');
    }

    final sessionId = _uuid.v4();
    final tokens = <String, String>{
      for (final fileId in request.files.keys) fileId: _uuid.v4(),
    };
    _sessions[sessionId] = tokens;

    return PrepareUploadResponse(sessionId: sessionId, fileTokens: tokens);
  }

  @override
  Future<UploadResponse> upload(
    ServiceCall call,
    Stream<FileChunk> request,
  ) async {
    final buffer = <String, List<int>>{};
    String? currentFileId;

    await for (final chunk in request) {
      final tokens = _sessions[chunk.sessionId];
      if (tokens == null || tokens[chunk.fileId] != chunk.token) {
        throw GrpcError.unauthenticated('Invalid session or token');
      }

      currentFileId = chunk.fileId;
      buffer[chunk.fileId] ??= [];
      buffer[chunk.fileId]!.addAll(chunk.data);

      if (chunk.isLast && currentFileId != null) {
        await onFileReceived(currentFileId, buffer[currentFileId]!);
        buffer.remove(currentFileId);
      }
    }

    return UploadResponse(success: true);
  }

  @override
  Future<PrepareDownloadResponse> prepareDownload(
    ServiceCall call,
    Empty request,
  ) async {
    return onPrepareDownload();
  }

  @override
  Stream<FileChunk> download(
    ServiceCall call,
    DownloadRequest request,
  ) async* {
    final stream = await onDownload(request);
    yield* stream;
  }

  @override
  Future<Empty> cancel(ServiceCall call, CancelRequest request) async {
    _sessions.remove(request.sessionId);
    _logger.info('[CANCEL] session ${request.sessionId}');
    return Empty();
  }
}

Future<Server> startGrpcServer({
  required LocalSendGrpcService service,
  required int port,
  SecurityContext? securityContext,
}) async {
  final server = Server.create(
    services: [service],
    codecRegistry: CodecRegistry(codecs: const [GzipCodec(), IdentityCodec()]),
  );

  await server.serve(
    port: port,
    security: securityContext != null
        ? ServerTlsCredentials(certificate: securityContext)
        : null,
  );

  _logger.info('gRPC server listening on port $port');
  return server;
}

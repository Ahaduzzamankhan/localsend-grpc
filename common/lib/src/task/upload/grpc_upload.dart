import 'package:common/model/device.dart';
import 'package:common/src/grpc/grpc_client.dart';
import 'package:localsend_grpc/localsend.pb.dart';

class GrpcUploadService {
  Future<void> upload({
    required Stream<List<int>> stream,
    required String contentType,
    required Device target,
    required String? sessionId,
    required String fileId,
    required String token,
    required int contentLength,
    required void Function(double) onSendProgress,
    required GrpcCancelToken cancelToken,
  }) async {
    final client = makeClient(target);
    int sent = 0;

    final chunkStream = () async* {
      await for (final bytes in stream) {
        if (cancelToken.cancelled) return;
        sent += bytes.length;
        onSendProgress(contentLength > 0 ? sent / contentLength : 0);
        yield FileChunk(
          sessionId: sessionId ?? '',
          fileId: fileId,
          token: token,
          data: bytes,
          isLast: false,
        );
      }
      yield FileChunk(
        sessionId: sessionId ?? '',
        fileId: fileId,
        token: token,
        data: [],
        isLast: true,
      );
    }();

    final call = client.upload(chunkStream);
    cancelToken.setCancel(() => call.cancel());
    await call;
  }
}

class GrpcCancelToken {
  bool cancelled = false;
  void Function()? _cancel;

  void cancel() {
    cancelled = true;
    _cancel?.call();
  }

  void setCancel(void Function() cancel) {
    _cancel = cancel;
  }
}

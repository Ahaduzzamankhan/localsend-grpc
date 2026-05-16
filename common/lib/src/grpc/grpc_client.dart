import 'package:grpc/grpc.dart';
import 'package:common/model/device.dart';
import 'package:localsend_grpc/localsend.pbgrpc.dart';

class GrpcClientCollection {
  final LocalSendClient discovery;
  final LocalSendClient longLiving;

  GrpcClientCollection({required this.discovery, required this.longLiving});
}

LocalSendClient makeClient(Device target, {Duration? timeout}) {
  final channel = ClientChannel(
    target.ip,
    port: target.port,
    options: ChannelOptions(
      credentials: target.https
          ? const ChannelCredentials.secure()
          : const ChannelCredentials.insecure(),
      connectTimeout: timeout ?? const Duration(seconds: 30),
    ),
  );
  return LocalSendClient(channel);
}

ClientChannel makeChannel(String ip, int port, bool https, {Duration? timeout}) {
  return ClientChannel(
    ip,
    port: port,
    options: ChannelOptions(
      credentials: https
          ? const ChannelCredentials.secure()
          : const ChannelCredentials.insecure(),
      connectTimeout: timeout ?? const Duration(seconds: 30),
    ),
  );
}

import 'package:common/model/device.dart';
import 'package:common/src/grpc/grpc_client.dart';
import 'package:common/util/task_runner.dart';
import 'package:localsend_grpc/localsend.pb.dart';
import 'package:localsend_grpc/localsend.pbgrpc.dart';
import 'package:logging/logging.dart';

final _logger = Logger('GrpcDiscovery');

class GrpcDiscoveryService {
  Stream<Device> scanNetwork({
    required String networkInterface,
    required int port,
    required bool https,
  }) {
    final prefix = networkInterface.split('.').take(3).join('.');
    final ips = List.generate(256, (i) => '$prefix.$i')
        .where((ip) => ip != networkInterface)
        .toList();

    final runner = TaskRunner<Device?>(
      initialTasks: ips.map((ip) => () => _probe(ip, port, https)).toList(),
      concurrency: 50,
    );

    return runner.stream.where((d) => d != null).cast<Device>();
  }

  Stream<Device> scanFavorites({
    required List<(String, int)> devices,
    required bool https,
  }) {
    final runner = TaskRunner<Device?>(
      initialTasks: devices
          .map((d) => () => _probe(d.$1, d.$2, https))
          .toList(),
      concurrency: 50,
    );

    return runner.stream.where((d) => d != null).cast<Device>();
  }

  Future<Device?> _probe(String ip, int port, bool https) async {
    final channel = makeChannel(ip, port, https, timeout: const Duration(seconds: 2));
    try {
      final client = LocalSendClient(channel);
      final info = await client.getInfo(Empty());
      _logger.info('[DISCOVER/gRPC] ${info.alias} ($ip)');
      return Device(
        ip: ip,
        port: port,
        https: https,
        alias: info.alias,
        version: info.version,
        deviceModel: info.deviceModel,
        deviceType: DeviceType.values.byName(info.deviceType),
        fingerprint: info.fingerprint,
        download: info.download,
      );
    } catch (_) {
      return null;
    } finally {
      await channel.shutdown();
    }
  }
}

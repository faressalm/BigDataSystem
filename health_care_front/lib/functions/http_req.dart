import 'dart:convert';

import 'package:health_care_front/models/heallth_service.dart';
import 'package:http/http.dart' as http;

Future<List<HealthService>> getheallthServicesList(
    DateTime start, DateTime end) async {
  var endpointUrl =
      'http://localhost:8080/getAllServices?start=${start.microsecondsSinceEpoch / 1000}&end=${end.microsecondsSinceEpoch / 1000}';
  var data = await http.get(Uri.parse(endpointUrl));
  var jsonData = json.decode(data.body);
  List<HealthService> heallthServicesList = jsonData
      .map<HealthService>((data) => HealthService.fromJson(data))
      .toList();

  return heallthServicesList;
}

import 'package:flutter/material.dart';
import 'package:health_care_front/Components/health_service_row.dart';
import 'package:health_care_front/models/heallth_service.dart';

import '../../../constant.dart';

class Body extends StatelessWidget {
  List<HealthService> healthServiceList = [
  ];

  Body({Key? key,required this.healthServiceList}) : super(key: key);
  @override
  Widget build(BuildContext context) {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 20),
      child: SizedBox(
        height: MediaQuery.of(context).size.height*0.7,
        child: Container(
          margin: const EdgeInsets.symmetric(vertical: 20),
          padding: const EdgeInsets.all(15),
          decoration: BoxDecoration(
            color: Color.fromARGB(255, 255, 253, 253),
            borderRadius: BorderRadius.circular(34),
          ),
          child:  SingleChildScrollView(
            child: ListView.builder(
              physics: const NeverScrollableScrollPhysics(),
              shrinkWrap: true,
              scrollDirection: Axis.vertical,
              itemCount: healthServiceList.length,
              itemBuilder: (BuildContext context, int index) {
                return HealthServiceRow(
                  healthService: healthServiceList[index],
                );
              },
            ),
          ),
        ),
      ),
    );
  }
}

import 'package:date_time_picker/date_time_picker.dart';
import 'package:flutter/material.dart';
import 'package:health_care_front/constant.dart';
import '../../Components/app_bar.dart';
import '../../Components/body.dart';
import '../../Components/loading_dialog.dart';
import '../../functions/http_req.dart';
import '../../models/heallth_service.dart';

class HomeScreen extends StatefulWidget {
  HomeScreen({Key? key}) : super(key: key);
  List<HealthService> healthServiceList = [];
  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  @override
  Widget build(BuildContext context) {
    Size size = MediaQuery.of(context).size;

    getAnalysis(DateTime start,DateTime end) async {
      loadingDialog(context);
      var newList =
          await getheallthServicesList(start, end);
      Navigator.pop(context);
      setState(() {
        widget.healthServiceList = newList;
      });
    }

    return Scaffold(
      body: LayoutBuilder(builder: (context, c) {
        return Center(
          child: Container(
            height: size.height,
            width: size.width,
            decoration: const BoxDecoration(
              color: kPrimaryColor,
            ),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: <Widget>[
                CustomAppBar(
                  getAnalysis: getAnalysis,
                ),
                Body(healthServiceList: widget.healthServiceList),
               ],
            ),
          ),
        );
      }),
    );
  }
}

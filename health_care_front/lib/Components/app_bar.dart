import 'package:date_time_picker/date_time_picker.dart';
import 'package:flutter/material.dart';

import '../../../constant.dart';
import 'date_button.dart';

class CustomAppBar extends StatefulWidget {
 final Function getAnalysis;

  CustomAppBar({Key? key,required this.getAnalysis}) : super(key: key);
  @override
  State<CustomAppBar> createState() => _CustomAppBarState();
}

class _CustomAppBarState extends State<CustomAppBar> {
  DateTimeRange dateRange =  DateTimeRange(start:DateTime.now(), end:DateTime.now(),);
  late DateTime start = DateTime.now();
  late DateTime end = DateTime.now();
  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.all(20),
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: Colors.white,
        borderRadius: BorderRadius.circular(46),
        boxShadow: [
          BoxShadow(
            offset: const Offset(0, -2),
            blurRadius: 30,
            color: Colors.black.withOpacity(0.16),
          ),
        ],
      ),
      child: Row(
        children: <Widget>[
         const Icon(Icons.health_and_safety_sharp,color: fPrimaryColor,),
        const  SizedBox(width: 5),
          Text(
            "Health Care System".toUpperCase(),
            style:const TextStyle(fontSize: 22, fontWeight: FontWeight.bold),
          ),
             const Spacer(),
             const   Text("From: "),
               ConstrainedBox(
                 constraints: const BoxConstraints(maxWidth: 200),
                 child: DateTimePicker(
                    type: DateTimePickerType.dateTimeSeparate,
                    dateMask: 'd MMM, yyyy',
                    initialValue: DateTime.now().toString(),
                    firstDate: DateTime(2000),
                    lastDate: DateTime(2100),
                    icon: Icon(Icons.event),
                    dateLabelText: 'Date',
                    timeLabelText: "Hour",
                    selectableDayPredicate: (date) {
                      return true;
                    },
                    onChanged: (val) {
                      start = DateTime.parse(val);
                    },
                    validator: (val) {
                      print(val);
                      return null;
                    },
                    onSaved: (val) => print(val),
                  ),
               )
             ,
             const   Text("To: "),
               ConstrainedBox(
                 constraints: const BoxConstraints(maxWidth: 200),
                 child: DateTimePicker(
                    type: DateTimePickerType.dateTimeSeparate,
                    dateMask: 'd MMM, yyyy',
                    initialValue: DateTime.now().toString(),
                    firstDate: DateTime(2000),
                    lastDate: DateTime(2100),
                    icon: Icon(Icons.event),
                    dateLabelText: 'Date',
                    timeLabelText: "Hour",
                    selectableDayPredicate: (date) {
                      return true;
                    },
                    onChanged: (val) {
                      end = DateTime.parse(val);
                    },
                    validator: (val) {
                      print(val);
                      return null;
                    },
                    onSaved: (val) => print(val),
                  ),
               )
             ,
          ClipRRect(
      borderRadius: BorderRadius.circular(25),
      child: FlatButton(
        padding: EdgeInsets.symmetric(horizontal: 25, vertical: 15),
        color: kPrimaryColor,
        onPressed: (){widget.getAnalysis(start,end);},
        child: Text(
          "Get Analysis".toUpperCase(),
                    style: TextStyle(color: Colors.white),
        ),
      ),
    )
        ],
      ),
    );
  }
}


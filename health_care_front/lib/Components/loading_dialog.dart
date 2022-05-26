import 'package:flutter/material.dart';
import 'package:health_care_front/constant.dart';

loadingDialog(BuildContext context) {
  return showDialog(
      context: context,
      barrierDismissible: false,
      builder: (BuildContext context) {
        return const WillPopScope(
          onWillPop: null,
          child:  Center(
            child: CircularProgressIndicator(
              color: fPrimaryColor,
            ),
          ),
        );
      });
}
import 'package:flutter/material.dart';

import '../../../constant.dart';

class DateButton extends StatelessWidget {
  final String title;
  final String text;
  final Function() press;
  const DateButton({
    Key? key,
    required this.title,
    required this.text,
    required this.press,
  }) : super(key: key);

  @override
  Widget build(BuildContext context) {
    return InkWell(
      onTap: press,
      child: Padding(
        padding: const EdgeInsets.symmetric(horizontal: 15),
        child: Row(
          children: [
            Text(
              title.toUpperCase(),
              style: TextStyle(
                fontSize: 20,
                color: kTextcolor.withOpacity(0.5),
              ),
            ),
         const   SizedBox(
              width:10
            ),
               Text(
              text,
              style: const TextStyle(
                fontSize: 20,
                color: fPrimaryColor,
                fontWeight: FontWeight.bold,
              ),
            ),
          ],
        ),
      ),
    );
  }
}

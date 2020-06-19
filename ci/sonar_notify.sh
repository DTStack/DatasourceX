#!/bin/bash
#参考钉钉文档 https://open-doc.dingtalk.com/microapp/serverapi2/qf2nxq
 sonarreport=$(curl -s http://172.16.100.198:8082/?projectname=dt-insight-web/dt-center-common-loader)
 curl -s "https://oapi.dingtalk.com/robot/send?access_token=24bce1c7512ad0e10b742fe3de1416082dca4d89dd48c99613e450f65f49b9a4" \
   -H "Content-Type: application/json" \
   -d "{
     \"msgtype\": \"markdown\",
     \"markdown\": {
         \"title\":\"sonar代码质量\",
         \"text\": \"## sonar代码质量报告: \n
> [sonar地址](http://172.16.100.198:9000/dashboard?id=dt-insight-web/dt-center-common-loader) \n
> ${sonarreport} \n\"
     }
 }"
#!/bin/bash
 sonarReport=$(curl -u 8104b6560961915a8cfec15a73044bef1909b51f: "http://172.16.100.158:9001/api/measures/search?projectKeys=dt_center_common_loader&metricKeys=alert_status,bugs,vulnerabilities,code_smells,coverage,duplicated_lines_density")
 # 六个参数依此遍历
 # 需要提前安装 jq 用于解析 json
 for i in {0..5} ; do
     metric=$(echo "${sonarReport}" | jq -r '.measures['$i'].metric')
     value=$(echo "${sonarReport}" | jq -r '.measures['$i'].value')
     if [ "${metric}" == 'alert_status' ]; then
       alert_status=${value}
     elif [ "${metric}" == 'bugs' ]; then
       bugs=${value}
     elif [ "${metric}" == 'vulnerabilities' ]; then
       vulnerabilities=${value}
     elif [ "${metric}" == 'code_smells' ]; then
       code_smells=${value}
     elif [ "${metric}" == 'coverage' ]; then
       coverage=${value}
     elif [ "${metric}" == 'duplicated_lines_density' ]; then
       duplicated_lines_density=${value}
     fi
 done

 curl -s "https://oapi.dingtalk.com/robot/send?access_token=1e97d68233a988399c412fa861fb93155b944d7c84d4ffd982685a6b4d034914" \
   -H "Content-Type: application/json" \
   -d "{
     \"msgtype\": \"markdown\",
     \"markdown\": {
         \"title\":\"sonar代码质量\",
         \"text\": \"## sonar代码质量报告: [sonar地址](http://172.16.100.158:9001/dashboard?id=dt_center_common_loader) \n
> 状态: ${alert_status} \n
> bug: ${bugs} \n
> 漏洞: ${vulnerabilities} \n
> 异味: ${code_smells} \n
> 覆盖率: ${coverage} \n
> 重复率: ${duplicated_lines_density} \n
   \"}
 }"
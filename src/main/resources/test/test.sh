#!/bin/bash
echo "hello 中国"

#PS3菜单提示需要输入的信息，配合select语句使用
PS3="Please select your menu:"
#select选择语句
select i in "Apache" "Mysql" "Spark"
do
        case $i in
                Apache )
                echo -e "\033[32mWait install Apache server...\033[0m"
                ;;
                Mysql )
                echo -e "\033[32mWait install Mysql server...\033[0m"
                ;;
                Spark )
                echo -e "\033[32mWait install Spark server...\033[0m"
                ;;
                * )
                echo -e "\033[32mUsage: {$0 Apache|Mysql|Spark|Help}\033[1m"
                ;;
        esac
done









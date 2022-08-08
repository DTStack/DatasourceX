# 找不到相关驱动包解决方法

在当前目录下执行如下命令进行 install
````
mvn install:install-file -DgroupId=com.ibm.db2 -DartifactId=jcc -Dversion=11.5.0.0 -Dpackaging=jar -Dfile=jcc-11.5.0.0.jar
````
 # 快速编译
mvn clean install -DskipTests -Dfast -Pskip-webui-build -T 1C

#仓库地址，使用aliyun maven仓库
    <mirror>
      <id>alimaven</id>
      <name>aliyun maven</name>
      <url>http://maven.aliyun.com/nexus/content/groups/public/</url>
      <mirrorOf>central</mirrorOf>
    </mirror>

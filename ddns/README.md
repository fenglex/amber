工具用于更新阿里云dns对应ip地址
--
工具用于定时更新当前主机的外网ip到阿里云dns记录中，默认为每分钟检测一次ip变化


docker-compose方式启动
```
version: '3.1'
services:
  ddns:
    image: fenglex/ddns
    restart: always
    container_name: ddns
    environment:
      ACCESS_KEY_ID: access
      ACCESS_KEY_SECRET: secret
      DOMAIN: example.com
      RR: value
``` 


使用方式：

        1、对于普通用户建议直接依赖打包好的新版订阅sdk使用

        在程序的pom.xml文件中假如如下的依赖即可使用：

        <dependency>
            <groupId>com.aliyun.dts</groupId>
            <artifactId>dts-new-subscribe-sdk</artifactId>
            <version>{dts_new_sdk_version}</version>
        </dependency>

        最新的版本请在 https://mvnrepository.com/artifact/com.aliyun.dts/dts-new-subscribe-sdk/ 查看

        2、对于Kafka很了解的用户或者有对sdk内部的逻辑有改造需求的用户可以对该源码进行修改

配置说明：

    使用的时候主要逻辑如下，在test目录下也有参考的demo:

        ConsumerContext consumerContext = new ConsumerContext(brokerUrl, topic, sid, userName, password, initCheckpoint, subscribeMode);
        
        //if this parameter is set, force to use the initCheckpoint to initial
        consumerContext.setForceUseCheckpoint(isForceUseInitCheckpoint);

        //add user store
        consumerContext.setUserRegisteredStore(new UserMetaStore());

        DTSConsumer dtsConsumer = new DefaultDTSConsumer(consumerContext);

        dtsConsumer.addRecordListeners(buildRecordListener());
        
        dtsConsumer.start();
        
    其中的brokerUrl, topic, sid, userName, password在使用的时候进行替换，对应的信息都是在DTS订阅控制台进行配置。
    initCheckpoint是第一次启动DTS新版订阅client使用的起始位点，该位点只对assign和第一次启动的subscribe模式的client有效。
    
    subscribeMode代表了DTS新版订阅client的两种使用方式：
        1、ConsumerContext.ConsumerSubscribeMode.ASSIGN：
        
            对应Kafka client的assign模式，因为DTS为了保证消息的全局有序，每个topic只有一个partition，这种模式直接分配固定的partition 0，建议只启动一个client。
        
        2、ConsumerContext.ConsumerSubscribeMode.SUBSCRIBE:
            对应Kafka client的subscribe模式，因为DTS为了保证消息的全局有序，每个topic只有一个partition，所以对于配置了同一个sid的不同SDK客户端只会有一个客户端能够分配到partition 0并         接收到消息。启动多个SDK client可以起到容灾的效果，也就是一个SDK client失败了，另外一个SDK client可以自动的分配到partition 0并继续消费。

消息位点的管理：
    
    SDK的client需要知道在第一次启动、内部重试、重启SDK client等不同的场景下如何管理消费位点来确保数据不丢失，并且尽量的不重复。
    
    a.位点的保存： 订阅SDK默认5s会保存一次位点到本地本地localCheckpointStore文件，并提交一次位点到DTS server端，如果用户在模式配置了 
    consumerContext.setUserRegisteredStore(new UserMetaStore())使用自己外部的持久化共享存储介质（比如数据库）来保存位点的话，也会5s保存一次该位点到用户配置的存储方式。
    
    b.位点的使用,找到即返回，具体分为如下场景讨论：
    
    1、第一次启动SDK client：
        这种场景无论ASSIGN还是SUBSCRIBE模式都会使用传入的checkpoint去DTS server端查找对应的位点，并开始消费
   
    2、SDK内部对于比如DTS server端的DStore发生容灾切换或者其他可重试的一些错误去重新连接DTS server端：
        1）ASSIGN：
            对于该模式，不管有没有配置consumerContext.setForceUseCheckpoint(true | false)
            client使用的位点查找顺序如下：本地localCheckpointStore文件--->使用传入的intinal timestamp--->用户配置的外部存储的位点
            
        2）SUBSCRIBE：
            对于该模式，client使用的位点查找顺序如下：
            DTS server(DStore)保存的位点--->用户配置的外部存储的位点--->使用传入的intinal timestamp--->使用DTS server(新建DStore)的起始位点
    
    3、SDK client进程重启：
        1）ASSIGN：
           如果配置了consumerContext.setForceUseCheckpoint(true)的话，每次重启都会强制使用传入的initCheckpoint作为位点
           
           如果consumerContext.setForceUseCheckpoint(false)或者没设置的话使用的位点查找顺序如下：
           本地localCheckpointStore文件--->DTS server(DStore)保存的位点--->用户配置的外部存储的位点
        2）SUBSCRIBE：
           该模式consumerContext.setForceUseCheckpoint无效，位点的查找顺序为：
           DTS server(DStore)保存的位点--->用户配置的外部存储的位点--->使用传入的intinal timestamp--->使用DTS server(新建DStore)的起始位点
        
统计信息：
     
    SDK内部对从 DTS server(DStore)接收到的数据总数、接收rps、用户消费的数据量、消费rps，SDK内部缓存的数据量都有一个统计，例如：
          {"outCounts":6073728.0,"outBytes":2473651076,"outRps":18125.37,"outBps":7379638.54,"count":11.0,"inBytes":2.4751382E+9,"DStoreRecordQueue":0.0,"inCounts":6082097.0,"inRps":18112.68,"inBps":7371325.86,"__dt":1611808055414,"DefaultUserRecordQueue":0.0}
    
    说明如下：
    DStoreRecordQueue：从 DTS server(DStore)接收到的数的缓存队列目前的大小
    DefaultUserRecordQueue：序列化之后的数据的缓存队列目前的大小
    inCounts：从 DTS server(DStore)接收到的数据总数
    inRps：从 DTS server(DStore)接收到的数据Rps      
    inBps：从 DTS server(DStore)接收到的数据Bps   
    outCounts：client消费的数据总数
    outRps：client消费的Rps
    outBps：client消费的Bps
    
问题排查：
       
     1、连接不上---DProxy地址填写错误
        比如brokerUrl填写错误，写成了"dts-cn-hangzhou.aliyuncs.com:18009"
        
        client报错信息如下:
        ERROR CheckResult{isOk=false, errMsg='telnet dts-cn-hangzhou.aliyuncs.com:18009 failed, please check the network and if the brokerUrl is correct'} (com.aliyun.dts.subscribe.clients.DefaultDTSConsumer)
        
     2、连接不上---和真实的broker地址不通
     
       client报错信息如下:
       telnet real node xxx failed, please check the network
 
     3、连接不上---用户名密码填写错误
        
        client报错信息如下:
        ERROR CheckResult{isOk=false, errMsg='build kafka consumer failed, error: org.apache.kafka.common.errors.TimeoutException: Timeout expired while fetching topic metadata, probably the user name or password is wrong'} (com.aliyun.dts.subscribe.clients.DefaultDTSConsumer)
        
      4、连接不上---用户的位点不在范围
        比如配置了强制使用传入的位点：
        consumerContext.setUseCheckpoint(true);
        并且：
        把checkpoint = "1609725891"(2020-12-01 10:04:51，比dstore范围早)
        或者比如把checkpoint = "1610249501"2021-01-10 11:31:41，比dstore范围晚)
        
        client报错信息如下:
        com.aliyun.dts.subscribe.clients.exception.TimestampSeekException: RecordGenerator:seek timestamp for topic [cn_hangzhou_rm_bp11tv2923n87081s_rdsdt_dtsacct-0] with timestamp [1610249501] failed
        
        
        5、判断是从DTS server(DStore)拉取数据慢了，还是client消费数据慢了：
        
        查看输出的统计信息，主要关注DStoreRecordQueue和DefaultUserRecordQueue队列的大小，
        如果队列的大小一直是0，那么说明DTS server(DStore)拉取数据慢了，如果队列的大小一直是默认大小512说明是消费慢了
        

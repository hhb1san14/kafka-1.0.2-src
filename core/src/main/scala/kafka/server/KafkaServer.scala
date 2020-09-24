/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.io.{File, IOException}
import java.net.SocketTimeoutException
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.yammer.metrics.core.Gauge
import kafka.api.KAFKA_0_9_0
import kafka.cluster.Broker
import kafka.common.{GenerateBrokerIdException, InconsistentBrokerIdException}
import kafka.controller.KafkaController
import kafka.coordinator.group.GroupCoordinator
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.{LogConfig, LogManager}
import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter}
import kafka.network.SocketServer
import kafka.security.CredentialProvider
import kafka.security.auth.Authorizer
import kafka.utils._
import org.apache.kafka.clients.{ApiVersions, ManualMetadataUpdater, NetworkClient, NetworkClientUtils}
import org.apache.kafka.common.internals.ClusterResourceListeners
import org.apache.kafka.common.metrics.{JmxReporter, Metrics, _}
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControlledShutdownRequest, ControlledShutdownResponse}
import org.apache.kafka.common.security.{JaasContext, JaasUtils}
import org.apache.kafka.common.utils.{AppInfoParser, LogContext, Time}
import org.apache.kafka.common.{ClusterResource, Node}

import scala.collection.JavaConverters._
import scala.collection.{Map, Seq, mutable}

object KafkaServer {
  // Copy the subset of properties that are relevant to Logs
  // I'm listing out individual properties here since the names are slightly different in each Config class...
  private[kafka] def copyKafkaConfigToLog(kafkaConfig: KafkaConfig): java.util.Map[String, Object] = {
    val logProps = new util.HashMap[String, Object]()
    logProps.put(LogConfig.SegmentBytesProp, kafkaConfig.logSegmentBytes)
    logProps.put(LogConfig.SegmentMsProp, kafkaConfig.logRollTimeMillis)
    logProps.put(LogConfig.SegmentJitterMsProp, kafkaConfig.logRollTimeJitterMillis)
    logProps.put(LogConfig.SegmentIndexBytesProp, kafkaConfig.logIndexSizeMaxBytes)
    logProps.put(LogConfig.FlushMessagesProp, kafkaConfig.logFlushIntervalMessages)
    logProps.put(LogConfig.FlushMsProp, kafkaConfig.logFlushIntervalMs)
    logProps.put(LogConfig.RetentionBytesProp, kafkaConfig.logRetentionBytes)
    logProps.put(LogConfig.RetentionMsProp, kafkaConfig.logRetentionTimeMillis: java.lang.Long)
    logProps.put(LogConfig.MaxMessageBytesProp, kafkaConfig.messageMaxBytes)
    logProps.put(LogConfig.IndexIntervalBytesProp, kafkaConfig.logIndexIntervalBytes)
    logProps.put(LogConfig.DeleteRetentionMsProp, kafkaConfig.logCleanerDeleteRetentionMs)
    logProps.put(LogConfig.MinCompactionLagMsProp, kafkaConfig.logCleanerMinCompactionLagMs)
    logProps.put(LogConfig.FileDeleteDelayMsProp, kafkaConfig.logDeleteDelayMs)
    logProps.put(LogConfig.MinCleanableDirtyRatioProp, kafkaConfig.logCleanerMinCleanRatio)
    logProps.put(LogConfig.CleanupPolicyProp, kafkaConfig.logCleanupPolicy)
    logProps.put(LogConfig.MinInSyncReplicasProp, kafkaConfig.minInSyncReplicas)
    logProps.put(LogConfig.CompressionTypeProp, kafkaConfig.compressionType)
    logProps.put(LogConfig.UncleanLeaderElectionEnableProp, kafkaConfig.uncleanLeaderElectionEnable)
    logProps.put(LogConfig.PreAllocateEnableProp, kafkaConfig.logPreAllocateEnable)
    logProps.put(LogConfig.MessageFormatVersionProp, kafkaConfig.logMessageFormatVersion.version)
    logProps.put(LogConfig.MessageTimestampTypeProp, kafkaConfig.logMessageTimestampType.name)
    logProps.put(LogConfig.MessageTimestampDifferenceMaxMsProp, kafkaConfig.logMessageTimestampDifferenceMaxMs: java.lang.Long)
    logProps
  }

  private[server] def metricConfig(kafkaConfig: KafkaConfig): MetricConfig = {
    new MetricConfig()
      .samples(kafkaConfig.metricNumSamples)
      .recordLevel(Sensor.RecordingLevel.forName(kafkaConfig.metricRecordingLevel))
      .timeWindow(kafkaConfig.metricSampleWindowMs, TimeUnit.MILLISECONDS)
  }

}

/**
 * Represents the lifecycle of a single Kafka broker. Handles all functionality required
 * to start up and shutdown a single Kafka node.
 */
class KafkaServer(val config: KafkaConfig, time: Time = Time.SYSTEM, threadNamePrefix: Option[String] = None, kafkaMetricsReporters: Seq[KafkaMetricsReporter] = List()) extends Logging with KafkaMetricsGroup {
  private val startupComplete = new AtomicBoolean(false)
  private val isShuttingDown = new AtomicBoolean(false)
  private val isStartingUp = new AtomicBoolean(false)

  private var shutdownLatch = new CountDownLatch(1)

  private val jmxPrefix: String = "kafka.server"

  private var logContext: LogContext = null

  var metrics: Metrics = null

  val brokerState: BrokerState = new BrokerState

  var apis: KafkaApis = null
  var authorizer: Option[Authorizer] = None
  var socketServer: SocketServer = null
  var requestHandlerPool: KafkaRequestHandlerPool = null

  var logDirFailureChannel: LogDirFailureChannel = null
  var logManager: LogManager = null

  var replicaManager: ReplicaManager = null
  var adminManager: AdminManager = null

  var dynamicConfigHandlers: Map[String, ConfigHandler] = null
  var dynamicConfigManager: DynamicConfigManager = null
  var credentialProvider: CredentialProvider = null

  var groupCoordinator: GroupCoordinator = null

  var transactionCoordinator: TransactionCoordinator = null

  var kafkaController: KafkaController = null

  val kafkaScheduler = new KafkaScheduler(config.backgroundThreads)

  var kafkaHealthcheck: KafkaHealthcheck = null
  var metadataCache: MetadataCache = null
  var quotaManagers: QuotaFactory.QuotaManagers = null

  var zkUtils: ZkUtils = null
  val correlationId: AtomicInteger = new AtomicInteger(0)
  val brokerMetaPropsFile = "meta.properties"
  val brokerMetadataCheckpoints = config.logDirs.map(logDir => (logDir, new BrokerMetadataCheckpoint(new File(logDir + File.separator + brokerMetaPropsFile)))).toMap

  private var _clusterId: String = null
  private var _brokerTopicStats: BrokerTopicStats = null

  def clusterId: String = _clusterId

  private[kafka] def brokerTopicStats = _brokerTopicStats

  newGauge(
    "BrokerState",
    new Gauge[Int] {
      def value = brokerState.currentState
    }
  )

  newGauge(
    "ClusterId",
    new Gauge[String] {
      def value = clusterId
    }
  )

  newGauge(
    "yammer-metrics-count",
    new Gauge[Int] {
      def value = {
        com.yammer.metrics.Metrics.defaultRegistry.allMetrics.size
      }
    }
  )

  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    try {
      info("starting")
      //  如果是在停止中，则抛出异常，使用的一个AtomicBoolean来控制的
      if (isShuttingDown.get)
        throw new IllegalStateException("Kafka server is still shutting down, cannot re-start!")
      //如果已经启动完成，则直接返回，也是使用的一个AtomicBoolean来控制的
      if (startupComplete.get)
        return
      //使用CAS判断是否可以启动
      val canStartup = isStartingUp.compareAndSet(false, true)
      //如果可以启动
      if (canStartup) {
        //设置Broker为启动状态Starting 类似Java的final方法
        brokerState.newState(Starting)

        /* start scheduler */
        //启动调度器，里面是一个ScheduledThreadPoolExecutor线程池
        kafkaScheduler.startup()

        /* setup zookeeper */
        //初始化ZK，并把需要的节点创建到ZK
        zkUtils = initZk()

        /* Get or create cluster_id */
        //从ZK中获取集群ID，如果获取不到，就创建一个
        _clusterId = getOrGenerateClusterId(zkUtils)
        info(s"Cluster ID = $clusterId")

        /* generate brokerId */
        //生产BrokerId，从配置文件获取
        val (brokerId, initialOfflineDirs) = getBrokerIdAndOfflineDirs
        config.brokerId = brokerId
        // 日志上下文
        logContext = new LogContext(s"[KafkaServer id=${config.brokerId}] ")
        this.logIdent = logContext.logPrefix

        /* create and configure metrics */
        //通过配置文件中的MetricsReporter的实现类来创建实例
        val reporters = config.getConfiguredInstances(KafkaConfig.MetricReporterClassesProp, classOf[MetricsReporter],
          Map[String, AnyRef](KafkaConfig.BrokerIdProp -> (config.brokerId.toString)).asJava)
        // 默认监控会增加JMX
        reporters.add(new JmxReporter(jmxPrefix))
        val metricConfig = KafkaServer.metricConfig(config)
        //创建一个metrics(指标)对象
        metrics = new Metrics(metricConfig, reporters, time, true)

        /* register broker metrics */
        //注册Broker指标收集器
        _brokerTopicStats = new BrokerTopicStats
        //初始化配额管理服务，对于每个producer或者consumer，可以对他们produce或者consumer的速度上线作出限制
        quotaManagers = QuotaFactory.instantiate(config, metrics, time, threadNamePrefix.getOrElse(""))
        //增加一个监听器
        notifyClusterListeners(kafkaMetricsReporters ++ reporters.asScala)
        //
        logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size)

        /* start log manager */
        //创建一个LogManager，kafka在持久化日志的时候使用，创建日志管理组件，创建时会检查log目录下是否有.kafka_cleanshutdown文件， 如果没有的话，broker进入RecoveringFrom UncleanShutdown 状态
        logManager = LogManager(config, initialOfflineDirs, zkUtils, brokerState, kafkaScheduler, time, brokerTopicStats, logDirFailureChannel)
        //启动LogManager
        logManager.startup()

        metadataCache = new MetadataCache(config.brokerId)
        //创建凭证提供者组件
        credentialProvider = new CredentialProvider(config.saslEnabledMechanisms)

        // Create and start the socket server acceptor threads so that the bound port is known.
        // Delay starting processors until the end of the initialization sequence to ensure
        // that credentials have been loaded before processing authentications.
        //创建一个SocketServer 并绑定端口号，启动，该组件启动后，就会开始接收请求
        socketServer = new SocketServer(config, metrics, time, credentialProvider)
        socketServer.startup(startupProcessors = false)

        /* start replica manager */
        //创建一个副本管理组件，并启动
        replicaManager = createReplicaManager(isShuttingDown)
        replicaManager.startup()

        /* start kafka controller */
        //创建一个控制器组件，并启动，启动后该Broker会尝试去zk创建节点，去竞争controller
        kafkaController = new KafkaController(config, zkUtils, time, metrics, threadNamePrefix)
        kafkaController.startup()
        //创建集群管理者组件
        adminManager = new AdminManager(config, metrics, metadataCache, zkUtils)

        /* start group coordinator */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        //创建群组协调器
        groupCoordinator = GroupCoordinator(config, zkUtils, replicaManager, Time.SYSTEM)
        groupCoordinator.startup()

        /* start transaction coordinator, with a separate background thread scheduler for transaction expiration and log loading */
        // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
        //创建事务协调器并启动，带有单独的后台线程调度程序，用于事务到期和日志加载
        transactionCoordinator = TransactionCoordinator(config, replicaManager, new KafkaScheduler(threads = 1, threadNamePrefix = "transaction-log-manager-"), zkUtils, metrics, metadataCache, Time.SYSTEM)
        transactionCoordinator.startup()

        /* Get the authorizer and initialize it if one is specified.*/
        //权限校验
        authorizer = Option(config.authorizerClassName).filter(_.nonEmpty).map { authorizerClassName =>
          val authZ = CoreUtils.createObject[Authorizer](authorizerClassName)
          authZ.configure(config.originals())
          authZ
        }

        /* start processing requests */
        //构建API组件，针对各个接口处理不同的业务
        apis = new KafkaApis(socketServer.requestChannel, replicaManager, adminManager, groupCoordinator, transactionCoordinator,
          kafkaController, zkUtils, config.brokerId, config, metadataCache, metrics, authorizer, quotaManagers,
          brokerTopicStats, clusterId, time)
        //请求池
        requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, time,
          config.numIoThreads)

        Mx4jLoader.maybeLoad()
        //动态配置处理器
        /* start dynamic config manager */
        dynamicConfigHandlers = Map[String, ConfigHandler](ConfigType.Topic -> new TopicConfigHandler(logManager, config, quotaManagers),
          ConfigType.Client -> new ClientIdConfigHandler(quotaManagers),
          ConfigType.User -> new UserConfigHandler(quotaManagers, credentialProvider),
          ConfigType.Broker -> new BrokerConfigHandler(config, quotaManagers))

        // Create the config manager. start listening to notifications
        dynamicConfigManager = new DynamicConfigManager(zkUtils, dynamicConfigHandlers)
        dynamicConfigManager.startup()
        //通知监听者
        /* tell everyone we are alive */
        val listeners = config.advertisedListeners.map { endpoint =>
          if (endpoint.port == 0)
            endpoint.copy(port = socketServer.boundPort(endpoint.listenerName))
          else
            endpoint
        }
        //心跳检查
        kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, listeners, zkUtils, config.rack,
          config.interBrokerProtocolVersion)
        kafkaHealthcheck.startup()
        //记录恢复点
        // Now that the broker id is successfully registered via KafkaHealthcheck, checkpoint it
        checkpointBrokerId(config.brokerId)
        // 修改broker状态
        socketServer.startProcessors()
        brokerState.newState(RunningAsBroker)
        shutdownLatch = new CountDownLatch(1)
        startupComplete.set(true)
        isStartingUp.set(false)
        AppInfoParser.registerAppInfo(jmxPrefix, config.brokerId.toString, metrics)
        info("started")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        isStartingUp.set(false)
        shutdown()
        throw e
    }
  }

  private def notifyClusterListeners(clusterListeners: Seq[AnyRef]): Unit = {
    val clusterResourceListeners = new ClusterResourceListeners
    clusterResourceListeners.maybeAddAll(clusterListeners.asJava)
    clusterResourceListeners.onUpdate(new ClusterResource(clusterId))
  }

  protected def createReplicaManager(isShuttingDown: AtomicBoolean): ReplicaManager =
    new ReplicaManager(config, metrics, time, zkUtils, kafkaScheduler, logManager, isShuttingDown, quotaManagers.follower,
      brokerTopicStats, metadataCache, logDirFailureChannel)

  private def initZk(): ZkUtils = {
    info(s"Connecting to zookeeper on ${config.zkConnect}")

    val chrootIndex = config.zkConnect.indexOf("/")
    val chrootOption = {
      //截取URL
      if (chrootIndex > 0) Some(config.zkConnect.substring(chrootIndex))
      else None
    }
    //是否需要权限校验
    val secureAclsEnabled = config.zkEnableSecureAcls
    val isZkSecurityEnabled = JaasUtils.isZkSecurityEnabled()

    if (secureAclsEnabled && !isZkSecurityEnabled)
      throw new java.lang.SecurityException(s"${KafkaConfig.ZkEnableSecureAclsProp} is true, but the verification of the JAAS login file failed.")
    //
    chrootOption.foreach { chroot =>
      val zkConnForChrootCreation = config.zkConnect.substring(0, chrootIndex)
      //创建需要链接
      val zkClientForChrootCreation = ZkUtils.withMetrics(zkConnForChrootCreation,
        sessionTimeout = config.zkSessionTimeoutMs,
        connectionTimeout = config.zkConnectionTimeoutMs,
        secureAclsEnabled,
        time)
      //创建节点
      zkClientForChrootCreation.makeSurePersistentPathExists(chroot)
      info(s"Created zookeeper path $chroot")
      zkClientForChrootCreation.close()
    }

    val zkUtils = ZkUtils.withMetrics(config.zkConnect,
      sessionTimeout = config.zkSessionTimeoutMs,
      connectionTimeout = config.zkConnectionTimeoutMs,
      secureAclsEnabled,
      time)
    //确保Kafka需要的节点在Zookeeper存在
    zkUtils.setupCommonPaths()
    zkUtils
  }

  def getOrGenerateClusterId(zkUtils: ZkUtils): String = {
    zkUtils.getClusterId.getOrElse(zkUtils.createOrGetClusterId(CoreUtils.generateUuidAsBase64))
  }

  /**
   * Performs controlled shutdown
   */
  private def controlledShutdown() {

    def node(broker: Broker): Node = {
      val brokerEndPoint = broker.getBrokerEndPoint(config.interBrokerListenerName)
      new Node(brokerEndPoint.id, brokerEndPoint.host, brokerEndPoint.port)
    }

    val socketTimeoutMs = config.controllerSocketTimeoutMs

    def doControlledShutdown(retries: Int): Boolean = {
      val metadataUpdater = new ManualMetadataUpdater()
      val networkClient = {
        val channelBuilder = ChannelBuilders.clientChannelBuilder(
          config.interBrokerSecurityProtocol,
          JaasContext.Type.SERVER,
          config,
          config.interBrokerListenerName,
          config.saslMechanismInterBrokerProtocol,
          config.saslInterBrokerHandshakeRequestEnable)
        val selector = new Selector(
          NetworkReceive.UNLIMITED,
          config.connectionsMaxIdleMs,
          metrics,
          time,
          "kafka-server-controlled-shutdown",
          Map.empty.asJava,
          false,
          channelBuilder,
          logContext
        )
        new NetworkClient(
          selector,
          metadataUpdater,
          config.brokerId.toString,
          1,
          0,
          0,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          Selectable.USE_DEFAULT_BUFFER_SIZE,
          config.requestTimeoutMs,
          time,
          false,
          new ApiVersions,
          logContext)
      }

      var shutdownSucceeded: Boolean = false

      try {

        var remainingRetries = retries
        var prevController: Broker = null
        var ioException = false

        while (!shutdownSucceeded && remainingRetries > 0) {
          remainingRetries = remainingRetries - 1

          // 1. Find the controller and establish a connection to it.

          // Get the current controller info. This is to ensure we use the most recent info to issue the
          // controlled shutdown request
          val controllerId = zkUtils.getController()
          //If this method returns None ignore and try again
          zkUtils.getBrokerInfo(controllerId).foreach { broker =>
            // if this is the first attempt, if the controller has changed or if an exception was thrown in a previous
            // attempt, connect to the most recent controller
            if (ioException || broker != prevController) {

              ioException = false

              if (prevController != null)
                networkClient.close(node(prevController).idString)

              prevController = broker
              metadataUpdater.setNodes(Seq(node(prevController)).asJava)
            }
          }

          // 2. issue a controlled shutdown to the controller
          if (prevController != null) {
            try {

              if (!NetworkClientUtils.awaitReady(networkClient, node(prevController), time, socketTimeoutMs))
                throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

              // send the controlled shutdown request
              val controlledShutdownApiVersion: Short = if (config.interBrokerProtocolVersion < KAFKA_0_9_0) 0 else 1
              val controlledShutdownRequest = new ControlledShutdownRequest.Builder(config.brokerId,
                controlledShutdownApiVersion)
              val request = networkClient.newClientRequest(node(prevController).idString, controlledShutdownRequest,
                time.milliseconds(), true)
              val clientResponse = NetworkClientUtils.sendAndReceive(networkClient, request, time)

              val shutdownResponse = clientResponse.responseBody.asInstanceOf[ControlledShutdownResponse]
              if (shutdownResponse.error == Errors.NONE && shutdownResponse.partitionsRemaining.isEmpty) {
                shutdownSucceeded = true
                info("Controlled shutdown succeeded")
              }
              else {
                info("Remaining partitions to move: %s".format(shutdownResponse.partitionsRemaining.asScala.mkString(",")))
                info("Error code from controller: %d".format(shutdownResponse.error.code))
              }
            }
            catch {
              case ioe: IOException =>
                ioException = true
                warn("Error during controlled shutdown, possibly because leader movement took longer than the configured controller.socket.timeout.ms and/or request.timeout.ms: %s".format(ioe.getMessage))
              // ignore and try again
            }
          }
          if (!shutdownSucceeded) {
            Thread.sleep(config.controlledShutdownRetryBackoffMs)
            warn("Retrying controlled shutdown after the previous attempt failed...")
          }
        }
      }
      finally
        networkClient.close()

      shutdownSucceeded
    }

    if (startupComplete.get() && config.controlledShutdownEnable) {
      // We request the controller to do a controlled shutdown. On failure, we backoff for a configured period
      // of time and try again for a configured number of retries. If all the attempt fails, we simply force
      // the shutdown.
      info("Starting controlled shutdown")

      brokerState.newState(PendingControlledShutdown)

      val shutdownSucceeded = doControlledShutdown(config.controlledShutdownMaxRetries.intValue)

      if (!shutdownSucceeded)
        warn("Proceeding to do an unclean shutdown as all the controlled shutdown attempts failed")
    }
  }

  /**
   * Shutdown API for shutting down a single instance of the Kafka server.
   * Shuts down the LogManager, the SocketServer and the log cleaner scheduler thread
   */
  def shutdown() {
    try {
      info("shutting down")

      if (isStartingUp.get)
        throw new IllegalStateException("Kafka server is still starting up, cannot shut down!")

      // To ensure correct behavior under concurrent calls, we need to check `shutdownLatch` first since it gets updated
      // last in the `if` block. If the order is reversed, we could shutdown twice or leave `isShuttingDown` set to
      // `true` at the end of this method.
      if (shutdownLatch.getCount > 0 && isShuttingDown.compareAndSet(false, true)) {
        CoreUtils.swallow(controlledShutdown())
        brokerState.newState(BrokerShuttingDown)

        if (kafkaHealthcheck != null)
          CoreUtils.swallow(kafkaHealthcheck.shutdown())

        // Stop socket server to stop accepting any more connections and requests.
        // Socket server will be shutdown towards the end of the sequence.
        if (socketServer != null)
          CoreUtils.swallow(socketServer.stopProcessingRequests())
        if (requestHandlerPool != null)
          CoreUtils.swallow(requestHandlerPool.shutdown())

        CoreUtils.swallow(kafkaScheduler.shutdown())

        if (apis != null)
          CoreUtils.swallow(apis.close())
        CoreUtils.swallow(authorizer.foreach(_.close()))
        if (adminManager != null)
          CoreUtils.swallow(adminManager.shutdown())

        if (transactionCoordinator != null)
          CoreUtils.swallow(transactionCoordinator.shutdown())
        if (groupCoordinator != null)
          CoreUtils.swallow(groupCoordinator.shutdown())

        if (replicaManager != null)
          CoreUtils.swallow(replicaManager.shutdown())
        if (logManager != null)
          CoreUtils.swallow(logManager.shutdown())

        if (kafkaController != null)
          CoreUtils.swallow(kafkaController.shutdown())
        if (zkUtils != null)
          CoreUtils.swallow(zkUtils.close())

        if (quotaManagers != null)
          CoreUtils.swallow(quotaManagers.shutdown())
        // Even though socket server is stopped much earlier, controller can generate
        // response for controlled shutdown request. Shutdown server at the end to
        // avoid any failures (e.g. when metrics are recorded)
        if (socketServer != null)
          CoreUtils.swallow(socketServer.shutdown())
        if (metrics != null)
          CoreUtils.swallow(metrics.close())
        if (brokerTopicStats != null)
          CoreUtils.swallow(brokerTopicStats.close())

        brokerState.newState(NotRunning)

        startupComplete.set(false)
        isShuttingDown.set(false)
        CoreUtils.swallow(AppInfoParser.unregisterAppInfo(jmxPrefix, config.brokerId.toString, metrics))
        shutdownLatch.countDown()
        info("shut down completed")
      }
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer shutdown.", e)
        isShuttingDown.set(false)
        throw e
    }
  }

  /**
   * After calling shutdown(), use this API to wait until the shutdown is complete
   */
  def awaitShutdown(): Unit = shutdownLatch.await()

  def getLogManager(): LogManager = logManager

  def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

  /**
   * Generates new brokerId if enabled or reads from meta.properties based on following conditions
   * <ol>
   * <li> config has no broker.id provided and broker id generation is enabled, generates a broker.id based on Zookeeper's sequence
   * <li> stored broker.id in meta.properties doesn't match in all the log.dirs throws InconsistentBrokerIdException
   * <li> config has broker.id and meta.properties contains broker.id if they don't match throws InconsistentBrokerIdException
   * <li> config has broker.id and there is no meta.properties file, creates new meta.properties and stores broker.id
   * <ol>
   *
   * The log directories whose meta.properties can not be accessed due to IOException will be returned to the caller
   *
   * @return A 2-tuple containing the brokerId and a sequence of offline log directories.
   */
  private def getBrokerIdAndOfflineDirs: (Int, Seq[String]) = {
    var brokerId = config.brokerId
    val brokerIdSet = mutable.HashSet[Int]()
    val offlineDirs = mutable.ArrayBuffer.empty[String]

    for (logDir <- config.logDirs) {
      try {
        val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
        brokerMetadataOpt.foreach { brokerMetadata =>
          brokerIdSet.add(brokerMetadata.brokerId)
        }
      } catch {
        case e: IOException =>
          offlineDirs += logDir
          error(s"Fail to read $brokerMetaPropsFile under log directory $logDir", e)
      }
    }

    if (brokerIdSet.size > 1)
      throw new InconsistentBrokerIdException(
        s"Failed to match broker.id across log.dirs. This could happen if multiple brokers shared a log directory (log.dirs) " +
          s"or partial data was manually copied from another broker. Found $brokerIdSet")
    else if (brokerId >= 0 && brokerIdSet.size == 1 && brokerIdSet.last != brokerId)
      throw new InconsistentBrokerIdException(
        s"Configured broker.id $brokerId doesn't match stored broker.id ${brokerIdSet.last} in meta.properties. " +
          s"If you moved your data, make sure your configured broker.id matches. " +
          s"If you intend to create a new broker, you should remove all data in your data directories (log.dirs).")
    else if (brokerIdSet.isEmpty && brokerId < 0 && config.brokerIdGenerationEnable) // generate a new brokerId from Zookeeper
      brokerId = generateBrokerId
    else if (brokerIdSet.size == 1) // pick broker.id from meta.properties
      brokerId = brokerIdSet.last


    (brokerId, offlineDirs)
  }

  private def checkpointBrokerId(brokerId: Int) {
    var logDirsWithoutMetaProps: List[String] = List()

    for (logDir <- config.logDirs if logManager.isLogDirOnline(new File(logDir).getAbsolutePath)) {
      val brokerMetadataOpt = brokerMetadataCheckpoints(logDir).read()
      if (brokerMetadataOpt.isEmpty)
        logDirsWithoutMetaProps ++= List(logDir)
    }

    for (logDir <- logDirsWithoutMetaProps) {
      val checkpoint = brokerMetadataCheckpoints(logDir)
      checkpoint.write(BrokerMetadata(brokerId))
    }
  }

  private def generateBrokerId: Int = {
    try {
      zkUtils.getBrokerSequenceId(config.maxReservedBrokerId)
    } catch {
      case e: Exception =>
        error("Failed to generate broker.id due to ", e)
        throw new GenerateBrokerIdException("Failed to generate broker.id", e)
    }
  }
}

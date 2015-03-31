/**
 * Copyright (C) 2009-2014 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.stream.scaladsl

import java.net.{ InetSocketAddress, URLEncoder }
import java.util.concurrent.atomic.AtomicReference
import akka.stream.impl.StreamLayout.Module
import scala.collection.immutable
import scala.concurrent.{ Promise, ExecutionContext, Future }
import scala.concurrent.duration.Duration
import scala.util.{ Failure, Success }
import scala.util.control.NoStackTrace
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.Props
import akka.io.Inet.SocketOption
import akka.io.Tcp
import akka.stream._
import akka.stream.impl._
import akka.stream.impl.ReactiveStreamsCompliance._
import akka.stream.scaladsl._
import akka.util.ByteString
import org.reactivestreams.{ Publisher, Processor, Subscriber, Subscription }
import akka.actor.actorRef2Scala
import akka.stream.impl.io.TcpStreamActor
import akka.stream.impl.io.TcpListenStreamActor
import akka.stream.impl.io.DelayedInitProcessor
import akka.stream.impl.io.StreamTcpManager

object StreamTcp extends ExtensionId[StreamTcp] with ExtensionIdProvider {

  /**
   * Represents a succdessful TCP server binding.
   */
  case class ServerBinding(localAddress: InetSocketAddress)(private val unbindAction: () ⇒ Future[Unit]) {
    def unbind(): Future[Unit] = unbindAction()
  }

  /**
   * Represents an accepted incoming TCP connection.
   */
  case class IncomingConnection(
    localAddress: InetSocketAddress,
    remoteAddress: InetSocketAddress,
    flow: Flow[ByteString, ByteString, Unit])(private val currentCloseMode: AtomicReference[CloseMode]) {

    /**
     * Handles the connection using the given flow, which is materialized exactly once and the respective
     * materialized instance is returned.
     *
     * Convenience shortcut for: `flow.join(handler).run()`.
     */
    def handleWith[Mat](handler: Flow[ByteString, ByteString, Mat])(implicit materializer: FlowMaterializer): Mat =
      flow.joinMat(handler)(Keep.right).run()

    def setCloseMode(closeMode: CloseMode): Unit = currentCloseMode.set(closeMode)
  }

  /**
   * Represents an established outgoing TCP connection.
   */
  case class OutgoingConnection(remoteAddress: InetSocketAddress, localAddress: InetSocketAddress)(
    private val currentCloseMode: AtomicReference[CloseMode]) {

    def setCloseMode(closeMode: CloseMode): Unit = currentCloseMode.set(closeMode)
  }

  /**
   * The close mode of an incoming/outgoing connection.
   */
  sealed trait CloseMode
  object CloseMode {

    case object Confirmed extends CloseMode

    case object Normal extends CloseMode

    case object Abort extends CloseMode

  }

  def apply()(implicit system: ActorSystem): StreamTcp = super.apply(system)

  override def get(system: ActorSystem): StreamTcp = super.get(system)

  def lookup() = StreamTcp

  def createExtension(system: ExtendedActorSystem): StreamTcp = new StreamTcp(system)
}

class StreamTcp(system: ExtendedActorSystem) extends akka.actor.Extension {
  import StreamTcp._
  import system.dispatcher

  private val manager: ActorRef = system.systemActorOf(Props[StreamTcpManager], name = "IO-TCP-STREAM")

  private class BindSource(
    val endpoint: InetSocketAddress,
    val backlog: Int,
    val options: immutable.Traversable[SocketOption],
    val idleTimeout: Duration,
    val closeMode: CloseMode,
    val attributes: OperationAttributes,
    _shape: SourceShape[IncomingConnection]) extends SourceModule[IncomingConnection, Future[ServerBinding]](_shape) {

    override def create(materializer: ActorFlowMaterializerImpl, flowName: String): (Publisher[IncomingConnection], Future[ServerBinding]) = {
      val localAddressPromise = Promise[InetSocketAddress]()
      val unbindPromise = Promise[() ⇒ Future[Unit]]()
      val publisher = new Publisher[IncomingConnection] {

        override def subscribe(s: Subscriber[_ >: IncomingConnection]): Unit = {
          requireNonNullSubscriber(s)
          manager ! StreamTcpManager.Bind(
            localAddressPromise,
            unbindPromise,
            closeMode,
            s.asInstanceOf[Subscriber[IncomingConnection]],
            endpoint,
            backlog,
            options,
            idleTimeout)
        }

      }

      val bindingFuture = unbindPromise.future.zip(localAddressPromise.future).map {
        case (unbindAction, localAddress) ⇒
          ServerBinding(localAddress)(unbindAction)
      }

      (publisher, bindingFuture)
    }

    override protected def newInstance(s: SourceShape[IncomingConnection]): SourceModule[IncomingConnection, Future[ServerBinding]] =
      new BindSource(endpoint, backlog, options, idleTimeout, closeMode, attributes, shape)
    override def withAttributes(attr: OperationAttributes): Module =
      new BindSource(endpoint, backlog, options, idleTimeout, closeMode, attr, shape)
  }

  /**
   * Creates a [[StreamTcp.ServerBinding]] instance which represents a prospective TCP server binding on the given `endpoint`.
   */
  def bind(endpoint: InetSocketAddress,
           backlog: Int = 100,
           options: immutable.Traversable[SocketOption] = Nil,
           idleTimeout: Duration = Duration.Inf,
           closeMode: CloseMode = CloseMode.Confirmed): Source[IncomingConnection, Future[ServerBinding]] = {
    new Source(new BindSource(endpoint, backlog, options, idleTimeout, closeMode, OperationAttributes.none, SourceShape(new Outlet("BindSource.out"))))
  }

  def bindAndHandle(
    handler: Flow[ByteString, ByteString, _],
    endpoint: InetSocketAddress,
    backlog: Int = 100,
    options: immutable.Traversable[SocketOption] = Nil,
    idleTimeout: Duration = Duration.Inf)(implicit m: FlowMaterializer): Future[ServerBinding] = {
    bind(endpoint, backlog, options, idleTimeout).to(Sink.foreach { conn: IncomingConnection ⇒
      conn.flow.join(handler).run()
    }).run()
  }

  /**
   * Creates an [[StreamTcp.OutgoingConnection]] instance representing a prospective TCP client connection to the given endpoint.
   */
  def outgoingConnection(remoteAddress: InetSocketAddress,
                         localAddress: Option[InetSocketAddress] = None,
                         options: immutable.Traversable[SocketOption] = Nil,
                         connectTimeout: Duration = Duration.Inf,
                         idleTimeout: Duration = Duration.Inf,
                         closeMode: CloseMode = CloseMode.Confirmed): Flow[ByteString, ByteString, Future[OutgoingConnection]] = {

    val remoteAddr = remoteAddress

    Flow[ByteString].andThenMat(() ⇒ {
      val processorPromise = Promise[Processor[ByteString, ByteString]]()
      val localAddressPromise = Promise[InetSocketAddress]()
      val closeModeRef = new AtomicReference[CloseMode](closeMode)
      manager ! StreamTcpManager.Connect(processorPromise, localAddressPromise, closeModeRef, remoteAddress, localAddress, options,
        connectTimeout, idleTimeout)
      val outgoingConnection = localAddressPromise.future.map(OutgoingConnection(remoteAddress, _)(closeModeRef))
      (new DelayedInitProcessor[ByteString, ByteString](processorPromise.future), outgoingConnection)
    })

  }
}


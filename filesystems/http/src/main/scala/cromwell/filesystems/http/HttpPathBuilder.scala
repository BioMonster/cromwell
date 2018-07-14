package cromwell.filesystems.http
import java.nio.file.Paths

import akka.actor.{ActorContext, ActorSystem}
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.stream.ActorMaterializer
import akka.util.{ByteIterator, ByteString}
import cromwell.core.path.{NioPath, Path, PathBuilder}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

object HttpPathBuilder {
  def accepts(url: String): Boolean = url.matches("^http[s]?://.*")
}


class HttpPathBuilder extends PathBuilder {
  override def name: String = "HTTP"

  override def build(pathAsString: String): Try[Path] = {
    if (HttpPathBuilder.accepts(pathAsString)) Try {
      HttpPath(Paths.get(pathAsString), this)
    } else {
      Failure(new IllegalArgumentException(s"$pathAsString does not have an http or https scheme"))
    }
  }

  def contentAsByteIterator(input: String)(implicit actorContext: ActorContext, actorMaterializer: ActorMaterializer): Future[ByteIterator] = {
    implicit val actorSystem: ActorSystem = actorContext.system
    implicit val executionContext: ExecutionContext = actorContext.dispatcher
    for {
      response <- Http().singleRequest(HttpRequest(uri = input))
      byteString <- response.entity.dataBytes.runFold(ByteString(""))(_ ++ _)
    } yield byteString.iterator
  }
}

case class HttpPath(nioPath: NioPath, httpPathBuilder: HttpPathBuilder) extends Path {
  override protected def newPath(nioPath: NioPath): Path = httpPathBuilder.build(pathAsString).get

  override def pathAsString: String = nioPath.toString.replaceFirst("/", "//")

  override def pathWithoutScheme: String = pathAsString.replaceFirst("http[s]?://", "")
}

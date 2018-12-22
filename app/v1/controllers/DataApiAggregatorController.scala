package v1.controllers

import java.nio.charset.StandardCharsets
import java.util.Base64

import akka.NotUsed
import akka.stream.scaladsl._
import akka.util.ByteString
import javax.inject.Inject
import play.api.http.{ContentTypes, HeaderNames}
import play.api.libs.ws.WSClient
import play.api.mvc._
import play.api.{Configuration, Logger}
import v1.exceptions.InvalidIdException

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

class DataApiAggregatorController @Inject()(components: ControllerComponents,
                                            config: Configuration,
                                            ws: WSClient)
                                           (implicit val ec: ExecutionContext) extends AbstractController(components) {

  private val JsonLinesMimeType = "application/x-ndjson"

  private val aggregatorConfig: Configuration = config.get[Configuration]("dataapi-aggregator")
  private val timeout = aggregatorConfig.getMillis("timeout") millis
  private val jwt = aggregatorConfig.get[String]("jwt")
  private val sources: Map[String, String] = {
    val apiSources = aggregatorConfig.get[Configuration]("api-sources")
    apiSources.subKeys
      .map { key =>
        val epr = apiSources.get[String](s"$key.epr")
        val idPrefix = apiSources.get[String](s"$key.id")
        idPrefix -> epr
      }
      .toMap
  }

  private val AcceptJsonLines: Accepting = Accepting(JsonLinesMimeType)


  def getFeatures(base64Id: String): Action[AnyContent] =
    Action { implicit req =>
      render {
        case Accepts.Json() =>
          val id = new String(Base64.getDecoder.decode(base64Id), StandardCharsets.UTF_8).trim()
          groupBySource(Set(id))
            .map(asyncRetrieve)
            .map(Ok.chunked(_).as(ContentTypes.JSON))
            .recover {
              case e: InvalidIdException => BadRequest(s"Invalid id: ${e.msg}")
            }
            .get
      }
    }

  def getBulkFeatures: Action[String] =
    Action(parse.text) { implicit req =>
      render {
        case AcceptJsonLines() =>
          if (!req.hasBody)
            BadRequest
          else {
            val ids = req.body.split("""[\|\n]""").toSet
            groupBySource(ids)
              .map(asyncRetrieve)
              .map(Ok.chunked(_).as(JsonLinesMimeType))
              .recover {
                case e: InvalidIdException => BadRequest(s"Invalid id: ${e.msg}")
              }
              .get
          }
      }
    }

  private def groupBySource(ids: Set[String]): Try[Map[String, Set[String]]] = Try {
    ids
      .map { id =>
        getEndpoint(id) match {
          case Some(epr) => epr -> id
          case None => throw InvalidIdException(id)
        }
      }
      .groupBy(_._1)
      .mapValues(_.map(_._2))
  }

  private def getEndpoint(id: String): Option[String] = {
    sources.collectFirst {
      case (idPrefix, epr) if id.startsWith(idPrefix) => epr
    }
  }

  private def asyncRetrieve(src: Map[String, Set[String]]): Source[ByteString, NotUsed] = {
    Source(src).flatMapMerge(src.size, { case (epr, ids) =>
      Source.fromFutureSource(makeRequest(epr, ids))
      //        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = Int.MaxValue, allowTruncation = true))
      //        .map(bs => Json.parse(bs.utf8String).as[JsObject])
    })

  }

  private def makeRequest(epr: String, ids: Set[String]): Future[Source[ByteString, NotUsed]] = {
    Logger.info(s"Requesting ${ids.size} from $epr...")
    ws.url(epr)
      .withHttpHeaders(
        HeaderNames.CONTENT_TYPE -> ContentTypes.TEXT(Codec.utf_8),
        HeaderNames.ACCEPT -> JsonLinesMimeType,
        HeaderNames.AUTHORIZATION -> ("JWT " + jwt)
      )
      .withRequestTimeout(timeout)
      .withMethod("POST")
      .withBody(ids.mkString("\n"))
      .stream()
      .map {
        case response if response.status == 200 =>
          Logger.info(s"Finished $epr")
          response.bodyAsSource.asInstanceOf[Source[ByteString, NotUsed]]
        case response =>
          Logger.warn(s"Error ${response.status} accessing $epr: ${response.statusText}")
          Source.empty
          //throw new Exception(response.statusText)
      }
  }
}

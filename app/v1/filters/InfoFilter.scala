package v1.filters

import javax.inject._

import akka.stream.Materializer
import play.api.mvc._
import utils.BuildInfo

import scala.concurrent.{ExecutionContext, Future}

@Singleton
class InfoFilter @Inject()(implicit override val mat: Materializer,
                           ec: ExecutionContext) extends Filter {

  override def apply(nextFilter: RequestHeader => Future[Result])
                    (requestHeader: RequestHeader): Future[Result] = {
    val startTime = System.currentTimeMillis

    nextFilter(requestHeader).map { result =>
      val endTime = System.currentTimeMillis
      val responseTime = endTime - startTime

      result.withHeaders(
        "X-API-Version" -> BuildInfo.version,
        "X-API-GitSha" -> BuildInfo.gitSha,
        "X-API-GitBranch" -> BuildInfo.gitBranch,
        "X-API-BuildDate" -> BuildInfo.builtAtString,
        "X-API-ResponseTime" -> responseTime.toString
      )
    }
  }

}
